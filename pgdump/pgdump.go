// Package pgdump provides PostgreSQL heap file parsing functionality.
//
// It can extract data from PostgreSQL data files without requiring
// database credentials, using the fixed OIDs of system catalogs:
//   - 1262: pg_database (global/1262)
//   - 1259: pg_class (base/<db_oid>/1259)
//   - 1249: pg_attribute (base/<db_oid>/1249)
//
// # Basic Usage
//
//	// Parse pg_database to find databases
//	data, _ := os.ReadFile("/path/to/global/1262")
//	databases := pgdump.ParsePGDatabase(data)
//
//	// Parse pg_class to find tables
//	classData, _ := os.ReadFile("/path/to/base/16384/1259")
//	tables := pgdump.ParsePGClass(classData)
//
//	// Parse pg_attribute to get columns
//	attrData, _ := os.ReadFile("/path/to/base/16384/1249")
//	columns := pgdump.ParsePGAttribute(attrData, 0)
//
//	// Read table data
//	tableData, _ := os.ReadFile("/path/to/base/16384/<filenode>")
//	rows := pgdump.ReadRows(tableData, schema, true)
//
// # High-Level API
//
//	// Dump entire data directory
//	result, err := pgdump.DumpDataDir("/var/lib/postgresql/data", nil)
//
//	// With options
//	result, err := pgdump.DumpDataDir("/path/to/data", &pgdump.Options{
//	    DatabaseFilter: "mydb",
//	    TableFilter:    "password",
//	    ListOnly:       false,
//	})
package pgdump

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Options configures the dump operation
type Options struct {
	// DatabaseFilter filters databases by name (empty = all)
	DatabaseFilter string
	// TableFilter filters tables containing this string (empty = all)
	TableFilter string
	// ListOnly returns schema only, no row data
	ListOnly bool
	// SkipSystemTables skips pg_* tables (default: true)
	SkipSystemTables bool
	// PostgresVersion hints the PG version for schema detection (0 = auto)
	PostgresVersion int
}

// DumpResult contains the complete dump output
type DumpResult struct {
	Databases []DatabaseDump `json:"databases"`
}

// DatabaseDump contains dump for a single database
type DatabaseDump struct {
	OID    uint32      `json:"oid"`
	Name   string      `json:"name"`
	Tables []TableDump `json:"tables"`
}

// TableDump contains dump for a single table
type TableDump struct {
	OID      uint32                   `json:"oid"`
	Name     string                   `json:"name"`
	Filenode uint32                   `json:"filenode"`
	Kind     string                   `json:"kind"`
	Columns  []ColumnInfo             `json:"columns,omitempty"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	RowCount int                      `json:"row_count"`
}

// ColumnInfo describes a table column
type ColumnInfo struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	TypID int    `json:"typid"`
}

// DumpDataDir dumps all databases from a PostgreSQL data directory
func DumpDataDir(dataDir string, opts *Options) (*DumpResult, error) {
	if opts == nil {
		opts = &Options{SkipSystemTables: true}
	}

	// Read pg_database
	pgDatabasePath := filepath.Join(dataDir, "global", "1262")
	dbData, err := os.ReadFile(pgDatabasePath)
	if err != nil {
		return nil, err
	}

	databases := ParsePGDatabase(dbData)
	result := &DumpResult{}

	for _, db := range databases {
		// Skip templates
		if strings.HasPrefix(db.Name, "template") {
			continue
		}

		// Apply database filter
		if opts.DatabaseFilter != "" && db.Name != opts.DatabaseFilter {
			continue
		}

		dbDump := dumpDatabase(dataDir, db, opts)
		if dbDump != nil {
			result.Databases = append(result.Databases, *dbDump)
		}
	}

	return result, nil
}

// DumpDatabase dumps a single database by OID
func DumpDatabase(dataDir string, dbOID uint32, opts *Options) (*DatabaseDump, error) {
	if opts == nil {
		opts = &Options{SkipSystemTables: true}
	}

	// Read pg_database to get name
	pgDatabasePath := filepath.Join(dataDir, "global", "1262")
	dbData, err := os.ReadFile(pgDatabasePath)
	if err != nil {
		return nil, err
	}

	var dbInfo *DatabaseInfo
	for _, db := range ParsePGDatabase(dbData) {
		if db.OID == dbOID {
			dbInfo = &db
			break
		}
	}

	if dbInfo == nil {
		return nil, nil
	}

	return dumpDatabase(dataDir, *dbInfo, opts), nil
}

// DumpDatabaseFromFiles dumps a database using pre-read catalog files
func DumpDatabaseFromFiles(classData, attrData []byte, tableReader func(filenode uint32) ([]byte, error), opts *Options) (*DatabaseDump, error) {
	if opts == nil {
		opts = &Options{SkipSystemTables: true}
	}

	tables := ParsePGClass(classData)
	attributes := ParsePGAttribute(attrData, opts.PostgresVersion)

	result := &DatabaseDump{}

	for filenode, tableInfo := range tables {
		// Skip non-regular tables
		if tableInfo.Kind != "r" && tableInfo.Kind != "" {
			continue
		}

		// Skip system tables
		if opts.SkipSystemTables && strings.HasPrefix(tableInfo.Name, "pg_") {
			continue
		}

		// Apply table filter
		if opts.TableFilter != "" && !strings.Contains(strings.ToLower(tableInfo.Name), strings.ToLower(opts.TableFilter)) {
			continue
		}

		tableDump := dumpTableFromReader(filenode, tableInfo, attributes, tableReader, opts)
		if tableDump != nil {
			result.Tables = append(result.Tables, *tableDump)
		}
	}

	return result, nil
}

func dumpDatabase(dataDir string, db DatabaseInfo, opts *Options) *DatabaseDump {
	basePath := filepath.Join(dataDir, "base", strconv.FormatUint(uint64(db.OID), 10))

	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		return nil
	}

	attrData, err := os.ReadFile(filepath.Join(basePath, "1249"))
	if err != nil {
		return nil
	}

	tableReader := func(filenode uint32) ([]byte, error) {
		return os.ReadFile(filepath.Join(basePath, strconv.FormatUint(uint64(filenode), 10)))
	}

	result, _ := DumpDatabaseFromFiles(classData, attrData, tableReader, opts)
	if result != nil {
		result.OID = db.OID
		result.Name = db.Name
	}

	return result
}

func dumpTableFromReader(filenode uint32, tableInfo TableInfo, attributes map[uint32][]AttrInfo, tableReader func(uint32) ([]byte, error), opts *Options) *TableDump {
	attrs := attributes[tableInfo.OID]

	result := &TableDump{
		OID:      tableInfo.OID,
		Name:     tableInfo.Name,
		Filenode: filenode,
		Kind:     tableInfo.Kind,
	}

	// Add column info
	for _, attr := range attrs {
		result.Columns = append(result.Columns, ColumnInfo{
			Name:  attr.Name,
			Type:  TypeName(attr.TypID),
			TypID: attr.TypID,
		})
	}

	if opts.ListOnly {
		return result
	}

	// Read table data
	tableData, err := tableReader(filenode)
	if err != nil {
		return result
	}

	// Convert AttrInfo to Column for decoding
	columns := make([]Column, len(attrs))
	for i, attr := range attrs {
		columns[i] = Column{
			Name:  attr.Name,
			TypID: attr.TypID,
			Len:   attr.Len,
			Num:   attr.Num,
		}
	}

	result.Rows = ReadRows(tableData, columns, true)
	result.RowCount = len(result.Rows)

	return result
}

// ParseFile parses a single heap file and returns raw tuples
func ParseFile(data []byte) []TupleEntry {
	return ReadTuples(data, true)
}

// ParseFileWithSchema parses a heap file using a column schema
func ParseFileWithSchema(data []byte, columns []Column) []map[string]interface{} {
	return ReadRows(data, columns, true)
}
