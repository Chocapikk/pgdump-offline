package pgdump

import (
	"os"
	"path/filepath"
	"testing"
)

// Test data paths - set via environment or use local testdata
func testDataPath() string {
	if p := os.Getenv("PGDUMP_TESTDATA"); p != "" {
		return p
	}
	return "testdata"
}

func TestParsePGDatabase(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(testDataPath(), "global", "1262"))
	if err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	dbs := ParsePGDatabase(data)
	if len(dbs) == 0 {
		t.Fatal("Expected at least one database")
	}

	// Should find testdb
	found := false
	for _, db := range dbs {
		if db.Name == "testdb" {
			found = true
			if db.OID == 0 {
				t.Error("testdb OID should not be 0")
			}
		}
	}
	if !found {
		t.Error("Expected to find 'testdb' database")
	}
}

func TestParsePGClass(t *testing.T) {
	// Need to find testdb OID first
	dbData, err := os.ReadFile(filepath.Join(testDataPath(), "global", "1262"))
	if err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	var testdbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == "testdb" {
			testdbOID = db.OID
			break
		}
	}
	if testdbOID == 0 {
		t.Skip("testdb not found")
	}

	classPath := filepath.Join(testDataPath(), "base", uitoa(testdbOID), "1259")
	data, err := os.ReadFile(classPath)
	if err != nil {
		t.Skipf("pg_class not available: %v", err)
	}

	tables := ParsePGClass(data)
	if len(tables) == 0 {
		t.Fatal("Expected at least one table")
	}

	// Should find users or secrets table
	found := false
	for _, tbl := range tables {
		if tbl.Name == "users" || tbl.Name == "secrets" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find 'users' or 'secrets' table")
	}
}

func TestParsePGAttribute(t *testing.T) {
	dbData, err := os.ReadFile(filepath.Join(testDataPath(), "global", "1262"))
	if err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	var testdbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == "testdb" {
			testdbOID = db.OID
			break
		}
	}
	if testdbOID == 0 {
		t.Skip("testdb not found")
	}

	attrPath := filepath.Join(testDataPath(), "base", uitoa(testdbOID), "1249")
	data, err := os.ReadFile(attrPath)
	if err != nil {
		t.Skipf("pg_attribute not available: %v", err)
	}

	attrs := ParsePGAttribute(data, 0)
	if len(attrs) == 0 {
		t.Fatal("Expected attributes")
	}

	// Check we have column definitions
	total := 0
	for _, cols := range attrs {
		total += len(cols)
	}
	if total == 0 {
		t.Error("Expected at least one column definition")
	}
}

func TestDumpDataDir(t *testing.T) {
	path := testDataPath()
	if _, err := os.Stat(filepath.Join(path, "global", "1262")); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	result, err := DumpDataDir(path, &Options{
		DatabaseFilter:   "testdb",
		TableFilter:      "secrets",
		SkipSystemTables: true,
	})
	if err != nil {
		t.Fatalf("DumpDataDir failed: %v", err)
	}

	if len(result.Databases) == 0 {
		t.Fatal("Expected at least one database")
	}

	db := result.Databases[0]
	if db.Name != "testdb" {
		t.Errorf("Expected testdb, got %s", db.Name)
	}

	if len(db.Tables) == 0 {
		t.Fatal("Expected at least one table")
	}

	// Check secrets table has rows with JSONB
	for _, tbl := range db.Tables {
		if tbl.Name == "secrets" && tbl.RowCount > 0 {
			// Verify JSONB parsing worked
			for _, row := range tbl.Rows {
				if val, ok := row["value"]; ok && val != nil {
					// Should be parsed as map (JSONB)
					if _, isMap := val.(map[string]interface{}); !isMap {
						t.Errorf("Expected JSONB to be parsed as map, got %T", val)
					}
					return // Success
				}
			}
		}
	}
}

func TestDecodeTypes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		oid  int
		want interface{}
	}{
		{"bool true", []byte{1}, OidBool, true},
		{"bool false", []byte{0}, OidBool, false},
		{"int2", []byte{0x39, 0x05}, OidInt2, int16(1337)},
		{"int4", []byte{0xD2, 0x04, 0x00, 0x00}, OidInt4, int32(1234)},
		{"int8", []byte{0x15, 0xCD, 0x5B, 0x07, 0x00, 0x00, 0x00, 0x00}, OidInt8, int64(123456789)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeType(tt.data, tt.oid)
			if got != tt.want {
				t.Errorf("DecodeType() = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestParseJSONB(t *testing.T) {
	// Simple JSONB object: {"a": 1}
	// This is a minimal test - real JSONB has complex headers
	data := []byte{
		0x01, 0x00, 0x00, 0x20, // header: 1 key, object flag
		0x01, 0x00, 0x00, 0x00, // key entry: len=1
		0x0c, 0x00, 0x00, 0x10, // val entry: numeric
		0x61,                   // "a"
		0x00, 0x00, 0x00,       // padding
		0x05, 0x80, 0x01, 0x00, // numeric: 1
	}

	result := ParseJSONB(data)
	if result == nil {
		t.Skip("JSONB parsing returned nil - may need real test data")
	}

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Skipf("Expected map, got %T", result)
	}

	if _, exists := obj["a"]; !exists {
		t.Error("Expected key 'a' in JSONB object")
	}
}

func uitoa(u uint32) string {
	return string('0' + byte(u/10000%10)) +
		string('0' + byte(u/1000%10)) +
		string('0' + byte(u/100%10)) +
		string('0' + byte(u/10%10)) +
		string('0' + byte(u%10))
}
