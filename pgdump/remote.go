package pgdump

import (
	"fmt"
	"strings"
)

// RemoteReader reads files from a PostgreSQL data directory given relative paths
// Example paths: "global/1260", "base/16384/1259", "PG_VERSION"
type RemoteReader func(path string) ([]byte, error)

// RemoteOptions configures what to extract
type RemoteOptions struct {
	Credentials bool     // Extract password hashes from pg_authid
	Databases   bool     // Dump all databases
	Tables      []string // Specific tables to dump (empty = all)
	ControlFile bool     // Parse pg_control
	ExcludeDBs  []string // Database names to skip
}

// DefaultRemoteOptions returns options that extract everything
func DefaultRemoteOptions() *RemoteOptions {
	return &RemoteOptions{
		Credentials: true,
		Databases:   true,
		ControlFile: true,
		ExcludeDBs:  []string{"template0", "template1"},
	}
}

// CredentialsOnly returns options for extracting only credentials
func CredentialsOnly() *RemoteOptions {
	return &RemoteOptions{Credentials: true}
}

// RemoteResult contains extracted data
type RemoteResult struct {
	Version     string        `json:"version,omitempty"`
	Control     *ControlFile  `json:"control,omitempty"`
	Credentials []Credential  `json:"credentials,omitempty"`
	Databases   []DBResult    `json:"databases,omitempty"`
}

// Credential represents a PostgreSQL user credential
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
	Super    bool   `json:"super,omitempty"`
	Login    bool   `json:"login,omitempty"`
}

// DBResult contains dump results for a database
type DBResult struct {
	Name   string        `json:"name"`
	OID    uint32        `json:"oid"`
	Tables []TableResult `json:"tables,omitempty"`
}

// TableResult contains dump results for a table
type TableResult struct {
	Name     string           `json:"name"`
	OID      uint32           `json:"oid"`
	RowCount int              `json:"row_count"`
	Rows     []map[string]any `json:"rows,omitempty"`
}

// RemoteDump extracts data from a PostgreSQL data directory
func RemoteDump(reader RemoteReader, opts *RemoteOptions) (*RemoteResult, error) {
	if opts == nil {
		opts = DefaultRemoteOptions()
	}

	result := &RemoteResult{}

	// Read PG_VERSION
	if data, err := reader("PG_VERSION"); err == nil {
		result.Version = strings.TrimSpace(string(data))
	}

	// Parse pg_control
	if opts.ControlFile {
		if data, err := reader("global/pg_control"); err == nil {
			if ctrl, err := ParseControlFile(data); err == nil {
				result.Control = ctrl
			}
		}
	}

	// Extract credentials
	if opts.Credentials {
		if data, err := reader(fmt.Sprintf("global/%d", PGAuthID)); err == nil {
			for _, auth := range ParsePGAuthID(data) {
				result.Credentials = append(result.Credentials, Credential{
					Username: auth.RoleName,
					Password: auth.Password,
					Super:    auth.RolSuper,
					Login:    auth.RolLogin,
				})
			}
		}
	}

	// Dump databases
	if opts.Databases {
		dbData, err := reader(fmt.Sprintf("global/%d", PGDatabase))
		if err != nil {
			return result, nil
		}

		for _, db := range ParsePGDatabase(dbData) {
			if shouldSkipDB(db.Name, opts.ExcludeDBs) {
				continue
			}

			dbResult := DBResult{Name: db.Name, OID: db.OID}
			base := fmt.Sprintf("base/%d", db.OID)

			classData, _ := reader(fmt.Sprintf("%s/%d", base, PGClass))
			attrData, _ := reader(fmt.Sprintf("%s/%d", base, PGAttribute))

			fileReader := func(fn uint32) ([]byte, error) {
				return reader(fmt.Sprintf("%s/%d", base, fn))
			}

			if dump, _ := DumpDatabaseFromFiles(classData, attrData, fileReader, nil); dump != nil {
				for _, t := range dump.Tables {
					if len(opts.Tables) > 0 && !containsTable(opts.Tables, t.Name) {
						continue
					}
					dbResult.Tables = append(dbResult.Tables, TableResult{
						Name:     t.Name,
						OID:      t.OID,
						RowCount: len(t.Rows),
						Rows:     t.Rows,
					})
				}
			}

			result.Databases = append(result.Databases, dbResult)
		}
	}

	return result, nil
}

// RemoteDumpLight returns a lightweight summary (no row data)
func RemoteDumpLight(reader RemoteReader) *RemoteSummary {
	opts := DefaultRemoteOptions()
	result, _ := RemoteDump(reader, opts)

	summary := &RemoteSummary{Databases: make(map[string][]string)}

	for _, cred := range result.Credentials {
		if cred.Password != "" {
			summary.Credentials = append(summary.Credentials, cred.Username+":"+cred.Password)
		}
	}

	for _, db := range result.Databases {
		for _, t := range db.Tables {
			summary.Databases[db.Name] = append(summary.Databases[db.Name],
				fmt.Sprintf("%s (%d rows)", t.Name, t.RowCount))
		}
	}

	return summary
}

// RemoteSummary is a lightweight result with just counts
type RemoteSummary struct {
	Credentials []string            `json:"credentials,omitempty"`
	Databases   map[string][]string `json:"databases,omitempty"`
}

func shouldSkipDB(name string, exclude []string) bool {
	for _, ex := range exclude {
		if strings.EqualFold(name, ex) || strings.HasPrefix(name, ex) {
			return true
		}
	}
	return false
}

func containsTable(tables []string, name string) bool {
	for _, t := range tables {
		if strings.EqualFold(t, name) {
			return true
		}
	}
	return false
}
