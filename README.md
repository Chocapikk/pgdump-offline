# pgdump-offline

Dump PostgreSQL data from leaked heap files - no credentials needed.

## The Technique

PostgreSQL uses **fixed OIDs** for system catalogs:
- `1262` - pg_database (list all databases)
- `1259` - pg_class (list all tables)  
- `1249` - pg_attribute (list all columns)

With a file read vulnerability, leak these 3 files to discover the entire schema, then dump any table.

## Installation

### As CLI Tool

```bash
go install github.com/chocapikk/pgdump-offline@latest
```

### As Library

```bash
go get github.com/chocapikk/pgdump-offline/pgdump
```

## CLI Usage

```bash
# Auto-discover and dump everything
pgdump-offline -d /path/to/pg_data/

# List databases and tables only
pgdump-offline -d /path/to/pg_data/ -list

# Dump specific database
pgdump-offline -d /path/to/pg_data/ -db windmill

# Filter tables by name
pgdump-offline -d /path/to/pg_data/ -t password
pgdump-offline -d /path/to/pg_data/ -t token
pgdump-offline -d /path/to/pg_data/ -t secret

# Parse single file
pgdump-offline -f /path/to/1262  # pg_database
pgdump-offline -f /path/to/1259  # pg_class
pgdump-offline -f /path/to/1249  # pg_attribute
```

## Library Usage

### High-Level API

```go
import "github.com/chocapikk/pgdump-offline/pgdump"

// Dump entire data directory
result, err := pgdump.DumpDataDir("/var/lib/postgresql/data", nil)

// With options
result, err := pgdump.DumpDataDir("/path/to/data", &pgdump.Options{
    DatabaseFilter: "mydb",
    TableFilter:    "password",
    ListOnly:       false,
})

// Access results
for _, db := range result.Databases {
    for _, table := range db.Tables {
        for _, row := range table.Rows {
            fmt.Printf("%s: %v\n", row["email"], row["password_hash"])
        }
    }
}
```

### Low-Level API (for custom file readers)

```go
import "github.com/chocapikk/pgdump-offline/pgdump"

// Parse pg_database to find databases
dbData, _ := os.ReadFile("/leaked/global/1262")
databases := pgdump.ParsePGDatabase(dbData)
// => [{OID: 16384, Name: "windmill"}, ...]

// Parse pg_class to find tables  
classData, _ := os.ReadFile("/leaked/base/16384/1259")
tables := pgdump.ParsePGClass(classData)
// => map[filenode]TableInfo{16815: {Name: "password", OID: 16527}, ...}

// Parse pg_attribute to get columns
attrData, _ := os.ReadFile("/leaked/base/16384/1249")
columns := pgdump.ParsePGAttribute(attrData, 0)
// => map[tableOID][]AttrInfo{16527: [{Name: "email", TypID: 1043}, ...]}

// Build schema and dump table
schema := []pgdump.Column{
    {Name: "email", TypID: pgdump.OidVarchar, Len: -1},
    {Name: "password_hash", TypID: pgdump.OidVarchar, Len: -1},
}
tableData, _ := os.ReadFile("/leaked/base/16384/16815")
rows := pgdump.ReadRows(tableData, schema, true)
```

### Integration with Custom File Readers

```go
// For use with HTTP-based file read vulnerabilities
result, err := pgdump.DumpDatabaseFromFiles(
    classData,  // pg_class content
    attrData,   // pg_attribute content
    func(filenode uint32) ([]byte, error) {
        // Your custom file reader (HTTP, LFI exploit, etc.)
        return httpReadFile(fmt.Sprintf("/base/16384/%d", filenode))
    },
    &pgdump.Options{TableFilter: "password"},
)
```

## Example Output

```json
{
  "databases": [
    {
      "oid": 16384,
      "name": "windmill",
      "tables": [
        {
          "name": "password",
          "columns": [
            {"name": "email", "type": "varchar"},
            {"name": "password_hash", "type": "varchar"}
          ],
          "rows": [
            {
              "email": "admin@windmill.dev",
              "password_hash": "$argon2id$v=19$m=4096,t=3,p=1$..."
            }
          ]
        }
      ]
    }
  ]
}
```

## Supported Types

- **Scalars**: bool, int2, int4, int8, float4, float8, text, varchar, name, char, oid
- **Date/Time**: date, time, timestamp, timestamptz, interval
- **Binary**: bytea, uuid
- **Network**: inet, macaddr
- **Complex**: jsonb (fully parsed), numeric, arrays

## File Locations

```
/var/lib/postgresql/data/           # Default Linux
/var/lib/postgresql/15/main/        # Debian/Ubuntu
C:\Program Files\PostgreSQL\data\   # Windows

global/1262                         # pg_database
base/<db_oid>/1259                  # pg_class  
base/<db_oid>/1249                  # pg_attribute
base/<db_oid>/<filenode>            # Table data
```

## Building

```bash
git clone https://github.com/chocapikk/pgdump-offline
cd pgdump-offline
go build

# Cross-compile
GOOS=windows GOARCH=amd64 go build -o pgdump-offline.exe
GOOS=darwin GOARCH=arm64 go build -o pgdump-offline-macos
GOOS=linux GOARCH=amd64 go build -o pgdump-offline-linux
```

## Credits

Based on research into PostgreSQL internals for the Windfall vulnerability chain.
Ported from Ruby (Metasploit Rex::Proto::PostgreSQL) to Go.

## License

MIT
