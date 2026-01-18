// pgdump-offline - Dump PostgreSQL data from leaked heap files
//
// Usage:
//
//	pgdump-offline -d /path/to/pg_data/         # Auto-discover and dump all
//	pgdump-offline -f /path/to/file             # Parse single heap file
//	pgdump-offline -d /path/to/pg_data/ -db windmill -t password  # Filter
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chocapikk/pgdump-offline/pgdump"
)

func main() {
	var (
		dataDir      string
		singleFile   string
		databaseName string
		tableFilter  string
		outputJSON   bool
		listOnly     bool
		verbose      bool
	)

	flag.StringVar(&dataDir, "d", "", "PostgreSQL data directory")
	flag.StringVar(&singleFile, "f", "", "Single heap file to parse")
	flag.StringVar(&databaseName, "db", "", "Filter by database name")
	flag.StringVar(&tableFilter, "t", "", "Filter tables containing this string")
	flag.BoolVar(&outputJSON, "json", true, "Output as JSON")
	flag.BoolVar(&listOnly, "list", false, "List databases/tables only")
	flag.BoolVar(&verbose, "v", false, "Verbose output")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `pgdump-offline - Dump PostgreSQL data from leaked heap files

Usage:
  %s -d /path/to/pg_data/                    # Auto-discover and dump all
  %s -d /path/to/pg_data/ -list              # List databases and tables
  %s -d /path/to/pg_data/ -db windmill       # Dump specific database
  %s -d /path/to/pg_data/ -t password        # Dump tables matching filter
  %s -f /path/to/1259                        # Parse single file (pg_class)

Fixed OIDs (works on any PostgreSQL):
  1262 - pg_database (global/1262)
  1259 - pg_class    (base/<db_oid>/1259)  
  1249 - pg_attribute (base/<db_oid>/1249)

Options:
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if singleFile != "" {
		parseSingleFile(singleFile)
		return
	}

	if dataDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -d (data directory) or -f (single file) required")
		flag.Usage()
		os.Exit(1)
	}

	opts := &pgdump.Options{
		DatabaseFilter:   databaseName,
		TableFilter:      tableFilter,
		ListOnly:         listOnly,
		SkipSystemTables: true,
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "[*] Scanning %s\n", dataDir)
	}

	result, err := pgdump.DumpDataDir(dataDir, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		for _, db := range result.Databases {
			fmt.Fprintf(os.Stderr, "[*] Database: %s (OID: %d) - %d tables\n",
				db.Name, db.OID, len(db.Tables))
		}
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
	}
}

func parseSingleFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	filename := filepath.Base(path)

	switch filename {
	case "1262":
		fmt.Println("Detected: pg_database (global)")
		dbs := pgdump.ParsePGDatabase(data)
		for _, db := range dbs {
			fmt.Printf("  Database: %s (OID: %d)\n", db.Name, db.OID)
		}

	case "1259":
		fmt.Println("Detected: pg_class")
		tables := pgdump.ParsePGClass(data)
		for _, t := range tables {
			fmt.Printf("  Table: %s (OID: %d, filenode: %d, kind: %s)\n",
				t.Name, t.OID, t.Filenode, t.Kind)
		}

	case "1249":
		fmt.Println("Detected: pg_attribute")
		attrs := pgdump.ParsePGAttribute(data, 0)
		for relid, cols := range attrs {
			fmt.Printf("  Relation %d:\n", relid)
			for _, c := range cols {
				fmt.Printf("    %d: %s (%s)\n", c.Num, c.Name, pgdump.TypeName(c.TypID))
			}
		}

	default:
		fmt.Println("Generic heap file - extracting tuples")
		tuples := pgdump.ParseFile(data)
		fmt.Printf("Found %d tuples\n", len(tuples))
		for i, t := range tuples {
			if i >= 10 {
				fmt.Printf("... and %d more\n", len(tuples)-10)
				break
			}
			fmt.Printf("Tuple %d: %d bytes\n", i, len(t.Tuple.Data))
		}
	}
}
