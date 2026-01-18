package pgdump

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

// ToCSV writes the dump result as CSV to the writer
// Each table becomes a separate CSV section with a header
func (r *DumpResult) ToCSV(w io.Writer) error {
	for _, db := range r.Databases {
		if err := db.ToCSV(w); err != nil {
			return err
		}
	}
	return nil
}

// ToCSV writes a single database dump as CSV
func (d *DatabaseDump) ToCSV(w io.Writer) error {
	for _, table := range d.Tables {
		fmt.Fprintf(w, "# Database: %s, Table: %s\n", d.Name, table.Name)
		if err := table.ToCSV(w); err != nil {
			return err
		}
		fmt.Fprintln(w) // Empty line between tables
	}
	return nil
}

// ToCSV writes a single table as CSV
func (t *TableDump) ToCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	// Write header
	if len(t.Columns) == 0 {
		return nil
	}

	header := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		header[i] = col.Name
	}
	if err := cw.Write(header); err != nil {
		return err
	}

	// Write rows
	for _, row := range t.Rows {
		record := make([]string, len(t.Columns))
		for i, col := range t.Columns {
			val, ok := row[col.Name]
			if !ok || val == nil {
				record[i] = ""
			} else {
				record[i] = formatCSVValue(val)
			}
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// formatCSVValue formats a Go value as a CSV string
func formatCSVValue(val interface{}) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case bool:
		if v {
			return "true"
		}
		return "false"

	case int, int16, int32, int64, uint32:
		return fmt.Sprintf("%d", v)

	case float32, float64:
		return fmt.Sprintf("%v", v)

	case string:
		return v

	case []byte:
		return string(v)

	case []interface{}:
		// Arrays - JSON encode
		b, _ := json.Marshal(v)
		return string(b)

	case map[string]interface{}:
		// JSON/JSONB - JSON encode
		b, _ := json.Marshal(v)
		return string(b)

	default:
		return fmt.Sprintf("%v", v)
	}
}

// TableToCSV is a convenience function to export a single table
func TableToCSV(w io.Writer, table TableDump) error {
	return table.ToCSV(w)
}

// WriteCSVFile writes each table to a separate CSV section
func WriteCSVFile(w io.Writer, result *DumpResult) error {
	return result.ToCSV(w)
}
