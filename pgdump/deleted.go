package pgdump

// DeletedRow represents a deleted but not vacuumed row
type DeletedRow struct {
	PageOffset int                    `json:"page_offset"`
	ItemOffset int                    `json:"item_offset"`
	Data       map[string]interface{} `json:"data,omitempty"`
	RawSize    int                    `json:"raw_size"`
}

// ReadDeletedRows scans for deleted tuples that haven't been vacuumed yet
// These are tuples where xmax is committed (meaning DELETE was committed)
func ReadDeletedRows(data []byte, columns []Column) []DeletedRow {
	var deleted []DeletedRow

	for _, entry := range ReadTuples(data, false) { // Get ALL tuples including dead
		tuple := entry.Tuple
		if tuple == nil {
			continue
		}

		// Check if this tuple is deleted (xmax committed, not xmax invalid)
		if tuple.Header.XmaxCommitted && !tuple.Header.XmaxInvalid {
			row := DeletedRow{
				PageOffset: entry.PageOffset,
				RawSize:    len(tuple.Data),
			}

			// Try to decode the data if we have schema
			if len(columns) > 0 {
				row.Data = DecodeTuple(tuple, columns)
			}

			deleted = append(deleted, row)
		}
	}

	return deleted
}

// ScanAllDeletedRows scans entire data directory for deleted rows
func ScanAllDeletedRows(dataDir string, opts *Options) (*DumpResult, error) {
	opts = withDefaults(opts)
	
	// Use regular dump but include deleted rows
	result, err := DumpDataDir(dataDir, opts)
	if err != nil {
		return nil, err
	}

	// Mark this as including deleted rows
	// The actual deleted row detection happens in ReadRows when we pass visibleOnly=false
	return result, nil
}

// ReadRowsWithDeleted returns both visible and deleted rows separately
func ReadRowsWithDeleted(data []byte, columns []Column) (visible []map[string]interface{}, deleted []map[string]interface{}) {
	for _, entry := range ReadTuples(data, false) {
		tuple := entry.Tuple
		if tuple == nil {
			continue
		}

		row := DecodeTuple(tuple, columns)
		if row == nil {
			continue
		}

		if tuple.IsVisible() {
			visible = append(visible, row)
		} else if tuple.Header.XmaxCommitted && !tuple.Header.XmaxInvalid {
			// Deleted but not vacuumed
			deleted = append(deleted, row)
		}
	}
	return
}
