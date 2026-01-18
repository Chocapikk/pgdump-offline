package pgdump

import (
	"fmt"
	"regexp"
	"strings"
)

// SearchResult represents a match found during search
type SearchResult struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Column   string                 `json:"column"`
	RowNum   int                    `json:"row_num"`
	Value    interface{}            `json:"value"`
	Row      map[string]interface{} `json:"row,omitempty"`
}

// SearchOptions configures the search behavior
type SearchOptions struct {
	Pattern       string // Regex pattern to search for
	CaseSensitive bool   // Case-sensitive search
	IncludeRow    bool   // Include full row in results
	MaxResults    int    // Maximum results (0 = unlimited)
}

// Search searches across all databases and tables for a pattern
func Search(dataDir string, opts *SearchOptions) ([]SearchResult, error) {
	if opts == nil {
		return nil, fmt.Errorf("search options required")
	}

	// Compile regex
	pattern := opts.Pattern
	if !opts.CaseSensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}

	// Dump everything
	result, err := DumpDataDir(dataDir, &Options{SkipSystemTables: true})
	if err != nil {
		return nil, err
	}

	var matches []SearchResult

	for _, db := range result.Databases {
		for _, table := range db.Tables {
			for rowNum, row := range table.Rows {
				for colName, value := range row {
					if matchValue(value, re) {
						match := SearchResult{
							Database: db.Name,
							Table:    table.Name,
							Column:   colName,
							RowNum:   rowNum,
							Value:    value,
						}
						if opts.IncludeRow {
							match.Row = row
						}
						matches = append(matches, match)

						if opts.MaxResults > 0 && len(matches) >= opts.MaxResults {
							return matches, nil
						}
					}
				}
			}
		}
	}

	return matches, nil
}

// SearchInDump searches within an already-loaded dump result
func SearchInDump(result *DumpResult, opts *SearchOptions) ([]SearchResult, error) {
	if opts == nil {
		return nil, fmt.Errorf("search options required")
	}

	pattern := opts.Pattern
	if !opts.CaseSensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}

	var matches []SearchResult

	for _, db := range result.Databases {
		for _, table := range db.Tables {
			for rowNum, row := range table.Rows {
				for colName, value := range row {
					if matchValue(value, re) {
						match := SearchResult{
							Database: db.Name,
							Table:    table.Name,
							Column:   colName,
							RowNum:   rowNum,
							Value:    value,
						}
						if opts.IncludeRow {
							match.Row = row
						}
						matches = append(matches, match)

						if opts.MaxResults > 0 && len(matches) >= opts.MaxResults {
							return matches, nil
						}
					}
				}
			}
		}
	}

	return matches, nil
}

// matchValue checks if a value matches the regex
func matchValue(value interface{}, re *regexp.Regexp) bool {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case string:
		return re.MatchString(v)
	case []byte:
		return re.Match(v)
	case map[string]interface{}:
		// Search in JSON/JSONB
		return matchMap(v, re)
	case []interface{}:
		// Search in arrays
		for _, elem := range v {
			if matchValue(elem, re) {
				return true
			}
		}
	default:
		// Convert to string and search
		str := fmt.Sprintf("%v", v)
		return re.MatchString(str)
	}
	return false
}

// matchMap recursively searches in a map
func matchMap(m map[string]interface{}, re *regexp.Regexp) bool {
	for key, val := range m {
		if re.MatchString(key) {
			return true
		}
		if matchValue(val, re) {
			return true
		}
	}
	return false
}

// QuickSearch is a convenience function for simple string search
func QuickSearch(dataDir, pattern string) ([]SearchResult, error) {
	return Search(dataDir, &SearchOptions{
		Pattern:       regexp.QuoteMeta(pattern),
		CaseSensitive: false,
		IncludeRow:    true,
	})
}

// SearchSecrets searches for common secret patterns
func SearchSecrets(dataDir string) ([]SearchResult, error) {
	patterns := []string{
		// API keys and tokens
		`(?i)(api[_-]?key|api[_-]?secret|access[_-]?key|secret[_-]?key)`,
		`(?i)(bearer|token|jwt|auth)`,
		// AWS
		`AKIA[0-9A-Z]{16}`,
		`(?i)aws[_-]?(access|secret)`,
		// Passwords
		`(?i)(password|passwd|pwd|secret)`,
		// Private keys
		`-----BEGIN (RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----`,
		// Connection strings
		`(?i)(postgres|mysql|mongodb|redis)://`,
		// Credit cards (basic pattern)
		`\b[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}\b`,
	}

	combinedPattern := strings.Join(patterns, "|")
	return Search(dataDir, &SearchOptions{
		Pattern:       combinedPattern,
		CaseSensitive: false,
		IncludeRow:    true,
	})
}
