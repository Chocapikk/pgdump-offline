package pgdump

import (
	"os"
	"path/filepath"
)

// AuthInfo contains PostgreSQL user authentication info
type AuthInfo struct {
	OID      uint32 `json:"oid"`
	RoleName string `json:"rolename"`
	Password string `json:"password,omitempty"`
	RolSuper bool   `json:"rolsuper"`
	RolLogin bool   `json:"rollogin"`
}

// ExtractPasswords extracts password hashes from pg_authid (global/1260)
func ExtractPasswords(dataDir string) ([]AuthInfo, error) {
	authFile := filepath.Join(dataDir, "global", "1260")
	data, err := os.ReadFile(authFile)
	if err != nil {
		return nil, err
	}
	return ParsePGAuthID(data), nil
}

// ParsePGAuthID parses pg_authid heap file
// pg_authid schema (simplified):
//   oid (4), rolname (name/64), rolsuper (bool), rolinherit (bool),
//   rolcreaterole (bool), rolcreatedb (bool), rolcanlogin (bool),
//   rolreplication (bool), rolbypassrls (bool), rolconnlimit (int4),
//   rolpassword (text), rolvaliduntil (timestamptz)
func ParsePGAuthID(data []byte) []AuthInfo {
	var results []AuthInfo

	for _, entry := range ReadTuples(data, false) { // Include dead tuples for forensics
		tuple := entry.Tuple
		if tuple == nil || len(tuple.Data) < 70 {
			continue
		}

		info := AuthInfo{}
		offset := 0

		// OID (4 bytes) - at start of tuple data for system catalogs
		if offset+4 <= len(tuple.Data) {
			info.OID = u32(tuple.Data, offset)
			offset += 4
		}

		// rolname - Name type (64 bytes, null-padded)
		if offset+64 <= len(tuple.Data) {
			info.RoleName = cstring(tuple.Data[offset:], 64)
			offset += 64
		}

		// Boolean flags (1 byte each)
		if offset+1 <= len(tuple.Data) {
			info.RolSuper = tuple.Data[offset] != 0
			offset++
		}

		// Skip: rolinherit, rolcreaterole, rolcreatedb
		offset += 3

		// rolcanlogin
		if offset+1 <= len(tuple.Data) {
			info.RolLogin = tuple.Data[offset] != 0
			offset++
		}

		// Skip: rolreplication, rolbypassrls (2 bytes)
		offset += 2

		// Align to 4 bytes for rolconnlimit (int4)
		offset = align(offset, 4)

		// Skip rolconnlimit (4 bytes)
		offset += 4

		// rolpassword - varlena (text)
		// Need to check null bitmap first
		if !tuple.IsNull(11) && offset < len(tuple.Data) {
			// Align for varlena
			offset = align(offset, 4)
			if offset < len(tuple.Data) {
				pwData, _ := ReadVarlena(tuple.Data[offset:])
				if len(pwData) > 0 {
					info.Password = string(pwData)
				}
			}
		}

		if info.RoleName != "" {
			results = append(results, info)
		}
	}

	return results
}

// ExtractPasswordsFromFiles extracts passwords using custom file reader
func ExtractPasswordsFromFiles(reader func(path string) ([]byte, error)) ([]AuthInfo, error) {
	data, err := reader("global/1260")
	if err != nil {
		return nil, err
	}
	return ParsePGAuthID(data), nil
}
