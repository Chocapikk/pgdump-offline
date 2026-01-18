package pgdump

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// RelMapMagic is the magic number for pg_filenode.map files
const RelMapMagic = 0x592717

// RelMapMaxMappings is the maximum number of mappings in the map file
const RelMapMaxMappings = 62

// RelMapping represents a single OID to filenode mapping
type RelMapping struct {
	OID      uint32 `json:"oid"`
	Filenode uint32 `json:"filenode"`
}

// RelMapFile represents a parsed pg_filenode.map file
type RelMapFile struct {
	Magic       uint32       `json:"magic"`
	NumMappings int32        `json:"num_mappings"`
	Mappings    []RelMapping `json:"mappings"`
	CRC         uint32       `json:"crc,omitempty"`
	IsGlobal    bool         `json:"is_global"`
	Path        string       `json:"path,omitempty"`
}

// ParseRelMapFile parses a pg_filenode.map file
// This file maps system catalog OIDs to their filenodes
// Structure:
//
//	magic (4 bytes) = 0x592717
//	num_mappings (4 bytes)
//	mappings[62] (8 bytes each = 496 bytes)
//	crc (4 bytes)
//
// Total size: 512 bytes
func ParseRelMapFile(data []byte) (*RelMapFile, error) {
	if len(data) < 512 {
		return nil, fmt.Errorf("relmap file too small: %d bytes (expected 512)", len(data))
	}

	rm := &RelMapFile{}

	// Read magic
	rm.Magic = binary.LittleEndian.Uint32(data[0:4])
	if rm.Magic != RelMapMagic {
		return nil, fmt.Errorf("invalid relmap magic: 0x%X (expected 0x%X)", rm.Magic, RelMapMagic)
	}

	// Read number of mappings
	rm.NumMappings = int32(binary.LittleEndian.Uint32(data[4:8]))
	if rm.NumMappings < 0 || rm.NumMappings > RelMapMaxMappings {
		return nil, fmt.Errorf("invalid number of mappings: %d", rm.NumMappings)
	}

	// Read mappings
	offset := 8
	for i := int32(0); i < rm.NumMappings; i++ {
		if offset+8 > len(data) {
			break
		}

		mapping := RelMapping{
			OID:      binary.LittleEndian.Uint32(data[offset : offset+4]),
			Filenode: binary.LittleEndian.Uint32(data[offset+4 : offset+8]),
		}
		rm.Mappings = append(rm.Mappings, mapping)
		offset += 8
	}

	// CRC is at offset 504 (after 62 mappings)
	crcOffset := 8 + RelMapMaxMappings*8
	if len(data) >= crcOffset+4 {
		rm.CRC = binary.LittleEndian.Uint32(data[crcOffset : crcOffset+4])
	}

	return rm, nil
}

// ReadGlobalRelMap reads the global pg_filenode.map
func ReadGlobalRelMap(dataDir string) (*RelMapFile, error) {
	path := filepath.Join(dataDir, "global", "pg_filenode.map")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read global relmap: %w", err)
	}

	rm, err := ParseRelMapFile(data)
	if err != nil {
		return nil, err
	}

	rm.IsGlobal = true
	rm.Path = path
	return rm, nil
}

// ReadDatabaseRelMap reads a database's pg_filenode.map
func ReadDatabaseRelMap(dataDir string, dbOID uint32) (*RelMapFile, error) {
	path := filepath.Join(dataDir, "base", fmt.Sprintf("%d", dbOID), "pg_filenode.map")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read database relmap: %w", err)
	}

	rm, err := ParseRelMapFile(data)
	if err != nil {
		return nil, err
	}

	rm.IsGlobal = false
	rm.Path = path
	return rm, nil
}

// GetFilenode returns the filenode for an OID, or 0 if not found
func (rm *RelMapFile) GetFilenode(oid uint32) uint32 {
	for _, m := range rm.Mappings {
		if m.OID == oid {
			return m.Filenode
		}
	}
	return 0
}

// GetOID returns the OID for a filenode, or 0 if not found
func (rm *RelMapFile) GetOID(filenode uint32) uint32 {
	for _, m := range rm.Mappings {
		if m.Filenode == filenode {
			return m.OID
		}
	}
	return 0
}

// RelMapInfo contains information about all relmap files in a cluster
type RelMapInfo struct {
	Global    *RelMapFile   `json:"global"`
	Databases []*RelMapFile `json:"databases,omitempty"`
}

// ReadAllRelMaps reads all pg_filenode.map files in the cluster
func ReadAllRelMaps(dataDir string) (*RelMapInfo, error) {
	info := &RelMapInfo{}

	// Read global relmap
	globalMap, err := ReadGlobalRelMap(dataDir)
	if err != nil {
		return nil, err
	}
	info.Global = globalMap

	// Read database list
	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return info, nil // Return with just global map
	}

	for _, db := range ParsePGDatabase(dbData) {
		dbMap, err := ReadDatabaseRelMap(dataDir, db.OID)
		if err != nil {
			continue
		}
		info.Databases = append(info.Databases, dbMap)
	}

	return info, nil
}

// Well-known system catalog OIDs that may be in the relmap
var SystemCatalogNames = map[uint32]string{
	1247: "pg_type",
	1249: "pg_attribute",
	1255: "pg_proc",
	1259: "pg_class",
	1260: "pg_authid",
	1261: "pg_auth_members",
	1262: "pg_database",
	2396: "pg_shdepend",
	2964: "pg_db_role_setting",
	3592: "pg_shseclabel",
	6000: "pg_replication_origin",
	6100: "pg_subscription",
	1213: "pg_tablespace",
	2847: "pg_pltemplate", // deprecated but may exist
	3602: "pg_transform",
}

// GetCatalogName returns the name for a well-known catalog OID
func GetCatalogName(oid uint32) string {
	if name, ok := SystemCatalogNames[oid]; ok {
		return name
	}
	return ""
}

// EnhancedRelMapping includes the catalog name if known
type EnhancedRelMapping struct {
	OID         uint32 `json:"oid"`
	Filenode    uint32 `json:"filenode"`
	CatalogName string `json:"catalog_name,omitempty"`
}

// GetEnhancedMappings returns mappings with catalog names filled in
func (rm *RelMapFile) GetEnhancedMappings() []EnhancedRelMapping {
	result := make([]EnhancedRelMapping, len(rm.Mappings))
	for i, m := range rm.Mappings {
		result[i] = EnhancedRelMapping{
			OID:         m.OID,
			Filenode:    m.Filenode,
			CatalogName: GetCatalogName(m.OID),
		}
	}
	return result
}
