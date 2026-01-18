package pgdump

import (
	"testing"
)

// === TOAST Tests ===

func TestDecompressLZ4Empty(t *testing.T) {
	_, err := decompressLZ4([]byte{}, 0)
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

func TestDecompressPGLZShort(t *testing.T) {
	_, err := decompressPGLZ([]byte{}, 0)
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

func TestNewTOASTReaderForDB(t *testing.T) {
	reader := NewTOASTReaderForDB("/tmp", 12345)
	if reader == nil {
		t.Fatal("NewTOASTReaderForDB returned nil")
	}
	if reader.chunks == nil {
		t.Error("chunks map not initialized")
	}
	if reader.dataDir != "/tmp" {
		t.Errorf("dataDir = %q, want /tmp", reader.dataDir)
	}
	if reader.dbOID != 12345 {
		t.Errorf("dbOID = %d, want 12345", reader.dbOID)
	}
}

// === Sequence Tests ===

func TestIsSequenceFile(t *testing.T) {
	// Not a sequence file (too small)
	if IsSequenceFile(make([]byte, 100)) {
		t.Error("Small file should not be sequence")
	}

	// Create a fake sequence page
	page := make([]byte, PageSize)
	// Set special pointer to point near end
	special := uint16(PageSize - 4)
	page[16] = byte(special)
	page[17] = byte(special >> 8)
	// Set magic number at special
	page[special] = 0x17
	page[special+1] = 0x17

	if !IsSequenceFile(page) {
		t.Error("Valid sequence page not detected")
	}
}

func TestSequenceDataStruct(t *testing.T) {
	seq := SequenceData{
		Name:        "test_seq",
		LastValue:   100,
		StartValue:  1,
		IncrementBy: 1,
		MaxValue:    9223372036854775807,
		MinValue:    1,
		CacheValue:  1,
		IsCycled:    false,
		IsCalled:    true,
	}

	if seq.Name != "test_seq" {
		t.Errorf("Name = %q, want test_seq", seq.Name)
	}
	if seq.LastValue != 100 {
		t.Errorf("LastValue = %d, want 100", seq.LastValue)
	}
}

// === RelMap Tests ===

func TestParseRelMapFile(t *testing.T) {
	// Create a valid relmap file
	data := make([]byte, 512)

	// Magic
	data[0] = 0x17
	data[1] = 0x27
	data[2] = 0x59
	data[3] = 0x00

	// Num mappings = 2
	data[4] = 0x02
	data[5] = 0x00
	data[6] = 0x00
	data[7] = 0x00

	// Mapping 1: OID=1262, filenode=1262
	data[8] = 0xEE
	data[9] = 0x04
	data[10] = 0x00
	data[11] = 0x00
	data[12] = 0xEE
	data[13] = 0x04
	data[14] = 0x00
	data[15] = 0x00

	// Mapping 2: OID=1259, filenode=1259
	data[16] = 0xEB
	data[17] = 0x04
	data[18] = 0x00
	data[19] = 0x00
	data[20] = 0xEB
	data[21] = 0x04
	data[22] = 0x00
	data[23] = 0x00

	rm, err := ParseRelMapFile(data)
	if err != nil {
		t.Fatalf("ParseRelMapFile failed: %v", err)
	}

	if rm.Magic != RelMapMagic {
		t.Errorf("Magic = 0x%X, want 0x%X", rm.Magic, RelMapMagic)
	}
	if rm.NumMappings != 2 {
		t.Errorf("NumMappings = %d, want 2", rm.NumMappings)
	}
	if len(rm.Mappings) != 2 {
		t.Errorf("len(Mappings) = %d, want 2", len(rm.Mappings))
	}
}

func TestParseRelMapFileTooSmall(t *testing.T) {
	_, err := ParseRelMapFile(make([]byte, 100))
	if err == nil {
		t.Error("Expected error for small file")
	}
}

func TestParseRelMapFileInvalidMagic(t *testing.T) {
	data := make([]byte, 512)
	data[0] = 0xFF // Invalid magic
	_, err := ParseRelMapFile(data)
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestRelMapGetFilenode(t *testing.T) {
	rm := &RelMapFile{
		Mappings: []RelMapping{
			{OID: 1262, Filenode: 1262},
			{OID: 1259, Filenode: 16384},
		},
	}

	if fn := rm.GetFilenode(1259); fn != 16384 {
		t.Errorf("GetFilenode(1259) = %d, want 16384", fn)
	}
	if fn := rm.GetFilenode(9999); fn != 0 {
		t.Errorf("GetFilenode(9999) = %d, want 0", fn)
	}
}

func TestRelMapGetOID(t *testing.T) {
	rm := &RelMapFile{
		Mappings: []RelMapping{
			{OID: 1262, Filenode: 1262},
			{OID: 1259, Filenode: 16384},
		},
	}

	if oid := rm.GetOID(16384); oid != 1259 {
		t.Errorf("GetOID(16384) = %d, want 1259", oid)
	}
}

func TestGetCatalogName(t *testing.T) {
	tests := []struct {
		oid  uint32
		want string
	}{
		{1262, "pg_database"},
		{1259, "pg_class"},
		{1249, "pg_attribute"},
		{1260, "pg_authid"},
		{9999, ""},
	}

	for _, tt := range tests {
		got := GetCatalogName(tt.oid)
		if got != tt.want {
			t.Errorf("GetCatalogName(%d) = %q, want %q", tt.oid, got, tt.want)
		}
	}
}

// === Block Range Tests ===

func TestParseBlockRange(t *testing.T) {
	tests := []struct {
		input     string
		wantStart int
		wantEnd   int
		wantErr   bool
	}{
		{"", -1, -1, false},
		{"5", 5, 5, false},
		{"0:10", 0, 10, false},
		{"5:", 5, -1, false},
		{":20", -1, 20, false},
		{"10:5", -1, -1, true}, // Invalid: start > end
		{"-1:5", -1, -1, true}, // Invalid: negative
		{"abc", -1, -1, true},  // Invalid: not a number
	}

	for _, tt := range tests {
		br, err := ParseBlockRange(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseBlockRange(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseBlockRange(%q): unexpected error: %v", tt.input, err)
			continue
		}
		if tt.input == "" {
			if br != nil {
				t.Errorf("ParseBlockRange(%q): expected nil", tt.input)
			}
			continue
		}
		if br.Start != tt.wantStart {
			t.Errorf("ParseBlockRange(%q).Start = %d, want %d", tt.input, br.Start, tt.wantStart)
		}
		if br.End != tt.wantEnd {
			t.Errorf("ParseBlockRange(%q).End = %d, want %d", tt.input, br.End, tt.wantEnd)
		}
	}
}

func TestParseBlockInfo(t *testing.T) {
	// Create a valid page
	page := make([]byte, PageSize)

	// LSN
	page[0] = 0x01
	page[1] = 0x00
	page[2] = 0x00
	page[3] = 0x00
	page[4] = 0x00
	page[5] = 0x00
	page[6] = 0x00
	page[7] = 0x00

	// Checksum
	page[8] = 0x12
	page[9] = 0x34

	// Lower = 28 (after header)
	page[12] = 28
	page[13] = 0

	// Upper = 8000
	page[14] = 0x40
	page[15] = 0x1F

	// Special = 8192
	page[16] = 0x00
	page[17] = 0x20

	// Page size + version (8192 | 4)
	page[18] = 0x04
	page[19] = 0x20

	info := ParseBlockInfo(page, 0)
	if info == nil {
		t.Fatal("ParseBlockInfo returned nil")
	}

	if info.BlockNumber != 0 {
		t.Errorf("BlockNumber = %d, want 0", info.BlockNumber)
	}
	if info.Checksum != 0x3412 {
		t.Errorf("Checksum = 0x%X, want 0x3412", info.Checksum)
	}
	if info.PageSize != 8192 {
		t.Errorf("PageSize = %d, want 8192", info.PageSize)
	}
	if info.Version != 4 {
		t.Errorf("Version = %d, want 4", info.Version)
	}
}

func TestParseBlockInfoEmpty(t *testing.T) {
	page := make([]byte, PageSize) // All zeros

	info := ParseBlockInfo(page, 0)
	if info == nil {
		t.Fatal("ParseBlockInfo returned nil")
	}
	if !info.IsEmpty {
		t.Error("Expected IsEmpty = true for zero page")
	}
}

func TestBlockRangeStats(t *testing.T) {
	stats := BlockRangeStats{
		TotalBlocks: 10,
		StartBlock:  0,
		EndBlock:    9,
		EmptyBlocks: 2,
		UsedBlocks:  8,
		TotalItems:  100,
		TotalFree:   50000,
		AvgFillPct:  75.5,
	}

	if stats.TotalBlocks != 10 {
		t.Errorf("TotalBlocks = %d, want 10", stats.TotalBlocks)
	}
	if stats.UsedBlocks != 8 {
		t.Errorf("UsedBlocks = %d, want 8", stats.UsedBlocks)
	}
}

func TestBinaryBlockDump(t *testing.T) {
	dump := BinaryBlockDump{
		BlockNumber: 0,
		Offset:      0,
		HexDump:     "00000000  00 00 00 00  |....|",
		Size:        4,
	}

	if dump.BlockNumber != 0 {
		t.Errorf("BlockNumber = %d, want 0", dump.BlockNumber)
	}
	if dump.Size != 4 {
		t.Errorf("Size = %d, want 4", dump.Size)
	}
}

func TestFormatBinaryDump(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02, 0x03}
	dump := FormatBinaryDump(data)
	if dump == "" {
		t.Error("FormatBinaryDump returned empty string")
	}
	if len(dump) < 10 {
		t.Error("FormatBinaryDump output too short")
	}
}

// === TOAST Verbose Tests ===

func TestTOASTVerboseInfo(t *testing.T) {
	info := TOASTVerboseInfo{
		ToastRelID:        16385,
		TotalChunks:       10,
		UniqueValues:      3,
		TotalSize:         50000,
		AverageChunkSize:  5000,
		MaxChunksPerValue: 5,
		ChunkDistribution: map[int]int{2: 1, 3: 1, 5: 1},
	}

	if info.ToastRelID != 16385 {
		t.Errorf("ToastRelID = %d, want 16385", info.ToastRelID)
	}
	if info.MaxChunksPerValue != 5 {
		t.Errorf("MaxChunksPerValue = %d, want 5", info.MaxChunksPerValue)
	}
}

func TestCompressionStats(t *testing.T) {
	stats := CompressionStats{
		Compressed:     5,
		Uncompressed:   3,
		TotalRawSize:   100000,
		TotalExtSize:   50000,
		CompressionPct: 50.0,
	}

	if stats.Compressed != 5 {
		t.Errorf("Compressed = %d, want 5", stats.Compressed)
	}
	if stats.CompressionPct != 50.0 {
		t.Errorf("CompressionPct = %f, want 50.0", stats.CompressionPct)
	}
}

func TestTOASTValueInfo(t *testing.T) {
	info := TOASTValueInfo{
		ChunkID:      12345,
		NumChunks:    3,
		TotalSize:    15000,
		IsCompressed: true,
		RawSize:      20000,
		ExtSize:      15000,
	}

	if info.ChunkID != 12345 {
		t.Errorf("ChunkID = %d, want 12345", info.ChunkID)
	}
	if !info.IsCompressed {
		t.Error("Expected IsCompressed = true")
	}
}
