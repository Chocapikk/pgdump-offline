package pgdump

import (
	"testing"
)

func TestGetSegmentNumberFromPath(t *testing.T) {
	tests := []struct {
		path string
		want int
	}{
		{"/path/to/16384", 0},
		{"/path/to/16384.1", 1},
		{"/path/to/16384.2", 2},
		{"/path/to/16384.10", 10},
		{"/path/to/file.txt", 0}, // Not a number
		{"16384", 0},
		{"16384.5", 5},
	}

	for _, tt := range tests {
		got := GetSegmentNumberFromPath(tt.path)
		if got != tt.want {
			t.Errorf("GetSegmentNumberFromPath(%q) = %d, want %d", tt.path, got, tt.want)
		}
	}
}

func TestGlobalBlockToSegment(t *testing.T) {
	// Default segment size = 1GB = 131072 blocks of 8KB
	segSize := DefaultSegmentSize
	blocksPerSeg := segSize / PageSize

	tests := []struct {
		globalBlock int
		wantSeg     int
		wantLocal   int
	}{
		{0, 0, 0},
		{100, 0, 100},
		{blocksPerSeg - 1, 0, blocksPerSeg - 1},
		{blocksPerSeg, 1, 0},
		{blocksPerSeg + 100, 1, 100},
		{2 * blocksPerSeg, 2, 0},
	}

	for _, tt := range tests {
		seg, local := GlobalBlockToSegment(tt.globalBlock, segSize)
		if seg != tt.wantSeg || local != tt.wantLocal {
			t.Errorf("GlobalBlockToSegment(%d, %d) = (%d, %d), want (%d, %d)",
				tt.globalBlock, segSize, seg, local, tt.wantSeg, tt.wantLocal)
		}
	}
}

func TestSegmentOptions(t *testing.T) {
	opts := &SegmentOptions{
		SegmentNumber: 5,
		SegmentSize:   128 * 1024 * 1024, // 128MB
	}

	if opts.SegmentNumber != 5 {
		t.Errorf("SegmentNumber = %d, want 5", opts.SegmentNumber)
	}
	if opts.SegmentSize != 128*1024*1024 {
		t.Errorf("SegmentSize = %d, want 134217728", opts.SegmentSize)
	}
}

func TestSegmentInfo(t *testing.T) {
	info := SegmentInfo{
		BasePath:      "/path/to/16384.1",
		SegmentNumber: 1,
		SegmentSize:   DefaultSegmentSize,
		FileSize:      int64(DefaultSegmentSize),
		TotalBlocks:   DefaultSegmentSize / PageSize,
		GlobalOffset:  int64(DefaultSegmentSize),
	}

	if info.SegmentNumber != 1 {
		t.Errorf("SegmentNumber = %d, want 1", info.SegmentNumber)
	}
	if info.GlobalOffset != int64(DefaultSegmentSize) {
		t.Errorf("GlobalOffset = %d, want %d", info.GlobalOffset, DefaultSegmentSize)
	}
}

func TestDefaultSegmentSize(t *testing.T) {
	// 1GB
	expected := 1024 * 1024 * 1024
	if DefaultSegmentSize != expected {
		t.Errorf("DefaultSegmentSize = %d, want %d", DefaultSegmentSize, expected)
	}
}
