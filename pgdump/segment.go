package pgdump

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DefaultSegmentSize is PostgreSQL's default segment size (1GB)
const DefaultSegmentSize = 1024 * 1024 * 1024

// SegmentOptions controls how multi-segment files are read
type SegmentOptions struct {
	SegmentNumber int // Force specific segment number (0 = auto-detect from filename)
	SegmentSize   int // Segment size in bytes (0 = default 1GB)
}

// SegmentInfo contains information about a file segment
type SegmentInfo struct {
	BasePath      string `json:"base_path"`
	SegmentNumber int    `json:"segment_number"`
	SegmentSize   int    `json:"segment_size"`
	FileSize      int64  `json:"file_size"`
	TotalBlocks   int    `json:"total_blocks"`
	GlobalOffset  int64  `json:"global_offset"` // Offset in the logical file
}

// GetSegmentNumberFromPath extracts segment number from filename
// PostgreSQL uses: base_filenode, base_filenode.1, base_filenode.2, etc.
func GetSegmentNumberFromPath(path string) int {
	base := filepath.Base(path)
	
	// Check if filename ends with .N where N is a number
	idx := strings.LastIndex(base, ".")
	if idx == -1 {
		return 0 // No extension = segment 0
	}
	
	suffix := base[idx+1:]
	if num, err := strconv.Atoi(suffix); err == nil {
		return num
	}
	
	return 0
}

// GetSegmentInfo returns information about a segment file
func GetSegmentInfo(path string, opts *SegmentOptions) (*SegmentInfo, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	segSize := DefaultSegmentSize
	segNum := GetSegmentNumberFromPath(path)
	
	if opts != nil {
		if opts.SegmentSize > 0 {
			segSize = opts.SegmentSize
		}
		if opts.SegmentNumber > 0 {
			segNum = opts.SegmentNumber
		}
	}

	return &SegmentInfo{
		BasePath:      path,
		SegmentNumber: segNum,
		SegmentSize:   segSize,
		FileSize:      stat.Size(),
		TotalBlocks:   int(stat.Size() / PageSize),
		GlobalOffset:  int64(segNum) * int64(segSize),
	}, nil
}

// ListSegments finds all segments for a given base file
func ListSegments(basePath string) ([]SegmentInfo, error) {
	var segments []SegmentInfo
	
	// Check base file (segment 0)
	if stat, err := os.Stat(basePath); err == nil {
		segments = append(segments, SegmentInfo{
			BasePath:      basePath,
			SegmentNumber: 0,
			SegmentSize:   DefaultSegmentSize,
			FileSize:      stat.Size(),
			TotalBlocks:   int(stat.Size() / PageSize),
			GlobalOffset:  0,
		})
	}
	
	// Check for additional segments (.1, .2, etc.)
	for i := 1; i < 1000; i++ { // Max 1000 segments (1TB)
		segPath := fmt.Sprintf("%s.%d", basePath, i)
		stat, err := os.Stat(segPath)
		if err != nil {
			break // No more segments
		}
		
		segments = append(segments, SegmentInfo{
			BasePath:      segPath,
			SegmentNumber: i,
			SegmentSize:   DefaultSegmentSize,
			FileSize:      stat.Size(),
			TotalBlocks:   int(stat.Size() / PageSize),
			GlobalOffset:  int64(i) * int64(DefaultSegmentSize),
		})
	}
	
	return segments, nil
}

// ReadSegmentBlock reads a specific block from a segment
func ReadSegmentBlock(path string, blockNum int, opts *SegmentOptions) ([]byte, error) {
	segInfo, err := GetSegmentInfo(path, opts)
	if err != nil {
		return nil, err
	}
	
	if blockNum >= segInfo.TotalBlocks {
		return nil, fmt.Errorf("block %d beyond segment size (%d blocks)", blockNum, segInfo.TotalBlocks)
	}
	
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	
	offset := int64(blockNum * PageSize)
	if _, err := f.Seek(offset, 0); err != nil {
		return nil, err
	}
	
	data := make([]byte, PageSize)
	n, err := f.Read(data)
	if err != nil {
		return nil, err
	}
	
	return data[:n], nil
}

// ReadMultiSegmentFile reads data from a potentially multi-segment file
// globalBlockStart and globalBlockEnd are block numbers in the logical file
func ReadMultiSegmentFile(basePath string, globalBlockStart, globalBlockEnd int, opts *SegmentOptions) ([]byte, error) {
	segments, err := ListSegments(basePath)
	if err != nil {
		return nil, err
	}
	
	if len(segments) == 0 {
		return nil, fmt.Errorf("no segments found for %s", basePath)
	}
	
	segSize := DefaultSegmentSize
	if opts != nil && opts.SegmentSize > 0 {
		segSize = opts.SegmentSize
	}
	blocksPerSegment := segSize / PageSize
	
	var result []byte
	
	for blockNum := globalBlockStart; blockNum <= globalBlockEnd; blockNum++ {
		// Determine which segment this block is in
		segIdx := blockNum / blocksPerSegment
		localBlock := blockNum % blocksPerSegment
		
		if segIdx >= len(segments) {
			break // Beyond available segments
		}
		
		block, err := ReadSegmentBlock(segments[segIdx].BasePath, localBlock, opts)
		if err != nil {
			break
		}
		
		result = append(result, block...)
	}
	
	return result, nil
}

// GlobalBlockToSegment converts a global block number to segment info
func GlobalBlockToSegment(globalBlock int, segmentSize int) (segmentNum, localBlock int) {
	if segmentSize <= 0 {
		segmentSize = DefaultSegmentSize
	}
	blocksPerSegment := segmentSize / PageSize
	segmentNum = globalBlock / blocksPerSegment
	localBlock = globalBlock % blocksPerSegment
	return
}
