package pgdump

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// BlockRange represents a range of blocks to read
type BlockRange struct {
	Start int // -1 means from beginning
	End   int // -1 means to end
}

// ParseBlockRange parses a block range string like "0:10" or "5:" or ":20" or "5"
func ParseBlockRange(s string) (*BlockRange, error) {
	if s == "" {
		return nil, nil
	}

	br := &BlockRange{Start: -1, End: -1}

	if strings.Contains(s, ":") {
		parts := strings.SplitN(s, ":", 2)
		
		if parts[0] != "" {
			start, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, fmt.Errorf("invalid start block: %s", parts[0])
			}
			if start < 0 {
				return nil, fmt.Errorf("start block cannot be negative")
			}
			br.Start = start
		}
		
		if parts[1] != "" {
			end, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid end block: %s", parts[1])
			}
			if end < 0 {
				return nil, fmt.Errorf("end block cannot be negative")
			}
			br.End = end
		}
	} else {
		// Single block number
		block, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("invalid block number: %s", s)
		}
		if block < 0 {
			return nil, fmt.Errorf("block number cannot be negative")
		}
		br.Start = block
		br.End = block
	}

	// Validate range
	if br.Start >= 0 && br.End >= 0 && br.Start > br.End {
		return nil, fmt.Errorf("start block (%d) cannot be greater than end block (%d)", br.Start, br.End)
	}

	return br, nil
}

// ReadBlockRange reads a specific range of blocks from a file
func ReadBlockRange(path string, blockRange *BlockRange) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	totalBlocks := int(fileSize / PageSize)

	// Determine actual range
	start := 0
	end := totalBlocks - 1

	if blockRange != nil {
		if blockRange.Start >= 0 {
			start = blockRange.Start
		}
		if blockRange.End >= 0 {
			end = blockRange.End
		}
	}

	// Validate bounds
	if start >= totalBlocks {
		return nil, fmt.Errorf("start block %d beyond file size (%d blocks)", start, totalBlocks)
	}
	if end >= totalBlocks {
		end = totalBlocks - 1
	}
	if start > end {
		return nil, fmt.Errorf("invalid range: %d > %d", start, end)
	}

	// Calculate bytes to read
	startOffset := int64(start * PageSize)
	numBlocks := end - start + 1
	bytesToRead := numBlocks * PageSize

	// Seek to start
	if _, err := f.Seek(startOffset, 0); err != nil {
		return nil, err
	}

	// Read blocks
	data := make([]byte, bytesToRead)
	n, err := f.Read(data)
	if err != nil {
		return nil, err
	}

	return data[:n], nil
}

// BlockInfo contains information about a single block
type BlockInfo struct {
	BlockNumber uint32 `json:"block_number"`
	LSN         string `json:"lsn"`
	Checksum    uint16 `json:"checksum"`
	Flags       uint16 `json:"flags"`
	Lower       uint16 `json:"lower"`
	Upper       uint16 `json:"upper"`
	Special     uint16 `json:"special"`
	PageSize    int    `json:"page_size"`
	Version     int    `json:"version"`
	ItemCount   int    `json:"item_count"`
	FreeSpace   int    `json:"free_space"`
	IsEmpty     bool   `json:"is_empty,omitempty"`
}

// ParseBlockInfo extracts information about a single block
func ParseBlockInfo(data []byte, blockNumber uint32) *BlockInfo {
	if len(data) < PageSize {
		return nil
	}

	info := &BlockInfo{
		BlockNumber: blockNumber,
	}

	// Check if page is all zeros
	isEmpty := true
	for _, b := range data[:PageSize] {
		if b != 0 {
			isEmpty = false
			break
		}
	}
	if isEmpty {
		info.IsEmpty = true
		return info
	}

	// Parse page header
	lsn := u64(data, 0)
	info.LSN = FormatLSN(lsn)
	info.Checksum = u16(data, 8)
	info.Flags = u16(data, 10)
	info.Lower = u16(data, 12)
	info.Upper = u16(data, 14)
	info.Special = u16(data, 16)

	psv := u16(data, 18)
	info.PageSize = int(psv & 0xFF00)
	info.Version = int(psv & 0x00FF)

	// Calculate item count and free space
	if info.Lower >= headerSize {
		info.ItemCount = (int(info.Lower) - headerSize) / itemIDSize
	}
	if info.Upper > info.Lower {
		info.FreeSpace = int(info.Upper) - int(info.Lower)
	}

	return info
}

// DumpBlockRange dumps information about blocks in a range
func DumpBlockRange(path string, blockRange *BlockRange) ([]BlockInfo, error) {
	data, err := ReadBlockRange(path, blockRange)
	if err != nil {
		return nil, err
	}

	startBlock := 0
	if blockRange != nil && blockRange.Start >= 0 {
		startBlock = blockRange.Start
	}

	var blocks []BlockInfo
	for i := 0; i < len(data)/PageSize; i++ {
		offset := i * PageSize
		block := data[offset : offset+PageSize]
		
		info := ParseBlockInfo(block, uint32(startBlock+i))
		if info != nil {
			blocks = append(blocks, *info)
		}
	}

	return blocks, nil
}

// ReadTuplesInRange reads tuples from a specific block range
func ReadTuplesInRange(path string, blockRange *BlockRange, includeDeleted bool) ([]TupleEntry, error) {
	data, err := ReadBlockRange(path, blockRange)
	if err != nil {
		return nil, err
	}

	return ReadTuples(data, includeDeleted), nil
}

// BlockRangeStats contains statistics about a block range
type BlockRangeStats struct {
	Path        string `json:"path"`
	TotalBlocks int    `json:"total_blocks"`
	StartBlock  int    `json:"start_block"`
	EndBlock    int    `json:"end_block"`
	EmptyBlocks int    `json:"empty_blocks"`
	UsedBlocks  int    `json:"used_blocks"`
	TotalItems  int    `json:"total_items"`
	TotalFree   int64  `json:"total_free_space"`
	AvgFillPct  float64 `json:"avg_fill_percent"`
}

// GetBlockRangeStats calculates statistics for a block range
func GetBlockRangeStats(path string, blockRange *BlockRange) (*BlockRangeStats, error) {
	blocks, err := DumpBlockRange(path, blockRange)
	if err != nil {
		return nil, err
	}

	stats := &BlockRangeStats{
		Path:        path,
		TotalBlocks: len(blocks),
	}

	if len(blocks) == 0 {
		return stats, nil
	}

	stats.StartBlock = int(blocks[0].BlockNumber)
	stats.EndBlock = int(blocks[len(blocks)-1].BlockNumber)

	var totalUsed int64
	for _, b := range blocks {
		if b.IsEmpty {
			stats.EmptyBlocks++
		} else {
			stats.UsedBlocks++
			stats.TotalItems += b.ItemCount
			stats.TotalFree += int64(b.FreeSpace)
			if b.PageSize > 0 {
				usedSpace := b.PageSize - b.FreeSpace
				totalUsed += int64(usedSpace)
			}
		}
	}

	if stats.UsedBlocks > 0 {
		totalCapacity := int64(stats.UsedBlocks * PageSize)
		if totalCapacity > 0 {
			stats.AvgFillPct = float64(totalUsed) / float64(totalCapacity) * 100
		}
	}

	return stats, nil
}

// BinaryBlockDump represents a hex dump of a block
type BinaryBlockDump struct {
	BlockNumber uint32 `json:"block_number"`
	Offset      int64  `json:"offset"`
	HexDump     string `json:"hex_dump"`
	Size        int    `json:"size"`
}

// DumpBinaryBlock returns a hex dump of a specific block
func DumpBinaryBlock(path string, blockNum int) (*BinaryBlockDump, error) {
	br := &BlockRange{Start: blockNum, End: blockNum}
	data, err := ReadBlockRange(path, br)
	if err != nil {
		return nil, err
	}

	return &BinaryBlockDump{
		BlockNumber: uint32(blockNum),
		Offset:      int64(blockNum * PageSize),
		HexDump:     hex.Dump(data),
		Size:        len(data),
	}, nil
}

// DumpBinaryRange returns hex dumps for a range of blocks
func DumpBinaryRange(path string, blockRange *BlockRange) ([]BinaryBlockDump, error) {
	data, err := ReadBlockRange(path, blockRange)
	if err != nil {
		return nil, err
	}

	startBlock := 0
	if blockRange != nil && blockRange.Start >= 0 {
		startBlock = blockRange.Start
	}

	var dumps []BinaryBlockDump
	for i := 0; i < len(data)/PageSize; i++ {
		offset := i * PageSize
		block := data[offset : offset+PageSize]

		dumps = append(dumps, BinaryBlockDump{
			BlockNumber: uint32(startBlock + i),
			Offset:      int64((startBlock + i) * PageSize),
			HexDump:     hex.Dump(block),
			Size:        PageSize,
		})
	}

	return dumps, nil
}

// FormatBinaryDump formats data as a classic hex dump (like xxd or hexdump)
func FormatBinaryDump(data []byte) string {
	return hex.Dump(data)
}
