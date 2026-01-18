package pgdump

import (
	"bytes"
	"compress/zlib"
	"io"
	"sort"
)

// TOASTPointer represents a TOAST pointer in PostgreSQL
// Format: varatt_external (18 bytes):
//   va_rawsize (4), va_extsize (4), va_valueid (4), va_toastrelid (4), va_padding (2)
type TOASTPointer struct {
	RawSize     uint32 // Original uncompressed size
	ExtSize     uint32 // External (compressed) size  
	ValueID     uint32 // chunk_id in TOAST table
	ToastRelID  uint32 // OID of TOAST table
	IsCompressed bool
}

// TOASTChunk represents a chunk from a TOAST table
type TOASTChunk struct {
	ChunkID  uint32
	ChunkSeq int32
	Data     []byte
}

// ParseTOASTPointer extracts TOAST pointer info from a varlena value
// TOAST pointers have first byte = 0x01 (external) or 0x02 (compressed external)
func ParseTOASTPointer(data []byte) *TOASTPointer {
	if len(data) < 18 {
		return nil
	}

	// Check for TOAST pointer indicator
	// Byte 0: 0x01 = external ondisk, 0x02 = compressed external
	tag := data[0]
	if tag != 0x01 && tag != 0x12 && tag != 0x02 {
		return nil
	}

	return &TOASTPointer{
		RawSize:      u32(data, 4),
		ExtSize:      u32(data, 8),
		ValueID:      u32(data, 12),
		ToastRelID:   u32(data, 16),
		IsCompressed: tag == 0x02 || tag == 0x12,
	}
}

// IsTOASTPointer checks if data is a TOAST pointer
func IsTOASTPointer(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	// First byte: 0x01 or 0x02 indicates external storage
	// But we also check it's not a short varlena
	first := data[0]
	return first == 0x01 || first == 0x02 || first == 0x12
}

// ReadTOASTTable reads all chunks from a TOAST table file
func ReadTOASTTable(data []byte) []TOASTChunk {
	var chunks []TOASTChunk

	// TOAST table schema:
	// chunk_id (oid/4), chunk_seq (int4/4), chunk_data (bytea/varlena)
	for _, entry := range ReadTuples(data, true) {
		tuple := entry.Tuple
		if tuple == nil || len(tuple.Data) < 8 {
			continue
		}

		chunk := TOASTChunk{}
		offset := 0

		// chunk_id (oid, 4 bytes)
		chunk.ChunkID = u32(tuple.Data, offset)
		offset += 4

		// chunk_seq (int4, 4 bytes)
		chunk.ChunkSeq = i32(tuple.Data, offset)
		offset += 4

		// chunk_data (bytea, varlena)
		// Align to 4 bytes for varlena
		offset = align(offset, 4)
		if offset < len(tuple.Data) {
			chunkData, _ := ReadVarlena(tuple.Data[offset:])
			chunk.Data = chunkData
		}

		if len(chunk.Data) > 0 {
			chunks = append(chunks, chunk)
		}
	}

	return chunks
}

// ReassembleTOAST reconstructs a value from TOAST chunks
func ReassembleTOAST(chunks []TOASTChunk, valueID uint32, compressed bool, rawSize uint32) []byte {
	// Filter and sort chunks for this value
	var valueChunks []TOASTChunk
	for _, c := range chunks {
		if c.ChunkID == valueID {
			valueChunks = append(valueChunks, c)
		}
	}

	if len(valueChunks) == 0 {
		return nil
	}

	// Sort by sequence number
	sort.Slice(valueChunks, func(i, j int) bool {
		return valueChunks[i].ChunkSeq < valueChunks[j].ChunkSeq
	})

	// Concatenate all chunks
	var buf bytes.Buffer
	for _, c := range valueChunks {
		buf.Write(c.Data)
	}

	data := buf.Bytes()

	// Decompress if needed (PostgreSQL uses pglz or lz4, but zlib is common)
	if compressed && len(data) > 0 {
		decompressed, err := decompressPGLZ(data, int(rawSize))
		if err == nil {
			return decompressed
		}
		// If pglz fails, try zlib
		if r, err := zlib.NewReader(bytes.NewReader(data)); err == nil {
			defer r.Close()
			if decompressed, err := io.ReadAll(r); err == nil {
				return decompressed
			}
		}
		// Return compressed data if decompression fails
		return data
	}

	return data
}

// decompressPGLZ decompresses PostgreSQL's pglz format
func decompressPGLZ(data []byte, rawSize int) ([]byte, error) {
	if len(data) < 4 {
		return data, nil
	}

	result := make([]byte, 0, rawSize)
	pos := 0

	for pos < len(data) && len(result) < rawSize {
		ctrl := data[pos]
		pos++

		for bit := 0; bit < 8 && pos < len(data) && len(result) < rawSize; bit++ {
			if ctrl&(1<<bit) != 0 {
				// Back-reference
				if pos+1 >= len(data) {
					break
				}
				// Read offset and length
				b1, b2 := data[pos], data[pos+1]
				pos += 2

				offset := int(b1) | (int(b2&0xF0) << 4)
				length := int(b2&0x0F) + 3

				if offset == 0 || offset > len(result) {
					continue
				}

				// Copy from back-reference
				start := len(result) - offset
				for i := 0; i < length && len(result) < rawSize; i++ {
					result = append(result, result[start+i%offset])
				}
			} else {
				// Literal byte
				result = append(result, data[pos])
				pos++
			}
		}
	}

	return result, nil
}

// TOASTReader provides TOAST-aware value reading
type TOASTReader struct {
	chunks map[uint32][]TOASTChunk // keyed by ToastRelID
}

// NewTOASTReader creates a new TOAST reader
func NewTOASTReader() *TOASTReader {
	return &TOASTReader{
		chunks: make(map[uint32][]TOASTChunk),
	}
}

// LoadTOASTTable loads chunks from a TOAST table
func (r *TOASTReader) LoadTOASTTable(toastRelID uint32, data []byte) {
	r.chunks[toastRelID] = ReadTOASTTable(data)
}

// ReadValue reads a value, resolving TOAST pointers if needed
func (r *TOASTReader) ReadValue(data []byte) []byte {
	ptr := ParseTOASTPointer(data)
	if ptr == nil {
		// Not a TOAST pointer, return as-is
		return data
	}

	chunks, ok := r.chunks[ptr.ToastRelID]
	if !ok {
		return nil // TOAST table not loaded
	}

	return ReassembleTOAST(chunks, ptr.ValueID, ptr.IsCompressed, ptr.RawSize)
}
