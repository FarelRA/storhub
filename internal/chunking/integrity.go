package chunking

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"strconv"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func CalculateCRC32CStreaming(filePath string, bufferSize int) (string, error) {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()
	return CalculateCRC32CReader(file, bufferSize)
}

func CalculateCRC32CReader(reader io.ReadSeeker, bufferSize int) (string, error) {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek reader start: %w", err)
	}
	h := crc32.New(crc32cTable)
	buf := make([]byte, bufferSize)
	if _, err := io.CopyBuffer(h, reader, buf); err != nil {
		return "", fmt.Errorf("calculate crc32c: %w", err)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("rewind reader: %w", err)
	}
	return formatCRC32C(h.Sum32()), nil
}

func VerifyFileIntegrity(filePath string, metadata FileMetadata, bufferSize int) error {
	crc32cSum, err := CalculateChunkedIntegrity(filePath, metadata, bufferSize)
	if err != nil {
		return err
	}
	if metadata.CRC32C != crc32cSum {
		return fmt.Errorf("crc32c mismatch: expected %s, got %s", metadata.CRC32C, crc32cSum)
	}
	return nil
}

func CalculateChunkedIntegrity(filePath string, metadata FileMetadata, bufferSize int) (string, error) {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()
	return CalculateChunkedIntegrityReader(file, metadata, bufferSize)
}

func CalculateChunkedIntegrityReader(readerAt io.ReaderAt, metadata FileMetadata, bufferSize int) (string, error) {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	chunks := append([]ChunkInfo(nil), metadata.Chunks...)
	stableSortChunks(chunks)
	if len(chunks) == 0 {
		if metadata.Size == 0 {
			return formatCRC32C(0), nil
		}
		return "", fmt.Errorf("file %s has no chunks", metadata.Name)
	}
	crc, err := verifyAndCombineChunkCRC32Cs(readerAt, chunks, bufferSize)
	if err != nil {
		return "", err
	}
	return formatCRC32C(crc), nil
}

func CombineChunkCRC32Cs(chunks []ChunkInfo) (string, error) {
	ordered := append([]ChunkInfo(nil), chunks...)
	stableSortChunks(ordered)
	if len(ordered) == 0 {
		return formatCRC32C(0), nil
	}
	crc, err := combineChunkCRC32Cs(ordered)
	if err != nil {
		return "", err
	}
	return formatCRC32C(crc), nil
}

func verifyAndCombineChunkCRC32Cs(readerAt io.ReaderAt, chunks []ChunkInfo, bufferSize int) (uint32, error) {
	combined := uint32(0)
	for i, chunk := range chunks {
		actual, err := checksumReaderAtRange(readerAt, chunk.Offset, chunk.Size, bufferSize)
		if err != nil {
			return 0, fmt.Errorf("hash chunk %d: %w", chunk.Index, err)
		}
		if chunk.CRC32C != "" && chunk.CRC32C != formatCRC32C(actual) {
			return 0, fmt.Errorf("chunk %d crc32c mismatch: expected %s, got %s", chunk.Index, chunk.CRC32C, formatCRC32C(actual))
		}
		if i == 0 {
			combined = actual
			continue
		}
		combined = crc32cCombine(combined, actual, chunk.Size)
	}
	return combined, nil
}

func combineChunkCRC32Cs(chunks []ChunkInfo) (uint32, error) {
	if len(chunks) == 0 {
		return 0, errorsNew("no chunks to combine")
	}
	combined, err := parseCRC32C(chunks[0].CRC32C)
	if err != nil {
		return 0, fmt.Errorf("decode chunk %d crc32c: %w", chunks[0].Index, err)
	}
	for _, chunk := range chunks[1:] {
		part, err := parseCRC32C(chunk.CRC32C)
		if err != nil {
			return 0, fmt.Errorf("decode chunk %d crc32c: %w", chunk.Index, err)
		}
		combined = crc32cCombine(combined, part, chunk.Size)
	}
	return combined, nil
}

func checksumReaderAtRange(readerAt io.ReaderAt, offset, size int64, bufferSize int) (uint32, error) {
	if size == 0 {
		return 0, nil
	}
	h := crc32.New(crc32cTable)
	section := io.NewSectionReader(readerAt, offset, size)
	buf := make([]byte, bufferSize)
	if _, err := io.CopyBuffer(h, section, buf); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

type hashingReadSeeker struct {
	base     io.ReadSeeker
	hash     hash.Hash32
	checksum string
}

func newHashingReadSeeker(base io.ReadSeeker) *hashingReadSeeker {
	return &hashingReadSeeker{base: base, hash: crc32.New(crc32cTable)}
}

func (h *hashingReadSeeker) Read(p []byte) (int, error) {
	n, err := h.base.Read(p)
	if n > 0 {
		_, _ = h.hash.Write(p[:n])
	}
	if err == io.EOF {
		h.checksum = formatCRC32C(h.hash.Sum32())
	}
	return n, err
}

func (h *hashingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	pos, err := h.base.Seek(offset, whence)
	if err != nil {
		return pos, err
	}
	if pos == 0 {
		h.hash = crc32.New(crc32cTable)
		h.checksum = ""
	}
	return pos, nil
}

func (h *hashingReadSeeker) Checksum() string {
	if h.checksum == "" {
		return formatCRC32C(h.hash.Sum32())
	}
	return h.checksum
}

func formatCRC32C(value uint32) string {
	return fmt.Sprintf("%08x", value)
}

func parseCRC32C(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(parsed), nil
}

func crc32cCombine(crc1, crc2 uint32, len2 int64) uint32 {
	if len2 <= 0 {
		return crc1
	}
	var even [32]uint32
	var odd [32]uint32
	odd[0] = 0x82f63b78
	row := uint32(1)
	for n := 1; n < 32; n++ {
		odd[n] = row
		row <<= 1
	}
	gf2MatrixSquare(&even, &odd)
	gf2MatrixSquare(&odd, &even)
	for {
		gf2MatrixSquare(&even, &odd)
		if len2&1 != 0 {
			crc1 = gf2MatrixTimes(&even, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
		gf2MatrixSquare(&odd, &even)
		if len2&1 != 0 {
			crc1 = gf2MatrixTimes(&odd, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
	}
	return crc1 ^ crc2
}

func gf2MatrixTimes(mat *[32]uint32, vec uint32) uint32 {
	sum := uint32(0)
	i := 0
	for vec != 0 {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
		vec >>= 1
		i++
	}
	return sum
}

func gf2MatrixSquare(square, mat *[32]uint32) {
	for n := 0; n < 32; n++ {
		square[n] = gf2MatrixTimes(mat, mat[n])
	}
}

func errorsNew(text string) error { return fmt.Errorf(text) }
