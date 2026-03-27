package storage

import (
	"fmt"
	"hash/crc32"
	"io"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type hashingReadSeeker struct {
	base     io.ReadSeeker
	hash     hash32
	checksum string
}

type hash32 interface {
	io.Writer
	Sum32() uint32
	Reset()
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
		h.hash.Reset()
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

func parseNumericReleaseTag(tag string) (int, bool) {
	return metadata.ParseNumericReleaseTag(tag)
}

func calculateChunkCRC32Cs(chunks []ChunkInfo) (string, error) {
	return chunking.CombineChunkCRC32Cs(chunks)
}
