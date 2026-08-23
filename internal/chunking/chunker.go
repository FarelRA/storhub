// Package chunking splits files into immutable, asset-sized chunks for
// upload to GitHub releases and reassembles them on download.
//
// A StreamingChunker walks a local file sequentially and hands out
// ChunkReaders, each covering one chunk-sized window of the file. Chunk
// sizes are clamped to the release-asset ceiling; a zero size means "use
// the default". Empty files yield a single zero-length chunk so every
// uploaded file has at least one observable part.
package chunking

import (
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
)

const (
	MaxReleaseAssetSize int64 = (2 * 1024 * 1024 * 1024) - 1
	DefaultChunkSize    int64 = MaxReleaseAssetSize
	DefaultBufferSize         = 1 * 1024 * 1024
)

type ChunkReader struct {
	reader    *io.SectionReader
	offset    int64
	size      int64
	chunkName string
	index     int
}

func (c *ChunkReader) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *ChunkReader) Seek(offset int64, whence int) (int64, error) {
	return c.reader.Seek(offset, whence)
}

func (c *ChunkReader) Size() int64 {
	return c.size
}

func (c *ChunkReader) Offset() int64 {
	return c.offset
}

func (c *ChunkReader) Name() string {
	return c.chunkName
}

func (c *ChunkReader) Index() int {
	return c.index
}

type StreamingChunker struct {
	file      *os.File
	fileSize  int64
	baseName  string
	chunkSize int64
	numChunks int
	nameWidth int
}

// NormalizedSize clamps a configured chunk size into the legal range:
// non-positive values fall back to DefaultChunkSize and values above the
// GitHub release-asset ceiling clamp to MaxReleaseAssetSize. Every consumer
// (config defaults, storage patch planning, fusefs overlay planning) funnels
// through this single definition so the ceiling cannot drift.
func NormalizedSize(chunkSize int64) int64 {
	if chunkSize <= 0 {
		return DefaultChunkSize
	}
	if chunkSize > MaxReleaseAssetSize {
		return MaxReleaseAssetSize
	}
	return chunkSize
}

func NewStreamingChunker(filePath, baseName string, chunkSize int64) (*StreamingChunker, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > MaxReleaseAssetSize {
		chunkSize = MaxReleaseAssetSize
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}
	// Count in 64-bit; a naive int() cast would wrap around on 32-bit
	// platforms for large files with small chunks.
	count := (info.Size() + chunkSize - 1) / chunkSize
	if count == 0 {
		count = 1
	}
	if count > math.MaxInt32 {
		file.Close()
		return nil, fmt.Errorf("file needs %d chunks; the maximum supported count is %d", count, math.MaxInt32)
	}
	return &StreamingChunker{
		file:      file,
		fileSize:  info.Size(),
		baseName:  baseName,
		chunkSize: chunkSize,
		numChunks: int(count),
		// Zero-pad chunk names to a width that keeps lexicographic order
		// equal to numeric order even past 999 parts.
		nameWidth: len(strconv.FormatInt(count, 10)),
	}, nil
}

func (s *StreamingChunker) GetChunk(index int) (*ChunkReader, error) {
	if index < 0 || index >= s.numChunks {
		return nil, fmt.Errorf("chunk index out of range: %d", index)
	}
	offset := int64(index) * s.chunkSize
	size := s.chunkSize
	if offset+size > s.fileSize {
		size = s.fileSize - offset
	}
	return &ChunkReader{
		reader:    io.NewSectionReader(s.file, offset, size),
		offset:    offset,
		size:      size,
		chunkName: fmt.Sprintf("%s.part%0*d", s.baseName, max(s.nameWidth, 3), index+1),
		index:     index,
	}, nil
}

func (s *StreamingChunker) NumChunks() int {
	return s.numChunks
}

func (s *StreamingChunker) Close() error {
	return s.file.Close()
}
