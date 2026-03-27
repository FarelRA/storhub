package chunking

import (
	"fmt"
	"io"
	"os"
)

const (
	MaxReleaseAssetSize           int64 = (2 * 1024 * 1024 * 1024) - 1
	DefaultChunkSize              int64 = 256 * 1024 * 1024
	DefaultBufferSize                   = 1 * 1024 * 1024
	DefaultMaxConcurrentTransfers       = 8
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
	chunks := int((info.Size() + chunkSize - 1) / chunkSize)
	if chunks == 0 {
		chunks = 1
	}
	return &StreamingChunker{
		file:      file,
		fileSize:  info.Size(),
		baseName:  baseName,
		chunkSize: chunkSize,
		numChunks: chunks,
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
		chunkName: fmt.Sprintf("%s.part%03d", s.baseName, index+1),
		index:     index,
	}, nil
}

func (s *StreamingChunker) NumChunks() int {
	return s.numChunks
}

func (s *StreamingChunker) Close() error {
	return s.file.Close()
}
