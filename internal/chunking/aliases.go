package chunking

import (
	"sort"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

type (
	ChunkInfo    = meta.ChunkInfo
	FileMetadata = meta.FileMeta
)

func stableSortChunks(chunks []ChunkInfo) {
	sort.SliceStable(chunks, func(i, j int) bool {
		return chunks[i].Offset < chunks[j].Offset
	})
}
