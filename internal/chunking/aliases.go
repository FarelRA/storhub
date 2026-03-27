package chunking

import (
	"sort"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

type (
	ChunkInfo    = meta.ChunkInfo
	FileMetadata = meta.FileMetadata
)

func stableSortChunks(chunks []ChunkInfo) {
	sort.SliceStable(chunks, func(i, j int) bool {
		if chunks[i].Offset != chunks[j].Offset {
			return chunks[i].Offset < chunks[j].Offset
		}
		return chunks[i].Index < chunks[j].Index
	})
}
