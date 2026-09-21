package metadata

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/FarelRA/storhub/internal/logging"
)

// repoMetadataJSON pins the blob wire format explicitly. The stored maps are
// unexported (every mutation must flow through the tracked mutators, which
// maintain the derived index and the size cache), and encoding/json ignores
// unexported fields, so the document shape is declared here instead of being
// inferred by reflection. Field order and tags match the historical document
// exactly, so output stays byte-identical.
type repoMetadataJSON struct {
	Version     int                   `json:"v"`
	Project     string                `json:"p"`
	TotalFiles  int                   `json:"tf"`
	TotalSize   int64                 `json:"ts"`
	LastMod     int64                 `json:"lm"`
	Root        DirMeta               `json:"rt"`
	Dirs        map[string]DirMeta    `json:"d"`
	Files       map[string]FileMeta   `json:"f"`
	Chunks      map[int64]ChunkInfo   `json:"c"`
	Releases    map[string]ReleaseRef `json:"r"`
	NextInode   uint64                `json:"ni,omitempty"`
	NextChunkID int64                 `json:"nc,omitempty"`
}

func (m *RepoMetadata) toShadow() repoMetadataJSON {
	return repoMetadataJSON{
		Version:     m.Version,
		Project:     m.Project,
		TotalFiles:  m.TotalFiles,
		TotalSize:   m.TotalSize,
		LastMod:     m.LastMod,
		Root:        m.Root,
		Dirs:        m.dirs,
		Files:       m.files,
		Chunks:      m.chunks,
		Releases:    m.releases,
		NextInode:   m.NextInode,
		NextChunkID: m.NextChunkID,
	}
}

// MarshalJSON serializes the blob document. Pointer receiver only:
// RepoMetadata embeds noCopy, so a value receiver (an implicit struct copy)
// is a `go vet` copylocks error. Marshal through a *RepoMetadata always;
// marshaling a bare value falls back to reflection and drops the unexported
// maps, which UnmarshalJSON's strict version gate then rejects on the way
// back in instead of silently yielding an empty tree.
func (m *RepoMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.toShadow())
}

// ToJSON serializes the tree to its legacy single-blob document form.
func (m *RepoMetadata) ToJSON() ([]byte, error) {
	// A blob document is always maxBlobVersion: version 6 is the split
	// (manifest + objects) layout, which ToJSON cannot express. The in-memory
	// Version records the tree's target layout; the write path re-stamps it
	// via MarkSplit when publishing the split. Marshaled through the shadow
	// directly (RepoMetadata embeds noCopy, so no struct copy here).
	sh := m.toShadow()
	if sh.Version > maxBlobVersion {
		sh.Version = maxBlobVersion
	}
	data, err := json.Marshal(&sh)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return data, nil
}

// FromJSON parses a metadata document into current form. Version detection
// and any upgrades belong entirely to Migrate (stacked, eager); this parser
// understands ONLY the current blob schema - legacy spellings never reach it.
// A version-6 split-index manifest is NOT a blob and must go through
// ParseManifest. The resulting tree carries the document version it was read
// as (maxBlobVersion: blobs stop at 5; 6 is manifest-only).
func (m *RepoMetadata) FromJSON(data []byte) error {
	upgraded, version, err := Migrate(data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(upgraded, m); err != nil {
		logging.Error(metaLog(), "metadata load failed", "err", err)
		return fmt.Errorf("unmarshal metadata: %w", err)
	}
	m.Version = version
	return nil
}

// UnmarshalJSON enforces the current-schema contract at the type level: only
// documents written in the current blob schema (version maxBlobVersion)
// decode. Older payloads must go through FromJSON/Migrate, and a split-index
// manifest must go through ParseManifest/LoadTree - a direct unmarshal fails
// loudly instead of silently yielding an empty tree from ignored unknown
// fields. Version 6 is manifest-only: a v6 document without a non-empty tree
// root is a truncated manifest, not a blob, and decoding it as one would
// hand the next commit an empty tree over the real index.
func (m *RepoMetadata) UnmarshalJSON(data []byte) error {
	probe, err := probeVersion(data)
	if err != nil {
		return fmt.Errorf("metadata probe: %w", err)
	}
	if probe.V == nil {
		return errors.New("metadata document lacks a schema version; use metadata.Migrate for older formats")
	}
	if probe.TreeRoot != "" {
		return fmt.Errorf("document is a v%d split-index manifest; load it via ParseManifest/LoadTree, not RepoMetadata", maxMetadataVersion)
	}
	if *probe.V == maxMetadataVersion {
		return fmt.Errorf("document claims v%d with no tree root: v%d documents are manifests (load via ParseManifest/LoadTree); blobs are v%d or older", maxMetadataVersion, maxMetadataVersion, maxBlobVersion)
	}
	if *probe.V != maxBlobVersion {
		return fmt.Errorf("metadata version %d is not a current blob version (%d); migrate first", *probe.V, maxBlobVersion)
	}
	var raw repoMetadataJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*m = RepoMetadata{
		Version:     *probe.V,
		Project:     raw.Project,
		TotalFiles:  raw.TotalFiles,
		TotalSize:   raw.TotalSize,
		LastMod:     raw.LastMod,
		Root:        raw.Root,
		dirs:        raw.Dirs,
		files:       raw.Files,
		chunks:      raw.Chunks,
		releases:    raw.Releases,
		NextInode:   raw.NextInode,
		NextChunkID: raw.NextChunkID,
	}
	return nil
}
