package storage

import (
	"context"
	"fmt"
	"log/slog"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	"github.com/FarelRA/storhub/internal/logging"
	"io"
)

func (h *StorHub) uploadChunks(ctx context.Context, project, releaseTag, uploadURL string, planner *chunking.StreamingChunker, prepare func(remaining int) (string, string, error)) ([]ChunkInfo, error) {
	sink := h.newChunkSink(ctx, project, releaseTag, uploadURL, planner.NumChunks(), prepare)
	err := sink.pumpParts(planner.NumChunks(), func(i int) (io.ReadSeeker, int64, int64, func(), error) {
		chunk, err := planner.GetChunk(i)
		if err != nil {
			return nil, 0, 0, nil, err
		}
		return chunk, chunk.Size(), chunk.Offset(), func() {}, nil
	})
	return sink.results, err
}

// chunkSink uploads chunk payloads one at a time, accumulating ChunkInfos
// and rotating to a fresh release whenever the server reports the current
// one full. It is the single home of name-collision retries and
// release-full rotation; every upload source funnels through put so a stale
// release choice can never strand an upload.
//
// Rotation terminates: each rotation invalidates the release cache and
// re-resolves against a fresh server list (with true counts near the
// ceiling), so a repeat pick means a concurrent writer filled it in the
// millisecond race window, and the next re-list observes that fill.
type chunkSink struct {
	hub     *StorHub
	ctx     context.Context
	project string
	namer   *assetNamer
	// total is the planned part count, for remaining-slot computation.
	// Sources are planner counts (bounded by the chunker at construction)
	// or checked conversions of ceil(size/chunkSize), so int is safe; the
	// release picker below takes int and widening would fork that contract.
	total      int
	results    []ChunkInfo
	releaseTag string
	uploadURL  string
	// prepare resolves a fresh release with room for the given remaining
	// chunk count. Invoked at most once per full release encountered.
	prepare func(remaining int) (tag, url string, err error)
}

func (h *StorHub) newChunkSink(ctx context.Context, project, releaseTag, uploadURL string, total int, prepare func(int) (string, string, error)) *chunkSink {
	return &chunkSink{
		hub: h, ctx: ctx, project: project,
		namer:      newAssetNamer(),
		total:      total,
		results:    make([]ChunkInfo, 0, total),
		releaseTag: releaseTag, uploadURL: uploadURL,
		prepare: prepare,
	}
}

// pumpParts is the single upload loop: it opens each of count parts in
// order and feeds it to put, which stays the only upload sink. Planned
// file parts and spooled request-body parts differ only in their open
// function, so both upload paths share this loop instead of hand-rolling
// their own. A failed open aborts with nothing to release; a failed put
// releases that part before aborting. Partial results stay in s.results
// for the caller to compensate on error.
func (s *chunkSink) pumpParts(count int, open func(i int) (reader io.ReadSeeker, size, offset int64, cleanup func(), err error)) error {
	for i := 0; i < count; i++ {
		reader, size, offset, cleanup, err := open(i)
		if err != nil {
			return err
		}
		if err := s.put(reader, size, offset); err != nil {
			cleanup()
			return err
		}
		cleanup()
	}
	return nil
}

// put uploads one chunk payload. The transport rewinds the reader per
// attempt. The returned ChunkInfo carries the release that actually holds
// the bytes, which may differ from the sink's initial target after a
// rotation. Partial results stay in s.results for the caller to compensate
// on error; put itself never deletes.
//
// Rotation is capped at maxReleaseRotations (central tunables, client.go):
// each rotation re-resolves against a fresh server list, so a repeat pick
// means a concurrent writer filled it in the race window: but a
// persistently-full set (many concurrent writers, no headroom) previously
// re-listed + re-uploaded forever. Exceeding the cap fails loudly instead.
func (s *chunkSink) put(reader io.ReadSeeker, size, offset int64) error {
	nameRetries := 0
	rotations := 0
	for {
		assetName, err := s.namer.Next()
		if err != nil {
			return err
		}
		assetID, err := s.hub.uploadAssetStreaming(s.ctx, s.project, s.releaseTag, s.uploadURL, assetName, reader, size)
		if err == nil {
			s.results = append(s.results, ChunkInfo{Size: size, Offset: offset, Release: s.releaseTag, AssetID: assetID, AssetOffset: 0})
			return nil
		}
		if isReleaseFull(err) {
			rotations++
			if rotations > maxReleaseRotations {
				return fmt.Errorf("upload chunk (offset %d): release %s full after %d rotations; concurrent writers hold every release at the %d-asset ceiling, retry the upload", offset, s.releaseTag, maxReleaseRotations, releaseAssetCap)
			}
			// Guard the logger being emitted on: projectLogger already
			// binds project, so no repeat "project" attr is passed.
			if logging.Enabled(s.hub.projectLogger(s.project), slog.LevelDebug) {
				logging.Debug(s.hub.projectLogger(s.project), "upload release full, rotating", "release", s.releaseTag, "uploaded", len(s.results), "total", s.total, "rotation", rotations)
			}
			s.hub.invalidateReleaseCache(s.project)
			tag, url, err := s.prepare(s.total - len(s.results))
			if err != nil {
				return err
			}
			s.releaseTag, s.uploadURL = tag, url
			continue
		}
		if isAlreadyExists(err) {
			if logging.Enabled(s.hub.projectLogger(s.project), slog.LevelDebug) {
				logging.Debug(s.hub.projectLogger(s.project), "upload chunk asset name collision, retry", "asset", assetName, "release", s.releaseTag)
			}
			nameRetries++
			if nameRetries >= maxNameRetries {
				return fmt.Errorf("upload chunk failed after %d name retries", maxNameRetries)
			}
			continue
		}
		return fmt.Errorf("upload chunk (offset %d): %w", offset, err)
	}
}

// uploadAssetStreaming uploads one chunk payload under assetName, then
// records it against the release asset count. It wraps the raw client
// upload with the owner gate and the count bump, so callers never use the
// raw call directly on this path.
func (h *StorHub) uploadAssetStreaming(ctx context.Context, project, releaseTag, uploadURL, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return 0, err
	}
	assetID, err := h.gh.UploadAsset(ctx, uploadURL, assetName, reader, size)
	if err != nil {
		return 0, err
	}
	h.bumpCachedReleaseAssetCount(project, releaseTag, assetID)
	return assetID, nil
}

// fetchAssetRange opens one closed [start, end] byte span of an asset for
// reading. The closed convention matches the asset client below, which
// renders it as a bytes=start-end header; callers pass end as
// offset+size-1, never as an exclusive bound.
func (h *StorHub) fetchAssetRange(ctx context.Context, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, 0, err
	}
	return h.gh.DownloadAssetStream(ctx, h.owner, project, assetID, start, end)
}

func (h *StorHub) fillAssetRange(ctx context.Context, project string, chunk ChunkInfo, dst []byte) error {
	if int64(len(dst)) != chunk.Size {
		return fmt.Errorf("asset range size mismatch: expected buffer %d, got %d", chunk.Size, len(dst))
	}
	return h.withAssetRangeReader(ctx, project, chunk, func(reader io.Reader) error {
		read, err := io.ReadFull(reader, dst)
		if err != nil {
			return err
		}
		if int64(read) != chunk.Size {
			return fmt.Errorf("asset range size mismatch: expected %d, got %d", chunk.Size, read)
		}
		return nil
	})
}

func (h *StorHub) withAssetRangeReader(ctx context.Context, project string, chunk ChunkInfo, fn func(io.Reader) error) error {
	// Single-attempt closure over the open→read→close sequence; withRetry
	// (retry.go) owns the backoff/sleep shape shared with purgeRetry and
	// downloadChunkWithRetry. Both open and read errors gate on
	// isRetryableDownloadError, preserving the old two-branch (API error plus CDN error) semantics.
	attempt := func() error {
		reader, _, err := h.fetchAssetRange(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			return err
		}
		err = fn(reader)
		if closeErr := reader.Close(); err == nil {
			err = closeErr
		}
		return err
	}
	// The old "exhausted retries" fallthrough was unreachable (the last
	// attempt returns its error directly); withRetry preserves that by
	// returning the final attempt's error.
	return h.withRetry(ctx, "asset-range", h.config.MaxRetries+1, isRetryableDownloadError, attempt)
}
