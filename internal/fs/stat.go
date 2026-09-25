package fs

import (
	"context"
	"sort"
	"time"
)

// StatPathContext returns the stat view of targetPath with lstat semantics.
func (s *Service) StatPathContext(ctx context.Context, project, targetPath string) (result *EntryInfo, err error) {
	err = s.withOp(project, "stat-path", false, []any{"path", targetPath}, func() error {
		// lstat semantics: the final symlink is NOT followed; intermediate
		// symlink components are resolved physically. Callers wanting
		// stat() semantics resolve first (StatResolveTracked) and look up.
		if targetPath != "" {
			if err := ValidateAccessPathShape(targetPath); err != nil {
				return err
			}
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := LstatResolveTracked(repo, targetPath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		if cleanPath == "" {
			result = EntryFromDirectory(&repo.Root, "", repo.DirNLink(""))
			return nil
		}
		// lstat semantics: a terminal symlink reports itself; intermediate
		// components were resolved above.
		if file := repo.FindFile(cleanPath); file != nil {
			result = EntryFromFile(file, cleanPath, repo.FileNLink(cleanPath))
			return nil
		}
		if dir := repo.GetDirectory(cleanPath); dir != nil {
			result = EntryFromDirectory(dir, cleanPath, repo.DirNLink(cleanPath))
			return nil
		}
		return NotFound(cleanPath)
	})
	return result, err
}

// StatFSContext returns aggregate filesystem counts for the project.
func (s *Service) StatFSContext(ctx context.Context, project string) (result *FSStats, err error) {
	err = s.withOp(project, "statfs", false, nil, func() error {
		repo, sha, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// Cache the O(files+chunks) aggregation per project, invalidated by
		// (commit SHA, local mutation counter) plus a short TTL backstop.
		// Rescanning the whole tree on every df burned permanently under
		// monitoring loops.
		state := s.state(project)
		now := time.Now()
		if cached, ok := state.cachedStatFS(sha, now); ok {
			result = cached
			return nil
		}
		// Aggregate live instead of trusting the repo's own cached counters:
		// those only refresh during metadata commits, so a freshly mutated
		// tree would otherwise report stale numbers.
		files := 0
		var totalBytes int64
		for _, file := range repo.Files() {
			if file.Symlink == "" {
				files++
				totalBytes += file.Size
			}
		}
		stats := &FSStats{Files: files, Directories: len(repo.Dirs()), Inodes: CountUniqueInodes(repo), Bytes: totalBytes, Releases: len(repo.Releases())}
		assetCounts := make(map[string]int, len(repo.Chunks()))
		for _, chunk := range repo.Chunks() {
			if chunk.Release != "" {
				assetCounts[chunk.Release]++
			}
		}
		for _, count := range assetCounts {
			stats.Assets += count
		}
		state.storeStatFS(sha, stats)
		result = stats
		return nil
	})
	return result, err
}

// cachedStatFS returns a fresh copy of the cached aggregate when it still
// matches the commit SHA, the local mutation counter, and the TTL.
func (p *projectState) cachedStatFS(sha string, now time.Time) (*FSStats, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statfs == nil || p.statfsSha != sha || p.statfsGen != p.mutations.Load() || now.Sub(p.statfsAt) >= statfsCacheTTL {
		return nil, false
	}
	copied := *p.statfs
	return &copied, true
}

// storeStatFS records the aggregate together with the invalidation keys
// observed at computation time. A mutation that landed during the scan
// bumps the counter afterwards, so the next read recomputes (the stored
// numbers are then merely redundant, never stale).
func (p *projectState) storeStatFS(sha string, stats *FSStats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := *stats
	p.statfs = &copied
	p.statfsSha = sha
	p.statfsGen = p.mutations.Load()
	p.statfsAt = time.Now()
}

// ReadDirContext lists the children of the directory at dirPath.
func (s *Service) ReadDirContext(ctx context.Context, project, dirPath string) (result []DirEntry, err error) {
	err = s.withOp(project, "readdir", false, []any{"path", dirPath}, func() error {
		// opendir follows a final symlink to a directory, so resolution is
		// stat-style.
		if dirPath != "" {
			if err := ValidateAccessPathShape(dirPath); err != nil {
				return err
			}
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, dirPath)
		if err != nil {
			return err
		}
		// CheckListDirAccessResolved consumes the walk's traversed chain
		// itself: no separate walk pass (and no re-resolution) here.
		if err := CheckListDirAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if cleanPath != "" && repo.FindFile(cleanPath) != nil {
			return NotDirectory(cleanPath)
		}
		if cleanPath != "" && !repo.HasDirectory(cleanPath) {
			return NotFound(cleanPath)
		}
		dirNames, fileNames := repo.DirectoryChildren(cleanPath)
		entries := make([]DirEntry, 0, len(dirNames)+len(fileNames))
		for _, name := range dirNames {
			if dir := repo.GetDirectory(name); dir != nil {
				entries = append(entries, DirEntryFromDirectory(*dir, name, repo.DirNLink(name)))
			}
		}
		for _, name := range fileNames {
			if file := repo.FindFile(name); file != nil {
				entries = append(entries, DirEntryFromFile(*file, name, repo.FileNLink(name)))
			}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		TouchDirectoryAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
		result = entries
		return nil
	})
	return result, err
}
