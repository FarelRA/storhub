package metadata

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
)

// LoadTree reconstructs a flat RepoMetadata from a manifest, fetching each
// referenced object through getObject (which the caller backs with the
// content-addressed cache + repo). Every object is verified against its
// content address here, so a bit-rotted cache entry cannot poison the tree
// even if the fetch layer returns it unchecked. The returned tree is not yet
// normalized; callers Normalize/RecomputeStats as they do after any load.
func LoadTree(manifest *Manifest, getObject func(sha string) ([]byte, error)) (*RepoMetadata, error) {
	started := time.Now()
	buckets := 0
	if manifest != nil {
		buckets = len(manifest.ChunkBuckets)
	}
	logging.Debug(metaLog(), "metadata load start", "buckets", buckets)
	meta, err := loadTree(manifest, getObject)
	if err != nil {
		logging.Error(metaLog(), "metadata load failed", "err", err, "elapsed", time.Since(started))
		return nil, err
	}
	logging.Debug(metaLog(), "metadata load complete", "files", len(meta.files), "dirs", len(meta.dirs), "chunks", len(meta.chunks), "releases", len(meta.releases), "elapsed", time.Since(started))
	return meta, nil
}

// loadTree is the load body behind the logging wrapper above.
func loadTree(manifest *Manifest, getObject func(sha string) ([]byte, error)) (*RepoMetadata, error) {
	if manifest == nil {
		return nil, fmt.Errorf("nil manifest")
	}
	meta := newBareRepoMetadata()
	meta.Version = maxMetadataVersion
	meta.Project = manifest.Project
	meta.NextInode = manifest.NextInode
	meta.NextChunkID = manifest.NextChunkID
	meta.LastMod = manifest.LastMod
	if err := loadNode(meta, "", manifest.TreeRoot, getObject, map[string]bool{}); err != nil {
		return nil, err
	}
	for _, sha := range manifest.ChunkBuckets {
		data, err := getObject(sha)
		if err != nil {
			return nil, fmt.Errorf("load chunk bucket %s: %w", shortObj(sha), err)
		}
		if err := verifyObject(sha, data, "chunk bucket"); err != nil {
			return nil, err
		}
		var b ChunkBucket
		if err := json.Unmarshal(data, &b); err != nil {
			return nil, fmt.Errorf("decode chunk bucket %s: %w", shortObj(sha), err)
		}
		for id, info := range b.Chunks {
			// Buckets are content-addressed by id range: a mis-bucketed
			// or duplicated id is corruption, not data. (Negative ids
			// can quotient-match bucket 0 under truncating division, so
			// they are rejected outright.)
			if id < 0 {
				return nil, fmt.Errorf("chunk bucket %s: negative chunk id %d", shortObj(sha), id)
			}
			if id/ChunkBucketSize != b.Index {
				return nil, fmt.Errorf("chunk bucket %s: chunk id %d does not belong to bucket index %d", shortObj(sha), id, b.Index)
			}
			if _, dup := meta.chunks[id]; dup {
				return nil, fmt.Errorf("chunk bucket %s: duplicate chunk id %d", shortObj(sha), id)
			}
			meta.chunks[id] = info
		}
	}
	if manifest.Releases != "" {
		data, err := getObject(manifest.Releases)
		if err != nil {
			return nil, fmt.Errorf("load releases %s: %w", shortObj(manifest.Releases), err)
		}
		if err := verifyObject(manifest.Releases, data, "releases"); err != nil {
			return nil, err
		}
		var rel ReleasesObject
		if err := json.Unmarshal(data, &rel); err != nil {
			return nil, fmt.Errorf("decode releases: %w", err)
		}
		for tag, ref := range rel.Releases {
			meta.releases[tag] = ref
		}
	}
	if manifest.Version < maxMetadataVersion {
		// Seconds-era manifest: its objects carry seconds timestamps.
		logging.Warn(metaLog(), "metadata timestamp fallback", "reason", "seconds-era manifest, migrating timestamps to nanoseconds", "from", manifest.Version, "to", maxMetadataVersion)
		migrateTreeTimesToNano(meta)
	}
	meta.RecomputeStats()
	// Reconcile the allocation counters against the content actually loaded:
	// a stale or regressed manifest must not yield a tree whose counters sit
	// behind live ids (callers of LoadTree may never Normalize).
	meta.reconcileCounters()
	return meta, nil
}

// loadNode recursively loads a directory node and its subtree. chain is
// the sha chain from the root to this node's parent: a sha repeating on
// its OWN ancestor chain is a corrupt cycle, but the same object shared
// by two paths (BuildTree dedups structurally identical subtrees to one
// object) is legitimate sharing and is applied once per path.
func loadNode(meta *RepoMetadata, dirPath, sha string, getObject func(string) ([]byte, error), chain map[string]bool) error {
	if sha == "" {
		return fmt.Errorf("empty node sha at %q", dirPath)
	}
	if chain[sha] {
		return fmt.Errorf("cycle in tree objects at %q (sha %s)", dirPath, shortObj(sha))
	}
	chain[sha] = true
	defer delete(chain, sha)
	data, err := getObject(sha)
	if err != nil {
		return fmt.Errorf("load tree node %q: %w", dirPath, err)
	}
	if err := verifyObject(sha, data, fmt.Sprintf("tree node %q", dirPath)); err != nil {
		return err
	}
	var node TreeNode
	if err := json.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("decode tree node %q: %w", dirPath, err)
	}
	if dirPath == "" {
		meta.Root = node.Meta
	} else {
		meta.dirs[dirPath] = node.Meta
	}
	for name, f := range node.Files {
		meta.files[joinStored(dirPath, name)] = f
	}
	for name, child := range node.Subdirs {
		if err := loadNode(meta, joinStored(dirPath, name), child, getObject, chain); err != nil {
			return err
		}
	}
	return nil
}

func joinStored(dirPath, name string) string {
	if dirPath == "" {
		return name
	}
	return dirPath + "/" + name
}

// verifyObject enforces the content address: bytes whose sha256 does not
// match the sha they were fetched by are corruption, not data.
func verifyObject(sha string, data []byte, what string) error {
	if got := ObjectSHA(data); got != sha {
		return fmt.Errorf("%s %s failed content-address check (sha256 %s)", what, shortObj(sha), shortObj(got))
	}
	return nil
}

func nodeDepth(p string) int {
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}

func shortObj(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// loadTreeFetchParallelism bounds concurrent object fetches during a
// parallel tree load. Loads are network-bound, so the bound sits above
// NumCPU while staying a polite upstream citizen per load (the
// per-minute rate governor still applies on top).
const loadTreeFetchParallelism = 8

// LoadTreeParallel reconstructs the same tree as LoadTree, fetching
// independent objects concurrently: sibling subtrees, chunk buckets, and
// the release catalog share no data dependencies (content-addressed and
// immutable), so a cold load pays one round per tree LEVEL instead of one
// per object. Verification, cycle detection, and error wordings match
// LoadTree; fetched bytes are memoized, so a deduped object shared by two
// paths still travels the wire once.
func LoadTreeParallel(manifest *Manifest, getObject func(sha string) ([]byte, error)) (*RepoMetadata, error) {
	started := time.Now()
	buckets := 0
	if manifest != nil {
		buckets = len(manifest.ChunkBuckets)
	}
	logging.Debug(metaLog(), "metadata parallel load start", "buckets", buckets)
	meta, err := loadTreeParallel(manifest, getObject)
	if err != nil {
		logging.Error(metaLog(), "metadata parallel load failed", "err", err, "elapsed", time.Since(started))
		return nil, err
	}
	logging.Debug(metaLog(), "metadata parallel load complete", "files", len(meta.files), "dirs", len(meta.dirs), "chunks", len(meta.chunks), "releases", len(meta.releases), "elapsed", time.Since(started))
	return meta, nil
}

// loadTreeParallel is the parallel load body behind the logging wrapper
// above.
func loadTreeParallel(manifest *Manifest, getObject func(sha string) ([]byte, error)) (*RepoMetadata, error) {
	if manifest == nil {
		return nil, fmt.Errorf("nil manifest")
	}
	if manifest.TreeRoot == "" {
		return nil, fmt.Errorf("empty node sha at %q", "")
	}
	l := &parallelTreeLoader{
		meta:    newBareRepoMetadata(),
		fetched: make(map[string][]byte),
		sem:     make(chan struct{}, loadTreeFetchParallelism),
		fetch:   getObject,
	}
	l.meta.Version = maxMetadataVersion
	l.meta.Project = manifest.Project
	l.meta.NextInode = manifest.NextInode
	l.meta.NextChunkID = manifest.NextChunkID
	l.meta.LastMod = manifest.LastMod
	// The root gates everything (its child list is discovered from its
	// bytes), so it loads inline; every independent fetch fans out below.
	rootData, err := l.get(manifest.TreeRoot)
	if err != nil {
		return nil, fmt.Errorf("load tree node %q: %w", "", err)
	}
	if err := verifyObject(manifest.TreeRoot, rootData, fmt.Sprintf("tree node %q", "")); err != nil {
		return nil, err
	}
	var root TreeNode
	if err := json.Unmarshal(rootData, &root); err != nil {
		return nil, fmt.Errorf("decode tree node %q: %w", "", err)
	}
	l.meta.Root = root.Meta
	for name, f := range root.Files {
		l.meta.files[name] = f
	}
	chain := []string{manifest.TreeRoot}
	for name, child := range root.Subdirs {
		l.spawnSubtree(name, child, chain)
	}
	for _, sha := range manifest.ChunkBuckets {
		l.spawnBucket(sha)
	}
	if manifest.Releases != "" {
		l.spawnReleases(manifest.Releases)
	}
	l.wg.Wait()
	if l.err != nil {
		return nil, l.err
	}
	if manifest.Version < maxMetadataVersion {
		// Seconds-era manifest: its objects carry seconds timestamps.
		logging.Warn(metaLog(), "metadata timestamp fallback", "reason", "seconds-era manifest, migrating timestamps to nanoseconds", "from", manifest.Version, "to", maxMetadataVersion)
		migrateTreeTimesToNano(l.meta)
	}
	l.meta.RecomputeStats()
	// Same tail as LoadTree: counters reconcile against loaded content.
	l.meta.reconcileCounters()
	return l.meta, nil
}

// parallelTreeLoader is the shared state of one LoadTreeParallel call.
// Tree inserts and the fetch memo serialize on mu; fetch interrogation
// itself runs off-lock so concurrent GETs overlap. stop short-circuits
// new work after the first failure; in-flight fetches still land (bounded
// waste on corrupt input, never a hang).
type parallelTreeLoader struct {
	meta    *RepoMetadata
	mu      sync.Mutex
	fetched map[string][]byte
	sem     chan struct{}
	wg      sync.WaitGroup
	stop    atomic.Bool
	errOnce sync.Once
	err     error
	fetch   func(string) ([]byte, error)
}

func (l *parallelTreeLoader) fail(err error) {
	l.errOnce.Do(func() {
		l.err = err
		l.stop.Store(true)
	})
}

// spawn runs fn with bounded parallelism: a free slot goes to a new
// goroutine, a saturated loader runs fn inline in the caller. Blocking
// for a slot while holding one would deadlock wide+deep trees (every
// slot held by a spawner waiting for a slot), and queueing unbounded
// goroutines behind a saturated link trades a network wait for a memory
// pile-up. Saturation already means the link is fully used, so inline
// fallback loses no throughput.
func (l *parallelTreeLoader) spawn(fn func()) {
	if l.stop.Load() {
		return
	}
	l.wg.Add(1)
	select {
	case l.sem <- struct{}{}:
		go func() {
			defer func() { <-l.sem; l.wg.Done() }()
			fn()
		}()
	default:
		defer l.wg.Done()
		fn()
	}
}

// get fetches one object, memoized: a deduped subtree shared by two paths
// is fetched once and applied per path. Concurrent first-flights may
// duplicate one fetch (idempotent GET, identical bytes); correctness never
// depends on uniqueness.
func (l *parallelTreeLoader) get(sha string) ([]byte, error) {
	l.mu.Lock()
	if data, ok := l.fetched[sha]; ok {
		l.mu.Unlock()
		return data, nil
	}
	l.mu.Unlock()
	data, err := l.fetch(sha)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.fetched[sha] = data
	l.mu.Unlock()
	return data, nil
}

func (l *parallelTreeLoader) spawnSubtree(name, sha string, chain []string) {
	// The child chain copies: siblings extend the shared parent chain
	// concurrently, and append may reuse the backing array.
	childChain := make([]string, len(chain)+1)
	copy(childChain, chain)
	childChain[len(chain)] = sha
	dirPath := name
	l.spawn(func() { l.loadSubtree(dirPath, sha, childChain) })
}

func (l *parallelTreeLoader) loadSubtree(dirPath, sha string, chain []string) {
	if l.stop.Load() {
		return
	}
	// chain already ends in sha (extended by the spawner); membership is
	// tested against the ancestors only.
	if slices.Contains(chain[:len(chain)-1], sha) {
		l.fail(fmt.Errorf("cycle in tree objects at %q (sha %s)", dirPath, shortObj(sha)))
		return
	}
	data, err := l.get(sha)
	if err != nil {
		l.fail(fmt.Errorf("load tree node %q: %w", dirPath, err))
		return
	}
	if err := verifyObject(sha, data, fmt.Sprintf("tree node %q", dirPath)); err != nil {
		l.fail(err)
		return
	}
	var node TreeNode
	if err := json.Unmarshal(data, &node); err != nil {
		l.fail(fmt.Errorf("decode tree node %q: %w", dirPath, err))
		return
	}
	l.mu.Lock()
	l.meta.dirs[dirPath] = node.Meta
	for name, f := range node.Files {
		l.meta.files[joinStored(dirPath, name)] = f
	}
	l.mu.Unlock()
	for name, child := range node.Subdirs {
		l.spawnSubtree(joinStored(dirPath, name), child, chain)
	}
}

func (l *parallelTreeLoader) spawnBucket(sha string) {
	l.spawn(func() { l.loadBucket(sha) })
}

func (l *parallelTreeLoader) loadBucket(sha string) {
	if l.stop.Load() {
		return
	}
	data, err := l.get(sha)
	if err != nil {
		l.fail(fmt.Errorf("load chunk bucket %s: %w", shortObj(sha), err))
		return
	}
	if err := verifyObject(sha, data, "chunk bucket"); err != nil {
		l.fail(err)
		return
	}
	var b ChunkBucket
	if err := json.Unmarshal(data, &b); err != nil {
		l.fail(fmt.Errorf("decode chunk bucket %s: %w", shortObj(sha), err))
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, info := range b.Chunks {
		if id < 0 {
			l.fail(fmt.Errorf("chunk bucket %s: negative chunk id %d", shortObj(sha), id))
			return
		}
		if id/ChunkBucketSize != b.Index {
			l.fail(fmt.Errorf("chunk bucket %s: chunk id %d does not belong to bucket index %d", shortObj(sha), id, b.Index))
			return
		}
		if _, dup := l.meta.chunks[id]; dup {
			l.fail(fmt.Errorf("chunk bucket %s: duplicate chunk id %d", shortObj(sha), id))
			return
		}
		l.meta.chunks[id] = info
	}
}

func (l *parallelTreeLoader) spawnReleases(sha string) {
	l.spawn(func() { l.loadReleases(sha) })
}

func (l *parallelTreeLoader) loadReleases(sha string) {
	if l.stop.Load() {
		return
	}
	data, err := l.get(sha)
	if err != nil {
		l.fail(fmt.Errorf("load releases %s: %w", shortObj(sha), err))
		return
	}
	if err := verifyObject(sha, data, "releases"); err != nil {
		l.fail(err)
		return
	}
	var rel ReleasesObject
	if err := json.Unmarshal(data, &rel); err != nil {
		l.fail(fmt.Errorf("decode releases: %w", err))
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for tag, ref := range rel.Releases {
		l.meta.releases[tag] = ref
	}
}
