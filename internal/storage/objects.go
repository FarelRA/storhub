package storage

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"

	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// indexFilePath is the split manifest: the single CAS point of a
// version-5 project.
const indexFilePath = ".storhub/index.json"

// objectRepoPath maps an object sha to its repo path under .storhub.
func objectRepoPath(sha string) string {
	return ".storhub/" + meta.ObjectPath(sha)
}

// objectCache is the client-side cache of content-addressed index objects.
// Objects are immutable (name = sha256 of content), so caching is correct:
// the bytes are verified against their address ONCE, at put time (and again
// by the fetch path before put). A hit does not re-hash: re-running sha256
// over up to an 8 MiB object on every read was the dominant per-hit cost,
// and a cached object's bytes cannot change under a content address (a
// corrupted file is a disk-rot event, handled by the reaper/refetch, not
// worth an O(size) hash per hit).
//
// The cache is LRU-bounded so a long-lived mount cannot grow without limit;
// eviction only costs a later refetch, never correctness. The bound is
// two-fold: an entry count AND a byte budget. Entries alone are not a disk
// bound — an index object can be up to the contents-API size limit, so a
// count-only cap still allows max × 8 MiB of cache per project. Recency is
// maintained with a container/list (O(1) touch) instead of a linear scan of
// the order slice per hit.
//
// defaultObjectCacheMaxBytes is the per-project disk budget. Index objects
// are small JSON nodes; a gigabyte of them cached is already far past any
// realistic working set, and exceeding it must cost refetches, not disk.
const defaultObjectCacheMaxBytes = 1 << 30

// objectLRUItem is one cache entry in the recency list.
type objectLRUItem struct {
	sha  string
	size int
}

type objectCache struct {
	dir      string
	max      int
	maxBytes int64
	mu       sync.Mutex
	lru      *list.List // front = most recently used, back = LRU victim
	elems    map[string]*list.Element
	sizes    map[string]int // sha -> byte size (for accounting)
	total    int64          // sum of sizes (guarded by mu)
	order    []string       // membership set (sha present in the cache)
	pos      map[string]int // sha -> index in order (O(1) swap-remove)
}

func newObjectCache(dir string, max int) *objectCache {
	if max <= 0 {
		max = 4096
	}
	return &objectCache{
		dir: dir, max: max, maxBytes: defaultObjectCacheMaxBytes,
		lru:   list.New(),
		elems: make(map[string]*list.Element),
		sizes: make(map[string]int),
		pos:   make(map[string]int),
	}
}

func (c *objectCache) path(sha string) string {
	return filepath.Join(c.dir, meta.ObjectPath(sha))
}

// get returns cached bytes for sha. A warm entry (verified when it entered
// the cache) is trusted with only an O(1) length check - re-hashing the whole
// object per hit was the dominant cost; a length mismatch means truncation or
// a partial write, so the entry is dropped as corrupt. A cold entry (first
// read in this process, e.g. bytes surviving from an earlier run) is verified
// against its content address once, then trusted.
func (c *objectCache) get(sha string) ([]byte, bool) {
	data, err := os.ReadFile(c.path(sha))
	if err != nil {
		return nil, false
	}
	c.mu.Lock()
	if known, ok := c.sizes[sha]; ok {
		if len(data) != known {
			// Corruption (disk rot, partial write): drop it, report a miss.
			c.removeLocked(sha)
			c.mu.Unlock()
			_ = os.Remove(c.path(sha))
			return nil, false
		}
		c.touchLocked(sha, len(data))
		c.mu.Unlock()
		return data, true
	}
	c.mu.Unlock()
	if meta.ObjectSHA(data) != sha {
		_ = os.Remove(c.path(sha))
		return nil, false
	}
	c.mu.Lock()
	c.touchLocked(sha, len(data))
	c.evictLocked()
	c.mu.Unlock()
	return data, true
}

// put stores data under sha after verifying it hashes to sha (the single
// verification point). Returns false (without storing) when the bytes do not
// match the claimed address.
func (c *objectCache) put(sha string, data []byte) bool {
	if meta.ObjectSHA(data) != sha {
		return false
	}
	p := c.path(sha)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return false
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return false
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return false
	}
	c.mu.Lock()
	c.touchLocked(sha, len(data))
	c.evictLocked()
	c.mu.Unlock()
	return true
}

// contains reports whether sha is currently cached (without reading bytes).
func (c *objectCache) contains(sha string) bool {
	c.mu.Lock()
	_, ok := c.sizes[sha]
	c.mu.Unlock()
	if !ok {
		return false
	}
	_, err := os.Stat(c.path(sha))
	return err == nil
}

// remove drops a sha from the cache (in-memory order + disk file). Used by
// prune after deleting an object upstream so a later load refetches.
func (c *objectCache) remove(sha string) {
	c.mu.Lock()
	c.removeLocked(sha)
	c.mu.Unlock()
	_ = os.Remove(c.path(sha))
}

// touchLocked records a use of sha: O(1) move-to-front on the recency list,
// inserting the entry if it is new.
func (c *objectCache) touchLocked(sha string, size int) {
	if el, ok := c.elems[sha]; ok {
		if old := c.sizes[sha]; old != size {
			c.total += int64(size - old)
			c.sizes[sha] = size
			el.Value = objectLRUItem{sha: sha, size: size}
		}
		c.lru.MoveToFront(el)
		return
	}
	c.sizes[sha] = size
	c.total += int64(size)
	c.elems[sha] = c.lru.PushFront(objectLRUItem{sha: sha, size: size})
	c.pos[sha] = len(c.order)
	c.order = append(c.order, sha)
}

// removeLocked drops sha from every structure. O(1) via the position index
// (swap-remove from order).
func (c *objectCache) removeLocked(sha string) {
	if el, ok := c.elems[sha]; ok {
		c.lru.Remove(el)
		delete(c.elems, sha)
	}
	if size, ok := c.sizes[sha]; ok {
		c.total -= int64(size)
		delete(c.sizes, sha)
	}
	if i, ok := c.pos[sha]; ok {
		last := len(c.order) - 1
		if i != last {
			moved := c.order[last]
			c.order[i] = moved
			c.pos[moved] = i
		}
		c.order = c.order[:last]
		delete(c.pos, sha)
	}
}

// evictLocked enforces the count and byte bounds, dropping least-recently-used
// entries (list back) first.
func (c *objectCache) evictLocked() {
	for c.lru.Len() > 0 && (c.lru.Len() > c.max || c.total > c.maxBytes) {
		back := c.lru.Back()
		if back == nil {
			return
		}
		sha := back.Value.(objectLRUItem).sha
		c.removeLocked(sha)
		_ = os.Remove(c.path(sha))
	}
}

// objectCacheFor returns the per-project object cache, creating it lazily.
// Caches live beneath CacheBase()/objects/<key>, where <key> is the same
// owner-qualified identity the git worktree and its lock use (gitCacheKey), so
// the orphan reaper can correlate an object cache to a live git-backed mount
// and spare it. The in-memory map is keyed by project (one owner per hub).
func (h *StorHub) objectCacheFor(project string) *objectCache {
	// Reads take RLock: the lookup is the common case and must not convoy
	// behind a single writer. Insert keeps the exclusive lock.
	h.objCacheMu.RLock()
	c, ok := h.objCaches[project]
	h.objCacheMu.RUnlock()
	if ok {
		return c
	}
	h.objCacheMu.Lock()
	defer h.objCacheMu.Unlock()
	if c, ok := h.objCaches[project]; ok {
		return c
	}
	dir := filepath.Join(h.config.ObjectCacheDir(), gitCacheKey(h.owner, project))
	c = newObjectCache(dir, h.config.ObjectCacheMaxEntries)
	h.objCaches[project] = c
	return c
}

// fetchObject loads one index object: cache first, then the repo (git worktree
// file or REST contents GET), verifying the content address on the way in. A
// missing object is an error: a manifest must never reference absent bytes.
func (h *StorHub) fetchObject(ctx context.Context, project, sha string) ([]byte, error) {
	cache := h.objectCacheFor(project)
	if data, ok := cache.get(sha); ok {
		return data, nil
	}
	var data []byte
	var err error
	if repo := h.getGitRepo(project); repo != nil {
		data, err = repo.readFileHead(ctx, objectRepoPath(sha))
	} else {
		if err = h.ensureOwner(ctx); err != nil {
			return nil, err
		}
		data, _, err = h.gh.GetFileContent(ctx, h.owner, project, objectRepoPath(sha), "")
	}
	if err != nil {
		return nil, fmt.Errorf("fetch object %s: %w", shortSHA(sha), err)
	}
	if meta.ObjectSHA(data) != sha {
		return nil, fmt.Errorf("object %s failed content verification on fetch", shortSHA(sha))
	}
	cache.put(sha, data)
	return data, nil
}

// writeObjects uploads every object in the set that is not already cached
// (cache membership means the bytes are already upstream, since objects are
// only cached after a successful fetch or write). Content-addressed writes
// are idempotent: same sha, same bytes, no conflict. Returns the number of
// objects actually uploaded. REST path only: the git backend commits objects
// together with the manifest in one multi-file commit (see publishIndex).
func (h *StorHub) writeObjects(ctx context.Context, project string, objects map[string][]byte) (int, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	if err := h.ensureOwner(ctx); err != nil {
		return 0, err
	}
	cache := h.objectCacheFor(project)
	// Deterministic order so retries and logs are stable.
	shas := sortedShas(objects)
	written := 0
	for _, sha := range shas {
		if cache.contains(sha) {
			continue
		}
		data := objects[sha]
		if _, _, err := h.gh.PutFileContent(ctx, h.owner, project, objectRepoPath(sha), data, "", "storhub: index object"); err != nil {
			var apiErr *ghapi.APIError
			// A concurrent writer may have uploaded the identical object
			// first; that is success for a content-addressed write. GitHub
			// reports a create collision two ways: 409 when a sha was
			// supplied and mismatched, and 422 ("SHA wasn't supplied" /
			// already exists) for a sha-less create onto an existing path —
			// exactly what an upstream-but-uncached object hits. Either
			// status is benign only when the upstream bytes verify against
			// the claimed address.
			if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusConflict || apiErr.StatusCode == http.StatusUnprocessableEntity) {
				upstream, _, gerr := h.gh.GetFileContent(ctx, h.owner, project, objectRepoPath(sha), "")
				var getErr *ghapi.APIError
				if gerr != nil && errors.As(gerr, &getErr) && getErr.NotFound() {
					// The path is empty upstream: this collision was NOT
					// "your object already exists". Propagate the ORIGINAL
					// status unchanged so the commit loop's conflict-rebase
					// path still recognizes it; never cache unverified bytes.
					return written, err
				}
				if gerr != nil {
					return written, fmt.Errorf("write object %s: collision (%d) and the upstream check failed: %w", shortSHA(sha), apiErr.StatusCode, gerr)
				}
				if meta.ObjectSHA(upstream) != sha {
					return written, fmt.Errorf("write object %s: collision (%d) but upstream bytes hash to %s", shortSHA(sha), apiErr.StatusCode, shortSHA(meta.ObjectSHA(upstream)))
				}
				cache.put(sha, data)
				written++
				continue
			}
			return written, fmt.Errorf("write object %s: %w", shortSHA(sha), err)
		}
		cache.put(sha, data)
		written++
	}
	return written, nil
}

func sortedShas(objects map[string][]byte) []string {
	shas := make([]string, 0, len(objects))
	for sha := range objects {
		shas = append(shas, sha)
	}
	sort.Strings(shas)
	return shas
}
