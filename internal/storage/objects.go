package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// indexFilePath is the v2 manifest: the single CAS point of a v2 project.
const indexFilePath = ".storhub/index.json"

// objectRepoPath maps an object sha to its repo path under .storhub.
func objectRepoPath(sha string) string {
	return ".storhub/" + meta.ObjectPath(sha)
}

// objectCache is the client-side cache of content-addressed index objects.
// Objects are immutable (name = sha256 of content), so caching is trivially
// correct: every read re-verifies the bytes against the name, and a mismatch
// is treated as a miss (the corrupt file is dropped so a refetch repopulates).
// The cache is what makes the Merkle layout viable: an eager cold load fetches
// every node once, and later commits only touch the changed chain because
// unchanged subtrees are already cached (and never re-uploaded).
//
// It is LRU-bounded so a long-lived mount cannot grow without limit; eviction
// only costs a later refetch, never correctness.
type objectCache struct {
	dir   string
	max   int
	mu    sync.Mutex
	order []string       // shas, least-recently-used first
	sizes map[string]int // sha -> byte size (for accounting)
}

func newObjectCache(dir string, max int) *objectCache {
	if max <= 0 {
		max = 4096
	}
	return &objectCache{dir: dir, max: max, sizes: make(map[string]int)}
}

func (c *objectCache) path(sha string) string {
	return filepath.Join(c.dir, meta.ObjectPath(sha))
}

// get returns cached bytes for sha, verifying they hash to sha. A miss (or a
// verification failure) returns ok=false.
func (c *objectCache) get(sha string) ([]byte, bool) {
	data, err := os.ReadFile(c.path(sha))
	if err != nil {
		return nil, false
	}
	if meta.ObjectSHA(data) != sha {
		// Corruption (disk rot, partial write): drop it and report a miss.
		_ = os.Remove(c.path(sha))
		c.mu.Lock()
		c.removeLocked(sha)
		c.mu.Unlock()
		return nil, false
	}
	c.mu.Lock()
	c.touchLocked(sha, len(data))
	c.mu.Unlock()
	return data, true
}

// put stores data under sha after verifying it hashes to sha. Returns false
// (without storing) when the bytes do not match the claimed address.
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

func (c *objectCache) touchLocked(sha string, size int) {
	if _, ok := c.sizes[sha]; ok {
		c.removeLocked(sha)
	}
	c.sizes[sha] = size
	c.order = append(c.order, sha)
}

func (c *objectCache) removeLocked(sha string) {
	for i, s := range c.order {
		if s == sha {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	delete(c.sizes, sha)
}

func (c *objectCache) evictLocked() {
	for len(c.order) > c.max {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.sizes, oldest)
		_ = os.Remove(c.path(oldest))
	}
}

// objectCacheFor returns the per-project object cache, creating it lazily.
// Caches live beneath CacheBase()/objects/<project>.
func (h *StorHub) objectCacheFor(project string) *objectCache {
	h.objCacheMu.Lock()
	defer h.objCacheMu.Unlock()
	if c, ok := h.objCaches[project]; ok {
		return c
	}
	dir := filepath.Join(h.config.ObjectCacheDir(), project)
	c := newObjectCache(dir, h.config.ObjectCacheMaxEntries)
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
// objects actually uploaded.
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
		if repo := h.getGitRepo(project); repo != nil {
			// Git objects are committed together with the manifest in one
			// multi-file commit by the caller; here we only need them in the
			// cache so the manifest CAS sees them as present.
			cache.put(sha, data)
			written++
			continue
		}
		if _, _, err := h.gh.PutFileContent(ctx, h.owner, project, objectRepoPath(sha), data, "", "storhub: index object"); err != nil {
			var apiErr *ghapi.APIError
			// A concurrent writer may have uploaded the identical object
			// first; that is success for a content-addressed write.
			if errors.As(err, &apiErr) && apiErr.StatusCode == 409 {
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
