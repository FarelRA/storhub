package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func objBytes(s string) []byte { return []byte(s) }

func TestObjectCachePutGetHit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := newObjectCache(dir, 128)
	data := objBytes(`{"m":{"i":1}}`)
	sha := meta.ObjectSHA(data)
	if !c.put(sha, data) {
		t.Fatal("put rejected valid object")
	}
	got, ok := c.get(sha)
	if !ok || !bytes.Equal(got, data) {
		t.Fatalf("cache miss or wrong bytes after put: ok=%v got=%q", ok, got)
	}
	// The object lives at objects/<2hex>/<62hex> beneath the cache dir.
	if _, err := os.Stat(filepath.Join(dir, meta.ObjectPath(sha))); err != nil {
		t.Fatalf("object not at expected path: %v", err)
	}
}

func TestObjectCacheGetMiss(t *testing.T) {
	t.Parallel()
	c := newObjectCache(t.TempDir(), 128)
	if _, ok := c.get(meta.ObjectSHA(objBytes("nope"))); ok {
		t.Fatal("expected miss for uncached object")
	}
}

func TestObjectCacheVerifyOnRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := newObjectCache(dir, 128)
	data := objBytes("good content")
	sha := meta.ObjectSHA(data)
	c.put(sha, data)
	// Corrupt the cached bytes on disk: a content-addressed cache must never
	// return bytes that do not hash to their name.
	if err := os.WriteFile(filepath.Join(dir, meta.ObjectPath(sha)), objBytes("TAMPERED"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.get(sha); ok {
		t.Fatal("get returned tampered bytes that do not match their sha")
	}
	// The corrupt file must have been dropped so a refetch can repopulate.
	if _, err := os.Stat(filepath.Join(dir, meta.ObjectPath(sha))); !os.IsNotExist(err) {
		t.Fatal("corrupt object not removed on verify failure")
	}
}

func TestObjectCachePutRejectsMismatchedSHA(t *testing.T) {
	t.Parallel()
	c := newObjectCache(t.TempDir(), 128)
	// Claiming the wrong address must not store the object.
	if c.put(meta.ObjectSHA(objBytes("other")), objBytes("this")) {
		t.Fatal("put accepted bytes that do not hash to the claimed sha")
	}
}

func TestObjectCacheLRUEviction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	max := 4
	c := newObjectCache(dir, max)
	shas := make([]string, 0, max+2)
	for i := 0; i < max+2; i++ {
		data := objBytes(fmt.Sprintf("object-%d", i))
		sha := meta.ObjectSHA(data)
		c.put(sha, data)
		shas = append(shas, sha)
	}
	// The two oldest must be evicted; the newest max must survive.
	if _, ok := c.get(shas[0]); ok {
		t.Fatal("oldest object not evicted")
	}
	if _, ok := c.get(shas[1]); ok {
		t.Fatal("second-oldest object not evicted")
	}
	for _, sha := range shas[2:] {
		if _, ok := c.get(sha); !ok {
			t.Fatalf("recent object %s wrongly evicted", sha[:8])
		}
	}
	// Evicted files are gone from disk too.
	if _, err := os.Stat(filepath.Join(dir, meta.ObjectPath(shas[0]))); !os.IsNotExist(err) {
		t.Fatal("evicted object still on disk")
	}
}

func TestObjectCacheGetRefreshesRecency(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := newObjectCache(dir, 2)
	a := objBytes("a")
	b := objBytes("b")
	d := objBytes("d")
	sa, sb, sd := meta.ObjectSHA(a), meta.ObjectSHA(b), meta.ObjectSHA(d)
	c.put(sa, a)
	c.put(sb, b)
	// Touch a so it becomes most-recent, then insert d: b (now oldest) evicts.
	c.get(sa)
	c.put(sd, d)
	if _, ok := c.get(sa); !ok {
		t.Fatal("recently-read object was evicted")
	}
	if _, ok := c.get(sb); ok {
		t.Fatal("least-recently-used object survived eviction")
	}
}

// The entry count alone is not a disk bound (an object can be up to the
// contents-API size limit), so eviction must also honor a byte budget,
// evicting least-recently-used first and keeping the accounting consistent.
func TestObjectCacheByteBudgetEviction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := newObjectCache(dir, 1000) // count cap irrelevant; bytes must bind
	c.maxBytes = 100
	var shas []string
	total := 0
	for i := 0; total <= 100; i++ {
		data := objBytes(strings.Repeat("x", 10) + fmt.Sprintf("%d", i))
		sha := meta.ObjectSHA(data)
		c.put(sha, data)
		shas = append(shas, sha)
		total += len(data)
	}
	c.mu.Lock()
	gotTotal, gotCount := c.total, len(c.elems)
	c.mu.Unlock()
	if gotTotal > c.maxBytes {
		t.Fatalf("byte budget exceeded after eviction: total=%d max=%d", gotTotal, c.maxBytes)
	}
	if gotCount == 0 {
		t.Fatal("byte eviction removed everything")
	}
	// The oldest entries are the ones gone, and gone from disk too.
	if _, ok := c.get(shas[0]); ok {
		t.Fatal("oldest object survived byte eviction")
	}
	if _, err := os.Stat(filepath.Join(dir, meta.ObjectPath(shas[0]))); !os.IsNotExist(err) {
		t.Fatal("byte-evicted object still on disk")
	}
}

// A single object larger than the whole budget must be evicted immediately,
// not wedge the eviction loop.
func TestObjectCacheOversizedSinglePut(t *testing.T) {
	t.Parallel()
	c := newObjectCache(t.TempDir(), 100)
	c.maxBytes = 10
	data := objBytes(strings.Repeat("y", 50))
	sha := meta.ObjectSHA(data)
	if !c.put(sha, data) {
		t.Fatal("put rejected valid object")
	}
	if _, ok := c.get(sha); ok {
		t.Fatal("object above the byte budget survived")
	}
	c.mu.Lock()
	total := c.total
	c.mu.Unlock()
	if total != 0 {
		t.Fatalf("byte accounting drifted on eviction: total=%d", total)
	}
}

// GitHub answers a sha-less create onto an existing path with 422 (and
// 409 when a supplied sha mismatched). Both are benign for a content-
// addressed write exactly when the upstream bytes carry the claimed address:
// the object is already there, so the write counts as success.
func TestWriteObjectsTreatsVerifiedCollisionAsSuccess(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusUnprocessableEntity} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			t.Setenv("STORHUB_CACHE_DIR", t.TempDir())
			ctx := context.Background()
			backend := newMockGitHub(t)
			hub := backend.newClient(t, smallTransferTestConfig())
			if err := hub.EnsureRepoContext(ctx, "collision-ok"); err != nil {
				t.Fatalf("ensure repo: %v", err)
			}
			data := objBytes(`{"m":{"i":7},"f":{}}`)
			sha := meta.ObjectSHA(data)
			// The object is already upstream (LRU evicted it from our cache,
			// or a crash-retry regenerated it): the sha-less create collides.
			if _, _, err := hub.gh.PutFileContent(ctx, hub.owner, "collision-ok", objectRepoPath(sha), data, "", "pre-place"); err != nil {
				t.Fatalf("pre-place: %v", err)
			}
			backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/objects/") {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"message":"Validation Failed","errors":[{"resource":"Content","code":"already_exists","field":"content","message":"SHA wasn't supplied"}]}`))
					return true
				}
				return false
			})
			written, err := hub.writeObjects(ctx, "collision-ok", map[string][]byte{sha: data})
			if err != nil {
				t.Fatalf("collision onto matching upstream bytes must be benign at %d: %v", status, err)
			}
			if written != 1 {
				t.Fatalf("benign collision must count as written, got %d", written)
			}
			if !hub.objectCacheFor("collision-ok").contains(sha) {
				t.Fatal("benign collision must populate the cache")
			}
		})
	}
}

// Conversely, a collision whose upstream bytes do NOT hash to the
// claimed address is not our object and must fail loudly, never be swallowed.
func TestWriteObjectsCollisionWithForeignContentFails(t *testing.T) {
	t.Setenv("STORHUB_CACHE_DIR", t.TempDir())
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.EnsureRepoContext(ctx, "collision-mismatch"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	data := objBytes(`{"m":{"i":8},"f":{}}`)
	sha := meta.ObjectSHA(data)
	// Upstream holds DIFFERENT bytes at the object path.
	if _, _, err := hub.gh.PutFileContent(ctx, hub.owner, "collision-mismatch", objectRepoPath(sha), objBytes("foreign bytes"), "", "foreign"); err != nil {
		t.Fatalf("foreign pre-place: %v", err)
	}
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/objects/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"message":"Validation Failed","errors":[{"resource":"Content","code":"already_exists","field":"content"}]}`))
			return true
		}
		return false
	})
	if _, err := hub.writeObjects(ctx, "collision-mismatch", map[string][]byte{sha: data}); err == nil {
		t.Fatal("collision onto foreign bytes must fail")
	}
	if hub.objectCacheFor("collision-mismatch").contains(sha) {
		t.Fatal("failed write must not cache the object")
	}
}

// A collision status on a path that is EMPTY upstream is not "your object
// already exists": the original error must propagate unchanged (a 409 stays
// a 409 so the commit loop's conflict-rebase path still recognizes it) and
// the unverified bytes must never be cached.
func TestWriteObjectsCollisionOnAbsentPathPropagatesOriginal(t *testing.T) {
	t.Setenv("STORHUB_CACHE_DIR", t.TempDir())
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.EnsureRepoContext(ctx, "collision-absent"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	data := objBytes(`{"m":{"i":9},"f":{}}`)
	sha := meta.ObjectSHA(data)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/objects/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"sha does not match"}`))
			return true
		}
		return false
	})
	_, err := hub.writeObjects(ctx, "collision-absent", map[string][]byte{sha: data})
	if err == nil {
		t.Fatal("collision onto an absent path must fail")
	}
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("original 409 must propagate for the commit loop's rebase path, got %v", err)
	}
	if hub.objectCacheFor("collision-absent").contains(sha) {
		t.Fatal("unverified bytes must not be cached")
	}
}
