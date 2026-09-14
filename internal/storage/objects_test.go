package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func objBytes(s string) []byte { return []byte(s) }

func TestObjectCachePutGetHit(t *testing.T) {
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
	c := newObjectCache(t.TempDir(), 128)
	if _, ok := c.get(meta.ObjectSHA(objBytes("nope"))); ok {
		t.Fatal("expected miss for uncached object")
	}
}

func TestObjectCacheVerifyOnRead(t *testing.T) {
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
	c := newObjectCache(t.TempDir(), 128)
	// Claiming the wrong address must not store the object.
	if c.put(meta.ObjectSHA(objBytes("other")), objBytes("this")) {
		t.Fatal("put accepted bytes that do not hash to the claimed sha")
	}
}

func TestObjectCacheLRUEviction(t *testing.T) {
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
