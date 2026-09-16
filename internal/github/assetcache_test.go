package github

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

func clockedClient(t *testing.T, now func() time.Time) *Client {
	t.Helper()
	cfg := storcfg.Default()
	cfg.Now = now
	return NewClient("t", cfg)
}

// TestAssetURLCacheSweepRemovesExpired pins the memory fix: entries whose
// TTL lapsed are physically deleted on the next insert, not merely
// logically expired. Before this, a mount reading 1M distinct chunks
// retained one signed URL per asset forever.
func TestAssetURLCacheSweepRemovesExpired(t *testing.T) {
	t.Parallel()
	clock := newTestClock()
	c := clockedClient(t, clock.Now)
	live := testSignedAssetURL(t, time.Time{}, clock.Now().Add(time.Hour))
	for i := 0; i < assetURLCacheCap; i++ {
		c.storeAssetURL(int64(i), live)
	}
	if len(c.assetURLs) != assetURLCacheCap {
		t.Fatalf("fresh entries must fill to the cap, len=%d", len(c.assetURLs))
	}
	clock.Advance(2 * time.Hour) // everything cached so far is now expired
	c.storeAssetURL(-7, testSignedAssetURL(t, time.Time{}, clock.Now().Add(time.Hour)))
	if len(c.assetURLs) != 1 {
		t.Fatalf("expired entries were never swept: len=%d, want 1", len(c.assetURLs))
	}
	if _, ok := c.cachedAssetURL(-7); !ok {
		t.Fatal("the fresh entry that triggered the sweep must survive it")
	}
}

// TestAssetURLCacheHardCapHoldsOldestFirst pins the burst bound: with a
// full cache of still-live entries (a sweep finds nothing to drop), an
// insert evicts the soonest-to-expire entry - a proxy for the oldest -
// and the map never exceeds the cap.
func TestAssetURLCacheHardCapHoldsOldestFirst(t *testing.T) {
	t.Parallel()
	clock := newTestClock()
	c := clockedClient(t, clock.Now)
	base := clock.Now()
	for i := 0; i < assetURLCacheCap; i++ {
		// Entry 0 expires first; the newest entry expires last.
		c.storeAssetURL(int64(i), testSignedAssetURL(t, time.Time{}, base.Add(time.Hour+time.Duration(i)*time.Second)))
	}
	c.storeAssetURL(int64(assetURLCacheCap), testSignedAssetURL(t, time.Time{}, base.Add(2*time.Hour)))
	if len(c.assetURLs) != assetURLCacheCap {
		t.Fatalf("cache exceeded cap after insert: len=%d, want %d", len(c.assetURLs), assetURLCacheCap)
	}
	if _, ok := c.cachedAssetURL(0); ok {
		t.Fatal("the soonest-expiring entry must be the eviction victim")
	}
	if _, ok := c.cachedAssetURL(int64(assetURLCacheCap)); !ok {
		t.Fatal("the newly stored entry must be cached")
	}
}

// overlapClock records how many callers were inside Now() at once. It is
// the observable for whether cachedAssetURL shares its lock (readers
// overlap) or holds it exclusively (readers serialize one by one).
type overlapClock struct {
	at     time.Time
	inside atomic.Int64
	peak   atomic.Int64
}

func (k *overlapClock) Now() time.Time {
	n := k.inside.Add(1)
	for {
		p := k.peak.Load()
		if n <= p || k.peak.CompareAndSwap(p, n) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	k.inside.Add(-1)
	return k.at
}

// TestCachedAssetURLReadsOverlap proves the read path takes RLock: N
// concurrent lookups are inside the client clock simultaneously. With the
// old exclusive Lock for reads, the peak would be exactly 1 and every
// FUSE range read would serialize globally.
func TestCachedAssetURLReadsOverlap(t *testing.T) {
	t.Parallel()
	clock := &overlapClock{at: time.Now()}
	c := clockedClient(t, clock.Now)
	c.storeAssetURL(1, testSignedAssetURL(t, time.Time{}, clock.at.Add(time.Hour)))

	const readers = 8
	var wg sync.WaitGroup
	got := make([]bool, readers)
	start := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, got[i] = c.cachedAssetURL(1)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, ok := range got {
		if !ok {
			t.Fatalf("reader %d missed a live cache entry", i)
		}
	}
	if peak := clock.peak.Load(); peak < 2 {
		t.Fatalf("reads serialized on the asset lock: max concurrent readers=%d, want >1", peak)
	}
}

// TestAssetURLCacheConcurrentReadWrite is the race-detector guard for the
// RWMutex split: readers inside RLock run against stores and
// invalidations under the write lock without data races or lost updates.
func TestAssetURLCacheConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	base := time.Now()
	c := clockedClient(t, func() time.Time { return base })
	c.storeAssetURL(1, testSignedAssetURL(t, time.Time{}, base.Add(time.Hour)))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c.cachedAssetURL(1)
				}
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.storeAssetURL(1, testSignedAssetURL(t, time.Time{}, base.Add(time.Hour)))
				c.invalidateAssetURL(1)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
