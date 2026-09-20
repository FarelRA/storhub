package fusefs

import (
	"runtime"
	"testing"
	"time"
)

func TestSafeNotifyDeleteDoesNotBlockCaller(t *testing.T) {
	oldNotifyDelete := notifyDeleteFunc
	oldConnected := fsConnectedFunc
	t.Cleanup(func() {
		notifyDeleteFunc = oldNotifyDelete
		fsConnectedFunc = oldConnected
	})
	fsConnectedFunc = func(*Filesystem) bool { return true }
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	notifyDeleteFunc = func(_ *storhubNode, _ string, _ *storhubNode) {
		started <- struct{}{}
		<-release
	}
	parent := &storhubNode{}
	child := &storhubNode{}
	done := make(chan struct{})
	go func() {
		safeNotifyDelete(parent, "swap", child)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyDelete blocked caller")
	}
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyDelete did not dispatch notification")
	}
	close(release)
}

func TestSafeNotifyEntryDoesNotBlockCaller(t *testing.T) {
	oldNotifyEntry := notifyEntryFunc
	oldConnected := fsConnectedFunc
	t.Cleanup(func() {
		notifyEntryFunc = oldNotifyEntry
		fsConnectedFunc = oldConnected
	})
	fsConnectedFunc = func(*Filesystem) bool { return true }
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	notifyEntryFunc = func(_ *storhubNode, _ string) {
		started <- struct{}{}
		<-release
	}
	node := &storhubNode{}
	done := make(chan struct{})
	go func() {
		safeNotifyEntry(node, "swap")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyEntry blocked caller")
	}
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyEntry did not dispatch notification")
	}
	close(release)
}

func TestSafeNotifySkipsWhenFilesystemUnmounted(t *testing.T) {
	dispatched := make(chan struct{}, 1)
	oldNotifyEntry := notifyEntryFunc
	oldNotifyDelete := notifyDeleteFunc
	t.Cleanup(func() {
		notifyEntryFunc = oldNotifyEntry
		notifyDeleteFunc = oldNotifyDelete
	})
	notifyEntryFunc = func(*storhubNode, string) { dispatched <- struct{}{} }
	notifyDeleteFunc = func(*storhubNode, string, *storhubNode) { dispatched <- struct{}{} }

	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := &storhubNode{fs: fsys}

	safeNotifyContent(node)
	safeNotifyEntry(node, "swap")
	safeNotifyDelete(node, "swap", nil)

	// The unmounted guard short-circuits before any dispatch goroutine is
	// spawned. If one had been spawned it would be runnable immediately,
	// and its send is buffered and non-blocking, so repeated yields give
	// it ample scheduling windows - no wall-clock wait needed.
	for i := 0; i < 1000; i++ {
		runtime.Gosched()
	}
	select {
	case <-dispatched:
		t.Fatal("notification dispatched without a FUSE connection")
	default:
	}
}
