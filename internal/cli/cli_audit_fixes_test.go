package cli

import (
	"context"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

// TestCpNeverWithExpectedRevisionIsUsageError pins R6: --reflink=never
// streams bytes through plain WriteFileAt with no revision plumbing, so
// pairing it with --expected-revision would silently drop the CAS guard.
// The combination fails loud as a usage error instead.
func TestCpNeverWithExpectedRevisionIsUsageError(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("hello world")}}
	err := runCpWithFake(t, fake, []string{"cp", "--reflink=never", "--expected-revision", "rev-1", "demo", "a.txt", "b.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("--reflink=never with --expected-revision must be a usage error, got %v", err)
	}
	if fake.cloneCalls != 0 || fake.writeCalls != 0 {
		t.Fatalf("rejected combination must not copy: clone=%d write=%d", fake.cloneCalls, fake.writeCalls)
	}
}

// TestTouchToleratesExistingCreate pins R10: touch creates first and
// tolerates AlreadyExists (a concurrent touch won the race), so the
// timestamp update always lands without check-then-act.
func TestTouchToleratesExistingCreate(t *testing.T) {
	fake := &touchExistsFake{}
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"touch", "--token", "x", "demo", "docs/f.txt"}); err != nil {
		t.Fatalf("touch on an existing path must succeed, got %v", err)
	}
	if !fake.stamped {
		t.Fatal("touch on an existing path must update timestamps")
	}
}

// touchExistsFake reports every path as already created: CreateFile always
// answers AlreadyExists so the test proves touch falls through to Chtimes.
type touchExistsFake struct {
	hubClient
	stamped bool
}

func (f *touchExistsFake) CreateFile(project, filePath string) (*storhub.FileMetadata, error) {
	return nil, shfs.AlreadyExists(filePath)
}

func (f *touchExistsFake) Chtimes(project, targetPath string, atime, mtime int64) error {
	f.stamped = true
	return nil
}

func (f *touchExistsFake) Shutdown(ctx context.Context) error { return nil }
