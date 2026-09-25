package storage

// sessions_test.go: RED-first coverage for stateful open sessions
// (sessions.go, Phase 2B). Uses the existing mock-GitHub harness
// (newMockGitHub / newClient / writeTempFile / smallTransferTestConfig).

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func sessUserCtx(uid uint32) context.Context {
	return shfs.WithIdentity(context.Background(), shfs.Identity{UID: uid, GID: uid})
}

func setupSessionFile(ctx context.Context, t *testing.T, hub *StorHub, project, path string, content []byte) {
	t.Helper()
	seed := writeTempFile(t, t.TempDir(), "seed.bin", content)
	if _, err := hub.UploadFileContext(ctx, project, path, seed); err != nil {
		t.Fatalf("upload %s: %v", path, err)
	}
	if err := hub.DrainProjectContext(ctx, project); err != nil {
		t.Fatalf("drain %s: %v", project, err)
	}
}

func freshSessionBytes(t *testing.T, hub *StorHub, project, path string) []byte {
	t.Helper()
	ctx := context.Background()
	meta, _, err := hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("fresh load %s: %v", project, err)
	}
	file := meta.FindFile(path)
	if file == nil {
		t.Fatalf("fresh read: %s not found", path)
	}
	data, err := hub.ReadPinnedFileContext(ctx, project, file, meta.Chunks(), 0, file.Size)
	if err != nil {
		t.Fatalf("fresh read %s: %v", path, err)
	}
	return data
}

func mustOpenSession(ctx context.Context, t *testing.T, hub *StorHub, project, path string, mode OpenMode, opts ...SessionOption) string {
	t.Helper()
	id, err := hub.OpenSession(ctx, project, path, mode, opts...)
	if err != nil {
		t.Fatalf("open session %s: %v", path, err)
	}
	return id
}

func TestSessionOpenReadWriteCloseRoundtrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionroundtrip"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("hello"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)
	got, err := hub.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("read = %q, want %q", got, "hello")
	}
	if _, err := hub.WriteSession(ctx, id, 5, []byte(" world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err = hub.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("read after write: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("read after write = %q, want %q", got, "hello world")
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	hubB := backend.newClient(t, smallTransferTestConfig())
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "hello world" {
		t.Fatalf("second hub sees %q, want %q", got, "hello world")
	}
}

func TestSessionSnapshotStability(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionsnapshot"
	setupSessionFile(ctx, t, hubA, proj, "data.txt", []byte("version-one"))

	id := mustOpenSession(ctx, t, hubA, proj, "data.txt", SessionReadOnly)

	seed := writeTempFile(t, t.TempDir(), "v2.bin", []byte("version-two"))
	if _, err := hubB.ReplaceFileContext(ctx, proj, "data.txt", seed); err != nil {
		t.Fatalf("concurrent replace: %v", err)
	}
	if err := hubB.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain rival: %v", err)
	}

	got, err := hubA.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("pinned read: %v", err)
	}
	if string(got) != "version-one" {
		t.Fatalf("session sees %q, want pinned %q", got, "version-one")
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close clean session: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "version-two" {
		t.Fatalf("rival content = %q, want %q", got, "version-two")
	}
}

func TestSessionOwnWritesVisible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionownwrites"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("abcdef"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 0, []byte("XYZ")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := hub.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "XYZdef" {
		t.Fatalf("own write invisible: got %q, want %q", got, "XYZdef")
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestSessionMultiCallAtomicity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionatomic"
	setupSessionFile(ctx, t, hubA, proj, "data.txt", []byte("base"))

	id := mustOpenSession(ctx, t, hubA, proj, "data.txt", SessionReadWrite)
	if _, err := hubA.WriteSession(ctx, id, 4, []byte("-one")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if _, err := hubA.WriteSession(ctx, id, 8, []byte("-two")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "base" {
		t.Fatalf("partial stages visible to second hub: %q", got)
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "base-one-two" {
		t.Fatalf("second hub sees %q, want %q", got, "base-one-two")
	}
}

func TestSessionSyncMidSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionsync"
	setupSessionFile(ctx, t, hubA, proj, "data.txt", []byte("start"))

	id := mustOpenSession(ctx, t, hubA, proj, "data.txt", SessionReadWrite)
	if _, err := hubA.WriteSession(ctx, id, 5, []byte("-mid")); err != nil {
		t.Fatalf("write mid: %v", err)
	}
	if err := hubA.SyncSession(ctx, id); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "start-mid" {
		t.Fatalf("after sync second hub sees %q, want %q", got, "start-mid")
	}
	if _, err := hubA.WriteSession(ctx, id, 9, []byte("-tail")); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "start-mid-tail" {
		t.Fatalf("after close second hub sees %q, want %q", got, "start-mid-tail")
	}
}

func TestSessionPureAppend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionappend"
	setupSessionFile(ctx, t, hubA, proj, "data.txt", []byte("ab"))

	id := mustOpenSession(ctx, t, hubA, proj, "data.txt", SessionWriteOnly|SessionAppend)
	if _, err := hubA.WriteSession(ctx, id, 999, []byte("cd")); err != nil {
		t.Fatalf("append write 1: %v", err)
	}
	if _, err := hubA.WriteSession(ctx, id, 0, []byte("ef")); err != nil {
		t.Fatalf("append write 2: %v", err)
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "abcdef" {
		t.Fatalf("appended = %q, want %q", got, "abcdef")
	}
}

func TestSessionTruncateCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessiontruncate"
	setupSessionFile(ctx, t, hubA, proj, "data.txt", []byte("hello world"))

	id := mustOpenSession(ctx, t, hubA, proj, "data.txt", SessionReadWrite)
	if err := hubA.TruncateSession(ctx, id, 5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	got, err := hubA.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("truncated read = %q, want %q", got, "hello")
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "hello" {
		t.Fatalf("second hub sees %q, want %q", got, "hello")
	}
}

func TestSessionScratchLinkThenClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionscratchlink"
	if err := hubA.MkdirContext(ctx, proj, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hubA.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain mkdir: %v", err)
	}

	id := mustOpenSession(ctx, t, hubA, proj, "", SessionReadWrite)
	if _, err := hubA.WriteSession(ctx, id, 0, []byte("scratch-data")); err != nil {
		t.Fatalf("write scratch: %v", err)
	}
	got, err := hubA.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("read scratch: %v", err)
	}
	if string(got) != "scratch-data" {
		t.Fatalf("scratch read = %q, want %q", got, "scratch-data")
	}
	if err := hubA.LinkSession(ctx, id, "docs/linked.txt"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "docs/linked.txt"); string(got) != "scratch-data" {
		t.Fatalf("linked content = %q, want %q", got, "scratch-data")
	}
}

func TestSessionScratchCloseWithoutLinkDiscards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionscratchdrop"

	id := mustOpenSession(ctx, t, hub, proj, "", SessionWriteOnly)
	if _, err := hub.WriteSession(ctx, id, 0, []byte("doomed")); err != nil {
		t.Fatalf("write scratch: %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close unlinked scratch must succeed: %v", err)
	}
	if _, err := hub.StatPathContext(ctx, proj, "doomed.txt"); err == nil {
		t.Fatal("unlinked scratch close created a file, want discard")
	}
	if _, err := hub.ReadSession(ctx, id, 0, 1); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("closed handle must be stale, got %v", err)
	}
}

func TestSessionTTLExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	// Wall time for the whole hub: the mock harness freezes the clock
	// for determinism, which would freeze idle expiry too. Set before
	// the client exists; reassigning after background loops start
	// races the reader in commitProjectMetadata.
	cfg.Now = time.Now
	hub := backend.newClient(t, cfg)
	hub.ConfigureSessions(WithSessionDefaultTTL(40 * time.Millisecond))

	id := mustOpenSession(ctx, t, hub, "projectsessionttl", "", SessionReadWrite)
	time.Sleep(100 * time.Millisecond)
	if _, err := hub.ReadSession(ctx, id, 0, 1); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("expired handle must be stale, got %v", err)
	}
	var stale *StaleSessionError
	if _, err := hub.ReadSession(ctx, id, 0, 1); !errors.As(err, &stale) {
		t.Fatalf("want typed StaleSessionError, got %v", err)
	}

	id2 := mustOpenSession(ctx, t, hub, "projectsessionttl", "", SessionWriteOnly)
	sh := hub.sessionHub()
	sh.mu.Lock()
	live := len(sh.byID)
	sh.mu.Unlock()
	if live != 1 {
		t.Fatalf("open must sweep the expired handle: %d live, want 1", live)
	}
	if err := hub.CloseSession(ctx, id2); err != nil {
		t.Fatalf("close survivor: %v", err)
	}
}

func TestSessionStaleIDError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	_, err := hub.ReadSession(ctx, "0123456789abcdef0123456789abcdef", 0, 1)
	var stale *StaleSessionError
	if !errors.As(err, &stale) {
		t.Fatalf("unknown id must answer StaleSessionError, got %v", err)
	}
	if !errors.Is(err, ErrStaleSession) {
		t.Fatalf("StaleSessionError must wrap ErrStaleSession: %v", err)
	}
}

func TestSessionCapsEnforced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	hub.ConfigureSessions(WithSessionMaxPerProject(2), WithSessionMaxPerUser(100))
	proj := "projectsessioncaps"

	id1 := mustOpenSession(ctx, t, hub, proj, "", SessionWriteOnly)
	id2 := mustOpenSession(ctx, t, hub, proj, "", SessionWriteOnly)
	if _, err := hub.OpenSession(ctx, proj, "", SessionWriteOnly); !errors.Is(err, ErrSessionProjectBusy) {
		t.Fatalf("third handle must hit the project cap, got %v", err)
	}
	for _, id := range []string{id1, id2} {
		if err := hub.CloseSession(ctx, id); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
}

func TestSessionUserCapEnforced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	hub.ConfigureSessions(WithSessionMaxPerUser(1), WithSessionMaxPerProject(100))

	id := mustOpenSession(ctx, t, hub, "projectsessionusera", "", SessionWriteOnly)
	if _, err := hub.OpenSession(ctx, "projectsessionuserb", "", SessionWriteOnly); !errors.Is(err, ErrSessionUserBusy) {
		t.Fatalf("second project handle must hit the user cap, got %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestSessionOwnershipMismatchFailsClosed(t *testing.T) {
	t.Parallel()
	hub, adminCtx, _, target := setupPrivProject(t, "projectsessionowner", "docs", "f.txt", []byte("secret"), 0o644)
	otherCtx := sessUserCtx(1002)

	id := mustOpenSession(privUserCtx(), t, hub, "projectsessionowner", target, SessionReadOnly)

	if _, err := hub.ReadSession(otherCtx, id, 0, 64); !errors.Is(err, ErrSessionOwnerMismatch) {
		t.Fatalf("stranger read must fail closed, got %v", err)
	}
	if _, err := hub.ReadSession(otherCtx, id, 0, 64); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("mismatch must wrap EPERM, got %v", err)
	}
	got, err := hub.ReadSession(privUserCtx(), id, 0, 64)
	if err != nil {
		t.Fatalf("owner read: %v", err)
	}
	if string(got) != "secret" {
		t.Fatalf("owner read = %q, want %q", got, "secret")
	}
	got, err = hub.ReadSession(adminCtx, id, 0, 64)
	if err != nil {
		t.Fatalf("admin bypass read: %v", err)
	}
	if string(got) != "secret" {
		t.Fatalf("admin read = %q, want %q", got, "secret")
	}
	if _, err := hub.WriteSession(otherCtx, id, 0, []byte("x")); !errors.Is(err, ErrSessionOwnerMismatch) {
		t.Fatalf("stranger write must fail closed, got %v", err)
	}
	if err := hub.CloseSession(privUserCtx(), id); err != nil {
		t.Fatalf("owner close: %v", err)
	}
}

func TestSessionRestartDrops(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())

	id := mustOpenSession(ctx, t, hubA, "projectsessionrestart", "", SessionWriteOnly)
	if _, err := hubB.ReadSession(ctx, id, 0, 1); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("fresh hub must not know the handle, got %v", err)
	}
	if _, err := hubA.StatSession(ctx, id); err != nil {
		t.Fatalf("original hub must keep its handle: %v", err)
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestSessionSabotagedCommitFailsLoud sabotage-verifies the fail-loud
// contract: with the remote contents endpoint faulted, Sync must report the
// error and retain staged state for retry instead of dropping it.
func TestSessionSabotagedCommitFailsLoud(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionsabotage"
	setupSessionFile(ctx, t, hubA, proj, "data.txt", []byte("base"))

	id := mustOpenSession(ctx, t, hubA, proj, "data.txt", SessionReadWrite)
	if _, err := hubA.WriteSession(ctx, id, 4, []byte("-staged")); err != nil {
		t.Fatalf("write: %v", err)
	}
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"injected failure"}`))
			return true
		}
		return false
	})
	if err := hubA.SyncSession(ctx, id); err == nil {
		t.Fatal("sync over failing remote must fail loud, got nil")
	}
	stat, err := hubA.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat after failed sync: %v", err)
	}
	if !stat.Dirty {
		t.Fatal("failed sync must retain staged state for retry")
	}
	backend.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	if err := hubA.SyncSession(ctx, id); err != nil {
		t.Fatalf("retry sync: %v", err)
	}
	if err := hubA.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := freshSessionBytes(t, hubB, proj, "data.txt"); string(got) != "base-staged" {
		t.Fatalf("second hub sees %q, want %q", got, "base-staged")
	}
}

func TestSessionModeValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionmodes"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("hello"))

	if _, err := hub.OpenSession(ctx, proj, "data.txt", 0); err == nil {
		t.Fatal("empty mode must fail")
	}
	if _, err := hub.OpenSession(ctx, proj, "data.txt", SessionReadOnly|SessionTruncate); err == nil {
		t.Fatal("truncate without write must fail")
	}
	if _, err := hub.OpenSession(ctx, proj, "data.txt", SessionReadOnly|SessionAppend); err == nil {
		t.Fatal("append without write must fail")
	}
	if _, err := hub.OpenSession(ctx, proj, "data.txt", SessionReadOnly|SessionExclusive); err == nil {
		t.Fatal("exclusive without create must fail")
	}
	if _, err := hub.OpenSession(ctx, proj, "missing.txt", SessionReadOnly); err == nil {
		t.Fatal("read of missing file must fail")
	}
	if _, err := hub.OpenSession(ctx, proj, "missing.txt", SessionWriteOnly); err == nil {
		t.Fatal("write without create of missing file must fail")
	}

	ro := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadOnly)
	if _, err := hub.WriteSession(ctx, ro, 0, []byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("write on read-only handle must be EBADF, got %v", err)
	}
	wo := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionWriteOnly)
	if _, err := hub.ReadSession(ctx, wo, 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("read on write-only handle must be EBADF, got %v", err)
	}
	if err := hub.CloseSession(ctx, ro); err != nil {
		t.Fatalf("close ro: %v", err)
	}
	if err := hub.CloseSession(ctx, wo); err != nil {
		t.Fatalf("close wo: %v", err)
	}
}

func TestParseOpenMode(t *testing.T) {
	t.Parallel()
	cases := map[string]OpenMode{
		"r":  SessionReadOnly,
		"r+": SessionReadWrite,
		"w":  SessionWriteOnly | SessionCreate | SessionTruncate,
		"w+": SessionReadWrite | SessionCreate | SessionTruncate,
		"a":  SessionWriteOnly | SessionCreate | SessionAppend,
		"a+": SessionReadWrite | SessionCreate | SessionAppend,
	}
	for in, want := range cases {
		got, err := ParseOpenMode(in)
		if err != nil {
			t.Fatalf("parse %q: %v", in, err)
		}
		if got != want {
			t.Fatalf("parse %q = %s, want %s", in, got, want)
		}
	}
	got, err := ParseOpenMode("wx")
	if err != nil {
		t.Fatalf("parse wx: %v", err)
	}
	if got&SessionExclusive == 0 || got&SessionCreate == 0 {
		t.Fatalf("parse wx = %s, want create+exclusive", got)
	}
	if _, err := ParseOpenMode("bogus"); err == nil {
		t.Fatal("bogus mode must fail")
	}
}

func TestOpenModeStringIsDebugOnly(t *testing.T) {
	t.Parallel()
	// String renders debug spellings ("rw", "r+w", "+create") that are
	// deliberately not fopen inputs: the mapping is one-way by design.
	// ("r" alone is both, a valid fopen input and the read-only bit.)
	for _, debug := range []string{"rw", "r+w", "w+create+trunc", "none"} {
		if _, err := ParseOpenMode(debug); err == nil {
			t.Fatalf("debug spelling %q must not parse as a mode", debug)
		}
	}
}

func TestSessionCommitAsOpenerNotCloser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionopener"
	adminCtx := shfs.WithIdentity(ctx, shfs.Identity{UID: 0, GID: 0, Admin: true})
	userA := sessUserCtx(1001)

	if err := hub.MkdirContext(adminCtx, proj, "sub"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, proj, "sub", 0o777); err != nil {
		t.Fatalf("chmod sub: %v", err)
	}
	id := mustOpenSession(userA, t, hub, proj, "", SessionReadWrite)
	if _, err := hub.WriteSession(userA, id, 0, []byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.LinkSession(userA, id, "sub/a.txt"); err != nil {
		t.Fatalf("link: %v", err)
	}
	// An admin closes a user's session: the files must still belong to
	// the opener who staged the bytes, not to the admin.
	if err := hub.CloseSession(adminCtx, id); err != nil {
		t.Fatalf("admin close: %v", err)
	}
	meta, _, err := hub.loadRepoMetadataFresh(ctx, proj)
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	got := meta.FindFile("sub/a.txt")
	if got == nil {
		t.Fatal("committed file missing")
	}
	if got.UID != 1001 {
		t.Fatalf("commit owner: want opener 1001, got %d", got.UID)
	}
}

func TestSessionRelinkRescuesTakenTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionrelink"
	adminCtx := shfs.WithIdentity(ctx, shfs.Identity{UID: 0, GID: 0, Admin: true})
	userA := sessUserCtx(1001)

	if err := hub.MkdirContext(adminCtx, proj, "sub"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, proj, "sub", 0o777); err != nil {
		t.Fatalf("chmod sub: %v", err)
	}
	id := mustOpenSession(userA, t, hub, proj, "", SessionReadWrite)
	if _, err := hub.WriteSession(userA, id, 0, []byte("data")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.LinkSession(userA, id, "sub/a.txt"); err != nil {
		t.Fatalf("link: %v", err)
	}
	// A concurrent writer takes the linked target before close.
	seed := writeTempFile(t, t.TempDir(), "rival.bin", []byte("rival"))
	if _, err := hub.UploadFileContext(adminCtx, proj, "sub/a.txt", seed); err != nil {
		t.Fatalf("rival upload: %v", err)
	}
	if err := hub.CloseSession(userA, id); err == nil {
		t.Fatal("close over a taken target must fail, got nil")
	}
	// Relink to a free name rescues the staged bytes; the rival keeps
	// its own content untouched.
	if err := hub.RelinkSession(userA, id, "sub/b.txt"); err != nil {
		t.Fatalf("relink: %v", err)
	}
	if err := hub.CloseSession(userA, id); err != nil {
		t.Fatalf("close after relink: %v", err)
	}
	meta, _, err := hub.loadRepoMetadataFresh(ctx, proj)
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	rescued := meta.FindFile("sub/b.txt")
	if rescued == nil {
		t.Fatal("relocated file missing")
	}
	rival := meta.FindFile("sub/a.txt")
	if rival == nil {
		t.Fatal("rival file missing")
	}
	if rescued.Inode == rival.Inode {
		t.Fatal("relink must not disturb the rival entry")
	}
}
