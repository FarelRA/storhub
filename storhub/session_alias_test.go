package storhub

import (
	"reflect"
	"testing"

	impl "github.com/FarelRA/storhub/internal/storage"
)

// The facade must export the whole session surface plus the prune error
// and request types: embedders drive sessions through this package, never
// through internal/storage directly.
func TestPublicSessionAliases(t *testing.T) {
	if reflect.TypeOf(OpenMode(0)) != reflect.TypeOf(impl.OpenMode(0)) {
		t.Fatal("OpenMode alias mismatch")
	}
	if reflect.TypeOf(SessionStat{}) != reflect.TypeOf(impl.SessionStat{}) {
		t.Fatal("SessionStat alias mismatch")
	}
	if reflect.TypeOf(StaleSessionError{}) != reflect.TypeOf(impl.StaleSessionError{}) {
		t.Fatal("StaleSessionError alias mismatch")
	}
	if reflect.TypeOf(PruneRequest{}) != reflect.TypeOf(impl.PruneRequest{}) {
		t.Fatal("PruneRequest alias mismatch")
	}
	if reflect.TypeOf(PruneConflictError{}) != reflect.TypeOf(impl.PruneConflictError{}) {
		t.Fatal("PruneConflictError alias mismatch")
	}
	if reflect.TypeOf(ChunkGCRefusedError{}) != reflect.TypeOf(impl.ChunkGCRefusedError{}) {
		t.Fatal("ChunkGCRefusedError alias mismatch")
	}
	if SessionReadOnly != impl.SessionReadOnly || SessionWriteOnly != impl.SessionWriteOnly ||
		SessionReadWrite != impl.SessionReadWrite || SessionCreate != impl.SessionCreate ||
		SessionTruncate != impl.SessionTruncate || SessionAppend != impl.SessionAppend ||
		SessionExclusive != impl.SessionExclusive {
		t.Fatal("session mode constants mismatch")
	}
	for _, err := range []error{ErrStaleSession, ErrSessionProjectBusy, ErrSessionUserBusy, ErrSessionOwnerMismatch, ErrSessionUnlinked, ErrSessionLinked, ErrSessionPathGone} {
		if err == nil {
			t.Fatal("session error alias is nil")
		}
	}
	if ErrStaleSession.Error() != impl.ErrStaleSession.Error() {
		t.Fatal("ErrStaleSession mismatch")
	}
	if mode, err := ParseOpenMode("w"); err != nil || mode != SessionWriteOnly|SessionCreate|SessionTruncate {
		t.Fatalf("ParseOpenMode(w) = %v, %v", mode, err)
	}
}
