package storhub

import (
	"context"
	"reflect"
	"testing"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	implfuse "github.com/FarelRA/storhub/internal/fusefs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
	impl "github.com/FarelRA/storhub/internal/storage"
)

func TestPublicAliasesAndConstants(t *testing.T) {
	if reflect.TypeOf(StorHub{}) != reflect.TypeOf(impl.StorHub{}) {
		t.Fatal("StorHub alias mismatch")
	}
	if reflect.TypeOf(FUSEOptions{}) != reflect.TypeOf(implfuse.Options{}) {
		t.Fatal("FUSEOptions alias mismatch")
	}
	if reflect.TypeOf(ChunkInfo{}) != reflect.TypeOf(meta.ChunkInfo{}) {
		t.Fatal("ChunkInfo alias mismatch")
	}
	if reflect.TypeOf(APIError{}) != reflect.TypeOf(ghapi.APIError{}) {
		t.Fatal("APIError alias mismatch")
	}
	if DefaultChunkSize != chunking.DefaultChunkSize || MaxReleaseAssetSize != chunking.MaxReleaseAssetSize {
		t.Fatal("chunking constants mismatch")
	}
	if NodeKindFile != meta.NodeKindFile || NodeKindSymlink != meta.NodeKindSymlink {
		t.Fatal("node kind constants mismatch")
	}
	if ErrFileNotFound.Error() != "file not found" || ErrProjectNotFound.Error() != "project not found" {
		t.Fatal("public errors mismatch")
	}
}

func TestDefaultConfigAndConstructors(t *testing.T) {
	if DefaultConfig().ChunkSize == 0 {
		t.Fatal("expected non-zero default config")
	}
	if _, err := NewStorHub(""); err == nil {
		t.Fatal("expected constructor to reject empty token")
	}
	if _, err := NewStorHubWithConfig("", Config{}); err == nil {
		t.Fatal("expected config constructor to reject empty token")
	}
	if _, err := NewStorHubWithContext(context.Background(), "", Config{}); err == nil {
		t.Fatal("expected context constructor to reject empty token")
	}
	if DefaultFUSEOptions().PageSize == 0 {
		t.Fatal("expected fuse defaults")
	}
}
