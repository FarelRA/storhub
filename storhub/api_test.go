package storhub

import (
	"context"
	"os"
	"path/filepath"
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

func TestIntegrityWrappers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload.txt")
	data := []byte("payload")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	fullCRC, err := chunking.CalculateCRC32CStreaming(path, 1)
	if err != nil {
		t.Fatalf("calculate crc: %v", err)
	}
	chunks := []ChunkInfo{{Index: 0, Offset: 0, Size: int64(len(data)), CRC32C: fullCRC}}
	combined, err := CombineChunkCRC32Cs(chunks)
	if err != nil || combined != fullCRC {
		t.Fatalf("unexpected combined crc: %q %v", combined, err)
	}
	if err := VerifyFileIntegrity(path, FileMetadata{Name: "payload", Size: int64(len(data)), CRC32C: fullCRC, Chunks: chunks}, 1); err != nil {
		t.Fatalf("verify integrity wrapper: %v", err)
	}
}
