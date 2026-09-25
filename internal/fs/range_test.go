package fs

import (
	"errors"
	"reflect"
	"testing"
)

// One ByteRange span plus one merge: the single range representation.
func TestByteRangeMerge(t *testing.T) {
	t.Parallel()
	got := MergeByteRanges([]ByteRange{{Start: 20, End: 25}, {Start: 0, End: 10}, {Start: 5, End: 15}})
	want := []ByteRange{{Start: 0, End: 15}, {Start: 20, End: 25}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge got %+v want %+v", got, want)
	}
	got = MergeByteRanges([]ByteRange{{Start: 0, End: 10}, {Start: 10, End: 20}})
	if !reflect.DeepEqual(got, []ByteRange{{Start: 0, End: 20}}) {
		t.Fatalf("adjacent spans must coalesce, got %+v", got)
	}
	if err := ValidateByteRange(ByteRange{Start: 5, End: 3}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("inverted span must fail with ErrInvalidArgument, got %v", err)
	}
	if err := ValidateByteRange(ByteRange{Start: -1, End: 3}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative span must fail with ErrInvalidArgument, got %v", err)
	}
}
