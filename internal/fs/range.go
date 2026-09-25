package fs

import (
	"sort"
)

// ByteRange is the S5 canonical half-open span [Start, End) over file
// bytes. It is the single span type for dirty-range tracking, range
// clones, and read windows: fusefs, storage, and chunking layers adopt
// this type instead of minting parallel Start/End structs. RangeEdit (an
// edit operation carrying replacement bytes) is the sibling: ByteRange
// names a span, RangeEdit mutates one.
type ByteRange struct {
	Start int64
	End   int64
}

// Len reports the span length.
func (r ByteRange) Len() int64 { return r.End - r.Start }

// ValidateByteRange rejects inverted or negative spans.
func ValidateByteRange(r ByteRange) error {
	if r.Start < 0 || r.End < 0 || r.End < r.Start {
		return InvalidArgument("byte range must satisfy 0 <= start <= end")
	}
	return nil
}

// MergeByteRanges sorts spans by start and coalesces overlapping or
// adjacent ones into the single disjoint cover. It is the one merge:
// every dirty-range accumulator funnels through here.
func MergeByteRanges(ranges []ByteRange) []ByteRange {
	if len(ranges) == 0 {
		return nil
	}
	sorted := append([]ByteRange(nil), ranges...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Start != sorted[j].Start {
			return sorted[i].Start < sorted[j].Start
		}
		return sorted[i].End < sorted[j].End
	})
	merged := sorted[:1]
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		if r.Start <= last.End {
			if r.End > last.End {
				last.End = r.End
			}
			continue
		}
		merged = append(merged, r)
	}
	return merged
}
