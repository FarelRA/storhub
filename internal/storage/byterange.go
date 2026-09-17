package storage

type byteRange struct {
	start int64
	end   int64
}

// mergeByteRange inserts next into the sorted, disjoint existing runs,
// merging overlaps. Both inputs are treated as values: the function never
// mutates its arguments' backing state, it returns a new slice.
//
// Order example: existing [0,10) [20,30), next [8,22) → [0,30).
// Empty or inverted next ([5,5), [9,3)) is dropped.
func mergeByteRange(existing []byteRange, next byteRange) []byteRange {
	cur := next
	if cur.end <= cur.start {
		return existing
	}
	merged := make([]byteRange, 0, len(existing)+1)
	inserted := false
	for _, current := range existing {
		if current.end < cur.start {
			merged = append(merged, current)
			continue
		}
		if cur.end < current.start {
			if !inserted {
				merged = append(merged, cur)
				inserted = true
			}
			merged = append(merged, current)
			continue
		}
		if current.start < cur.start {
			cur.start = current.start
		}
		if current.end > cur.end {
			cur.end = current.end
		}
	}
	if !inserted {
		merged = append(merged, cur)
	}
	return merged
}
