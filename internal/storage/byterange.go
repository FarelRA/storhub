package storage

type byteRange struct {
	start int64
	end   int64
}

func mergeByteRange(existing []byteRange, next byteRange) []byteRange {
	if next.end <= next.start {
		return existing
	}
	merged := make([]byteRange, 0, len(existing)+1)
	inserted := false
	for _, current := range existing {
		if current.end < next.start {
			merged = append(merged, current)
			continue
		}
		if next.end < current.start {
			if !inserted {
				merged = append(merged, next)
				inserted = true
			}
			merged = append(merged, current)
			continue
		}
		if current.start < next.start {
			next.start = current.start
		}
		if current.end > next.end {
			next.end = current.end
		}
	}
	if !inserted {
		merged = append(merged, next)
	}
	return merged
}
