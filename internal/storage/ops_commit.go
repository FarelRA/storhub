package storage

import (
	"fmt"
	"math"
	"strings"
)

func chunksEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// fileEqual reports whether two file entries carry the same identity and
// content. With ignoreChangedAt, ChangedAt is zeroed first: renames and
// child mutations bump it without changing what the entry IS.
func fileEqual(a, b FileMeta, ignoreChangedAt bool) bool {
	if ignoreChangedAt {
		a.ChangedAt, b.ChangedAt = 0, 0
	}
	return fileBodiesEqual(a, b)
}

func fileBodiesEqual(a, b FileMeta) bool {
	if !chunksEqual(a.Chunks, b.Chunks) ||
		a.Size != b.Size || a.Symlink != b.Symlink ||
		a.UploadedAt != b.UploadedAt || a.ModifiedAt != b.ModifiedAt ||
		a.AccessedAt != b.AccessedAt || a.Mode != b.Mode ||
		a.UID != b.UID || a.GID != b.GID || a.Inode != b.Inode ||
		len(a.XAttrs) != len(b.XAttrs) {
		return false
	}
	for k, v := range a.XAttrs {
		bv, ok := b.XAttrs[k]
		if !ok || string(v) != string(bv) {
			return false
		}
	}
	return true
}

// dirEqual reports whether two directory entries carry the same identity.
// With ignoreTimes, ModifiedAt/ChangedAt are zeroed first: renames and child
// mutations touch them without changing the directory's own identity.
func dirEqual(a, b DirMeta, ignoreTimes bool) bool {
	if ignoreTimes {
		a.ModifiedAt, b.ModifiedAt = 0, 0
		a.ChangedAt, b.ChangedAt = 0, 0
	}
	return dirBodiesEqual(a, b)
}

func dirBodiesEqual(a, b DirMeta) bool {
	if a.CreatedAt != b.CreatedAt || a.AccessedAt != b.AccessedAt ||
		a.Mode != b.Mode || a.UID != b.UID || a.GID != b.GID || a.Inode != b.Inode ||
		len(a.XAttrs) != len(b.XAttrs) {
		return false
	}
	for k, v := range a.XAttrs {
		bv, ok := b.XAttrs[k]
		if !ok || string(v) != string(bv) {
			return false
		}
	}
	return true
}

// chunkRecordsFor collects the catalog records a file's chunk IDs reference,
// making a put op self-contained (replay never depends on the records
// already existing upstream).
func chunkRecordsFor(meta *RepoMetadata, ids []int64) map[int64]ChunkInfo {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[int64]ChunkInfo, len(ids))
	for _, id := range ids {
		if info, ok := meta.Chunks()[id]; ok {
			out[id] = info
		}
	}
	return out
}

// opSummaryCounts renders the per-class op counts for a commit summary
// line, fixed order, non-zero classes only: "2 put, 1 del, 1 mkdir".
func opSummaryCounts(ops []Op) string {
	order := []struct {
		label string
		match func(OpType) bool
	}{
		{"put", func(t OpType) bool {
			return t == OpPutFile || t == OpTruncate || t == OpPatch || t == OpSetattr || t == OpXattr
		}},
		{"del", isDeleteClass},
		{"mkdir", func(t OpType) bool { return t == OpMkdir }},
		{"rename", func(t OpType) bool { return t == OpRename }},
		{"release", func(t OpType) bool { return t == OpRelease }},
		{"chunkprune", func(t OpType) bool { return t == OpChunkPrune }},
	}
	counts := make(map[string]int, len(order))
	for _, op := range ops {
		for _, c := range order {
			if c.match(op.Type) {
				counts[c.label]++
				break
			}
		}
	}
	parts := make([]string, 0, len(order))
	for _, c := range order {
		if counts[c.label] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[c.label], c.label))
		}
	}
	return strings.Join(parts, ", ")
}

// buildCommitMessage renders the commit message for a batch of ops: a
// summary first line plus one body line per op (capped), giving the
// metadata history the "what happened to this file" answer the generic
// message never could.
func buildCommitMessage(ops []Op, previousSHA string) string {
	if len(ops) == 0 {
		return "storhub: update metadata"
	}
	var sb strings.Builder
	counts := opSummaryCounts(ops)
	if previousSHA != "" {
		fmt.Fprintf(&sb, "storhub: %d ops (%s) on top of %s", len(ops), counts, shortSHA(previousSHA))
	} else {
		fmt.Fprintf(&sb, "storhub: %d ops (%s)", len(ops), counts)
	}
	const maxBodyLines = 100
	for i, op := range ops {
		if i == maxBodyLines {
			fmt.Fprintf(&sb, "\n+ %d more", len(ops)-i)
			break
		}
		sb.WriteString("\n")
		sb.WriteString(opMessageLine(op))
	}
	return sb.String()
}

func opMessageLine(op Op) string {
	var sb strings.Builder
	mode := func() string {
		if op.File != nil {
			return fmt.Sprintf("%04o", op.File.Mode)
		}
		if op.Dir != nil {
			return fmt.Sprintf("%04o", op.Dir.Mode)
		}
		return ""
	}
	switch op.Type {
	case OpPutFile:
		sb.WriteString("put ")
		sb.WriteString(opPath(op))
		if op.File != nil {
			if op.File.Symlink != "" {
				fmt.Fprintf(&sb, " symlink -> %s", op.File.Symlink)
			} else {
				release := ""
				for _, id := range op.File.Chunks {
					if info, ok := op.Chunks[id]; ok && info.Release != "" {
						release = info.Release
						break
					}
				}
				if release != "" {
					fmt.Fprintf(&sb, " %s %d chunks (%s) mode %s", humanizeBytes(op.File.Size), len(op.File.Chunks), release, mode())
				} else {
					fmt.Fprintf(&sb, " %s mode %s", humanizeBytes(op.File.Size), mode())
				}
			}
		}
	case OpTruncate:
		size := int64(0)
		if op.File != nil {
			size = op.File.Size
		}
		fmt.Fprintf(&sb, "trunc %s to %s", opPath(op), humanizeBytes(size))
	case OpPatch:
		sb.WriteString("patch ")
		sb.WriteString(opPath(op))
	case OpSetattr:
		fmt.Fprintf(&sb, "setattr %s", opPath(op))
		if m := mode(); m != "" {
			fmt.Fprintf(&sb, " mode %s", m)
		}
	case OpXattr:
		fmt.Fprintf(&sb, "xattr %s %s", opPath(op), op.XAttr)
	case OpDeleteFile:
		fmt.Fprintf(&sb, "del %s", opPath(op))
		if op.FreedChunks > 0 {
			fmt.Fprintf(&sb, " freed %d chunks", op.FreedChunks)
		}
	case OpMkdir:
		fmt.Fprintf(&sb, "mkdir %s", opPath(op))
		if m := mode(); m != "" {
			fmt.Fprintf(&sb, " mode %s", m)
		}
	case OpRmdir:
		fmt.Fprintf(&sb, "rmdir %s", opPath(op))
	case OpRename:
		to := opPath(op)
		if len(op.Paths) == 2 {
			to = op.Paths[1]
		}
		fmt.Fprintf(&sb, "rename %s -> %s", opPath(op), to)
	case OpRelease:
		verb := "add"
		if op.Release == nil {
			verb = "del"
		}
		fmt.Fprintf(&sb, "release %s %s", verb, op.Tag)
	case OpChunkPrune:
		fmt.Fprintf(&sb, "chunkprune %d chunk records", len(op.RemovedChunks))
	default:
		fmt.Fprintf(&sb, "%s %s", op.Type, opPath(op))
	}
	fmt.Fprintf(&sb, " [%s]", op.Cause)
	if op.Times > 1 {
		fmt.Fprintf(&sb, " (x%d)", op.Times)
	}
	return sb.String()
}

// causeFromMessage extracts the operation word from a transaction message
// ("storhub: mkdir /path" -> "mkdir") so op causes stay short and stable.
func causeFromMessage(message string) string {
	m := strings.TrimSpace(message)
	m = strings.TrimPrefix(m, "storhub:")
	m = strings.TrimSpace(m)
	if i := strings.IndexAny(m, " \t:"); i >= 0 {
		m = m[:i]
	}
	if m == "" {
		return "update"
	}
	return m
}

// humanizeBytes renders a byte count for commit messages: 512B, 3.0KiB,
// 2.2MiB, 1.5GiB. Scales truncate (floor) rather than round so the shown
// size never overstates the stored bytes.
func humanizeBytes(n int64) string {
	if n < 0 {
		return "?" + humanizeBytes(-n)
	}
	units := []struct {
		name string
		size float64
	}{
		{"B", 1}, {"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	}
	value := float64(n)
	unit := units[0]
	for _, u := range units {
		if value >= u.size {
			unit = u
		}
	}
	if unit.size == 1 {
		return fmt.Sprintf("%dB", n)
	}
	scaled := math.Floor(value/unit.size*10) / 10
	return fmt.Sprintf("%.1f%s", scaled, unit.name)
}
