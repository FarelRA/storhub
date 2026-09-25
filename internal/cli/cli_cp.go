package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

// cli_cp.go: the cp command (server-side copy with streaming fallback).
//
// cp copies bytes between two stored paths without moving them through the
// caller when the backend can clone: one CloneRange lands destination
// records that reference the same release assets the source bytes already
// live in. The --reflink mode selects how hard the command tries:
//
// always: CloneRange only. Any clone failure (missing source, degenerate
// holes-only range the core refuses, a backend without the op) is a loud
// error; nothing is streamed, so always proves the clone path.
// auto (default): try CloneRange, fall back to a windowed
// download-plus-upload streaming copy when the clone fails. With
// --expectedrevision set there is no fallback: the streaming path cannot
// honor the compare-and-swap, so a failed clone fails the command instead
// of applying without its guard.
//
// Whole-file clone is the full-range case: no offsets means src_off 0 and
// length equal to the source size, resolved with one StatPath.

// cloneRanger is the server-side copy op behind cp --reflink=always|auto.
// Resolved by type assertion (the posix.go routing rule): storhubClient
// satisfies it through its embedded hub, and a hub without the op fails
// loudly instead of silently streaming under --reflink=always.
type cloneRanger interface {
	CloneRange(context.Context, string, string, int64, string, int64, int64, ...storhub.MutateOption) (*storhub.FileMetadata, error)
}

// copyWindowSize bounds resident memory for every windowed CLI copy loop
// (cp streaming, cat streaming): each iteration moves at most this many
// bytes instead of buffering the whole object. One const so the two loops
// can never disagree on the resident-memory budget.
const copyWindowSize = 1 << 20

func (a *App) newCpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cp [flags] <project> <oldpath> <newpath> [src_off dst_off length]",
		Short: "Copy a file server-side, with streaming fallback",
		Long: `Cp copies stored bytes between two paths, trying a zero-byte
server-side clone first (--reflink=auto, the default) and streaming the
bytes when cloning is unavailable.

  --reflink=always  clone only; fail loudly when uncloneable
  --reflink=auto    clone, falling back to streaming copy (default)

With no offsets the whole file is copied; with src_off dst_off length
exactly that span moves (pwrite semantics on the destination).

Examples:
  storhub cp docs-project docs/a.txt docs/b.txt
  storhub cp --reflink=always docs-project docs/a.txt docs/b.txt
  storhub cp docs-project docs/a.txt docs/b.txt 0 4096 4096`,
		Args: usageArgs(func(_ *cobra.Command, args []string) error {
			if len(args) != 3 && len(args) != 6 {
				return fmt.Errorf("accepts 3 or 6 arg(s), received %d", len(args))
			}
			return nil
		}),
		RunE: a.runCp,
	}
	cmd.Flags().String("reflink", "auto", "Copy mode: always (clone only), auto (clone with streaming fallback)")
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) runCp(cmd *cobra.Command, args []string) error {
	mode, _ := cmd.Flags().GetString("reflink")
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "always", "auto":
		mode = strings.ToLower(strings.TrimSpace(mode))
	default:
		return &usageError{fmt.Errorf("invalid --reflink %q: must be always or auto", mode)}
	}
	project, src, dst := args[0], args[1], args[2]
	var srcOff, dstOff, length int64
	offsetsGiven := len(args) == 6
	if offsetsGiven {
		var err error
		if srcOff, err = parseNonNegativeArg(args[3], "src_off"); err != nil {
			return err
		}
		if dstOff, err = parseNonNegativeArg(args[4], "dst_off"); err != nil {
			return err
		}
		if length, err = parseNonNegativeArg(args[5], "length"); err != nil {
			return err
		}
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	ctx := cmd.Context()
	if !offsetsGiven {
		entry, err := hub.StatPathContext(ctx, project, src)
		if err != nil {
			return err
		}
		length = entry.Size
	}
	if length == 0 {
		// Validated no-op like the core: the destination must exist,
		// and nothing moves.
		if _, err := hub.StatPathContext(ctx, project, dst); err != nil {
			return err
		}
		if err := a.drainIfSyncRequested(ctx, cmd, project); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stderr, "copied %s -> %s (0 bytes, no-op)\n", src, dst)
		return nil
	}
	opts := revisionOpts(cmd)
	switch mode {
	case "always":
		meta, err := cloneWithFlags(ctx, hub, project, src, srcOff, dst, dstOff, length, opts, mode)
		if err != nil {
			return err
		}
		if err := a.drainIfSyncRequested(ctx, cmd, project); err != nil {
			return err
		}
		printFileSummary(a.stderr, fmt.Sprintf("copied %s -> %s (reflink)", src, dst), meta)
		return nil
	default: // auto
		meta, cerr := cloneWithFlags(ctx, hub, project, src, srcOff, dst, dstOff, length, opts, mode)
		if cerr == nil {
			if err := a.drainIfSyncRequested(ctx, cmd, project); err != nil {
				return err
			}
			printFileSummary(a.stderr, fmt.Sprintf("copied %s -> %s (reflink)", src, dst), meta)
			return nil
		}
		if opts != nil {
			// The clone carried a compare-and-swap guard the streaming
			// path cannot honor: failing loud beats applying unguarded.
			return cerr
		}
		a.warnfWithAttrs(nil, "clone unavailable; falling back to streaming copy",
			[]any{"err", cerr, "fallback", "streaming copy"})
		meta, err := streamingCopy(ctx, hub, project, src, srcOff, dst, dstOff, length)
		if err != nil {
			return err
		}
		if err := a.drainIfSyncRequested(ctx, cmd, project); err != nil {
			return err
		}
		printFileSummary(a.stderr, fmt.Sprintf("copied %s -> %s (streaming)", src, dst), meta)
		return nil
	}
}

// cloneWithFlags routes cp past the CAS guard when requested, failing
// loudly when the hub does not implement the op (the posix.go routing
// rule: never silently drop the guard or the reflink requirement).
func cloneWithFlags(ctx context.Context, hub hubClient, project, src string, srcOff int64, dst string, dstOff int64, length int64, opts []storhub.MutateOption, mode string) (*storhub.FileMetadata, error) {
	h, ok := hub.(cloneRanger)
	if !ok {
		return nil, fmt.Errorf("hub does not support server-side copy (--reflink=%s needs CloneRange)", mode)
	}
	return h.CloneRange(ctx, project, src, srcOff, dst, dstOff, length, opts...)
}

// streamingCopy is the download-plus-upload fallback: the source span is
// read window by window and written over the destination span (pwrite
// semantics; WriteFileAt zero-fills a dst gap). The destination is created
// when missing so a full-file stream lands on a fresh path. A newly
// created destination takes the source permission bits minus
// setuid/setgid (the CLI runs as the trusted local process, never
// admin), matching the CloneRange mode rule (internal/storage/clone.go
// clears via SanitizeWrittenFileModeForContext) so the --reflink
// choice never changes the result mode. An existing destination keeps
// its mode (range-write semantics, like CloneRange onto an existing
// file).
func streamingCopy(ctx context.Context, hub hubClient, project, src string, srcOff int64, dst string, dstOff int64, length int64) (*storhub.FileMetadata, error) {
	if _, err := hub.StatPathContext(ctx, project, dst); err != nil {
		if !isNotFoundErr(err) {
			return nil, err
		}
		if _, err := hub.CreateFileContext(ctx, project, dst); err != nil {
			return nil, err
		}
		srcEntry, serr := hub.StatPathContext(ctx, project, src)
		if serr != nil {
			return nil, serr
		}
		if cerr := hub.ChmodContext(ctx, project, dst, srcEntry.Mode&0o7777&^0o6000); cerr != nil {
			return nil, cerr
		}
	}
	var meta *storhub.FileMetadata
	var done int64
	for done < length {
		want := int64(copyWindowSize)
		if remaining := length - done; remaining < want {
			want = remaining
		}
		chunk, err := hub.ReadFileAtContext(ctx, project, src, srcOff+done, want)
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			// The source shrank underneath us; serve what exists.
			break
		}
		meta, err = hub.WriteFileAtContext(ctx, project, dst, dstOff+done, chunk)
		if err != nil {
			return nil, err
		}
		done += int64(len(chunk))
	}
	return meta, nil
}
