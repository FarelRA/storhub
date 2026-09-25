package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/spf13/cobra"
)

// cli_sessions.go: stateful open sessions (server-held handles) over the CLI.
//
// One parent, `session`, with one subcommand per manager verb. Every
// subcommand except `open` threads the handle through the persistent
// --handle flag, falling back to $STORHUB_HANDLE when the flag is absent
// (export it once per script instead of repeating the flag); `open`
// prints the new handle id to stdout (nothing else,
// so scripts can capture it) and every read-like output goes to stdout
// while status chatter stays on stderr, like cat(1).
//
//	storhub session open <project> [path] [--mode r] [--ttl 5m]
//	storhub session read --handle H [--offset 0] [--length N]
//	storhub session write --handle H <offset> <text>
//	storhub session append --handle H <text>
//	storhub session truncate --handle H <size>
//	storhub session stat --handle H [--json]
//	storhub session sync --handle H
//	storhub session link --handle H <path>
//	storhub session close --handle H [--sync]
//
// Identity: commands pass cmd.Context() unchanged into the hub. One-shot
// CLI runs carry no authenticated identity, so the manager sees the trusted
// local process; no identity is synthesized here. --sync on close drains the
// handle's project after commit, mirroring the one-shot mutation commands.

func (a *App) newSessionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "session",
		Short: "Manage stateful file sessions (handles)",
		Long: `Session opens a stateful file handle on the server and stages
writes there until sync or close commits them atomically.

Examples:
  storhub session open docs-project docs/readme.txt --mode r
  storhub session read --handle <id> --offset 0 --length 64
  storhub session close --handle <id> --sync`,
		Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.PersistentFlags().String("handle", "", "Session handle id (printed by session open)")
	cmd.AddCommand(a.newSessionOpenCmd())
	cmd.AddCommand(a.newSessionReadCmd())
	cmd.AddCommand(a.newSessionWriteCmd())
	cmd.AddCommand(a.newSessionAppendCmd())
	cmd.AddCommand(a.newSessionTruncateCmd())
	cmd.AddCommand(a.newSessionStatCmd())
	cmd.AddCommand(a.newSessionSyncCmd())
	cmd.AddCommand(a.newSessionLinkCmd())
	cmd.AddCommand(a.newSessionRelinkCmd())
	cmd.AddCommand(a.newSessionCloseCmd())
	return cmd
}

// drainSessionIfSyncRequested honors --sync on session mutators: it resolves
// the handle's project (one stat) and drains it, mirroring the one-shot
// mutation commands and the REST ?sync=1 spelling.
func (a *App) drainSessionIfSyncRequested(cmd *cobra.Command, hub hubClient, handle string) error {
	want, _ := cmd.Flags().GetBool("sync")
	if !want {
		return nil
	}
	stat, err := hub.StatSession(cmd.Context(), handle)
	if err != nil {
		return err
	}
	return a.drainIfSyncRequested(cmd.Context(), cmd, stat.Project)
}

func sessionHandle(cmd *cobra.Command) (string, error) {
	handle, _ := cmd.Flags().GetString("handle")
	// Script composability: export STORHUB_HANDLE once instead
	// of repeating --handle on every call. Flag wins over env.
	handle = flagOrEnv(handle, storcfg.EnvSessionHandle)
	if handle == "" {
		return "", &usageError{fmt.Errorf("missing --handle (or $STORHUB_HANDLE): open a session first with session open")}
	}
	return handle, nil
}

// parseSessionTTL parses a session --ttl value: a Go duration string like
// "5m". The message matches the REST open path (rest_sessions.go) so one
// spelling of the TTL contract reaches operators on both surfaces.
func parseSessionTTL(raw string) (time.Duration, error) {
	ttl, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, &usageError{fmt.Errorf("invalid --ttl %q: ttl must be a Go duration string like \"5m\"", raw)}
	}
	return ttl, nil
}

func (a *App) newSessionOpenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "open [flags] <project> [path]",
		Short: "Open a session and print its handle id",
		Long: `Open pins the file snapshot and prints the handle id to stdout.
An empty path opens an unlinked session that must be named with session link
before it can commit. Mode is a fopen-style string (r, r+, w, w+, a, a+,
with optional x for exclusive).`,
		Args: usageArgs(cobra.RangeArgs(1, 2)),
		RunE: a.runSessionOpen,
	}
	cmd.Flags().String("mode", "r", "Open mode: r, r+, w, w+, a, a+ with optional x")
	cmd.Flags().String("ttl", "", "Idle TTL as a Go duration (default 10m, capped at 1h)")
	return cmd
}

func (a *App) runSessionOpen(cmd *cobra.Command, args []string) error {
	modeRaw, _ := cmd.Flags().GetString("mode")
	mode, err := storage.ParseOpenMode(strings.TrimSpace(modeRaw))
	if err != nil {
		return &usageError{err}
	}
	var opts []storage.SessionOption
	if ttlRaw, _ := cmd.Flags().GetString("ttl"); strings.TrimSpace(ttlRaw) != "" {
		ttl, err := parseSessionTTL(ttlRaw)
		if err != nil {
			return err
		}
		opts = append(opts, storage.WithSessionTTL(ttl))
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	path := ""
	if len(args) == 2 {
		path = args[1]
	}
	id, err := hub.OpenSession(cmd.Context(), args[0], path, mode, opts...)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(a.stdout, id)
	return nil
}

func (a *App) newSessionReadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read [flags]",
		Short: "Read session bytes to stdout",
		Long: `Read streams [offset, offset+length) from the pinned snapshot
plus the handle's own staged writes. Length defaults to EOF.`,
		Args: usageArgs(cobra.NoArgs),
		RunE: a.runSessionRead,
	}
	cmd.Flags().Int64("offset", 0, "Byte offset to read from")
	cmd.Flags().Int64("length", -1, "Bytes to read (-1 means to EOF)")
	return cmd
}

func (a *App) runSessionRead(cmd *cobra.Command, _ []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	offset, _ := cmd.Flags().GetInt64("offset")
	length, _ := cmd.Flags().GetInt64("length")
	if offset < 0 {
		return &usageError{fmt.Errorf("--offset must be >= 0, got %d", offset)}
	}
	if length < -1 {
		return &usageError{fmt.Errorf("--length must be >= 0 or -1 for EOF, got %d", length)}
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	// Two-call shape (stat for EOF, then read): a concurrent writer can
	// change the size between the calls, so EOF reads are best-effort.
	if length == -1 {
		stat, err := hub.StatSession(cmd.Context(), handle)
		if err != nil {
			return err
		}
		length = stat.Size - offset
		if length < 0 {
			length = 0
		}
	}
	data, err := hub.ReadSession(cmd.Context(), handle, offset, length)
	if err != nil {
		return err
	}
	_, _ = a.stdout.Write(data)
	return nil
}

func (a *App) newSessionWriteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "write [flags] <offset> <text>",
		Short: "Stage bytes at an offset",
		Long: `Write stages bytes (or stdin with "-") at an offset, visible
only to this handle until sync or close. In append mode the offset is
ignored and bytes stage at the end.`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runSessionWrite,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionWrite(cmd *cobra.Command, args []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	offset, err := parseNonNegativeArg(args[0], "offset")
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	payload, err := readDataArg(a, args[1])
	if err != nil {
		return err
	}
	wrote, err := hub.WriteSession(cmd.Context(), handle, offset, payload)
	if err != nil {
		return err
	}
	if err := a.drainSessionIfSyncRequested(cmd, hub, handle); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "written %d bytes to %s\n", wrote, handle)
	return nil
}

func (a *App) newSessionAppendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "append [flags] <text>",
		Short: "Stage bytes at the end",
		Long: `Append stages bytes (or stdin with "-") at the current end,
visible only to this handle until sync or close.

Single-appender scope: the end offset is read then written in two
steps (the server forces end-offset writes only for sessions opened
in append mode, and this command accepts any mode), so two handles
appending concurrently can stage at the same offset and one commit
wins. Concurrent appenders must coordinate outside the session (e.g.
one writer, or separate handles merged later).`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runSessionAppend,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionAppend(cmd *cobra.Command, args []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	payload, err := readDataArg(a, args[0])
	if err != nil {
		return err
	}
	// Read-then-write: the server forces end-offset writes only for
	// sessions opened in append mode, while this command must work on a
	// handle opened in any mode, so it stats the staged end first. The
	// single-appender scope is documented on newSessionAppendCmd;
	// concurrent appenders race on the stat size and fail loud at
	// commit instead of interleaving silently.
	stat, err := hub.StatSession(cmd.Context(), handle)
	if err != nil {
		return err
	}
	wrote, err := hub.WriteSession(cmd.Context(), handle, stat.Size, payload)
	if err != nil {
		return err
	}
	if err := a.drainSessionIfSyncRequested(cmd, hub, handle); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "appended %d bytes to %s\n", wrote, handle)
	return nil
}

func (a *App) newSessionTruncateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "truncate [flags] <size>",
		Short: "Stage a resize",
		Long: `Truncate stages a resize to size bytes, visible only to this
handle until sync or close.`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runSessionTruncate,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionTruncate(cmd *cobra.Command, args []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	size, err := parseNonNegativeArg(args[0], "size")
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.TruncateSession(cmd.Context(), handle, size); err != nil {
		return err
	}
	if err := a.drainSessionIfSyncRequested(cmd, hub, handle); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "truncated %s to %d bytes\n", handle, size)
	return nil
}

type sessionStatDoc struct {
	Handle  string `json:"handle"`
	Project string `json:"project"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	Dirty   bool   `json:"dirty"`
	Stale   bool   `json:"stale"`
	Mode    string `json:"mode"`
}

func (a *App) newSessionStatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stat [flags]",
		Short: "Show session metadata",
		Long: `Stat prints a handle's project, path, size, dirty state, and
mode for humans, or one JSON object with --json.`,
		Args: usageArgs(cobra.NoArgs),
		RunE: a.runSessionStat,
	}
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON object")
	return cmd
}

func (a *App) runSessionStat(cmd *cobra.Command, _ []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	stat, err := hub.StatSession(cmd.Context(), handle)
	if err != nil {
		return err
	}
	doc := sessionStatDoc{
		Handle:  handle,
		Project: stat.Project,
		Path:    stat.Path,
		Size:    stat.Size,
		Dirty:   stat.Dirty,
		Stale:   stat.Stale,
		Mode:    stat.Mode.String(),
	}
	if jsonOut, _ := cmd.Flags().GetBool("json"); jsonOut {
		return json.NewEncoder(a.stdout).Encode(doc)
	}
	_, _ = fmt.Fprintf(a.stdout, "handle: %s\nproject: %s\npath: %s\nsize: %d bytes\ndirty: %t\nstale: %t\nmode: %s\n",
		doc.Handle, doc.Project, doc.Path, doc.Size, doc.Dirty, doc.Stale, doc.Mode)
	return nil
}

func (a *App) newSessionSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync [flags]",
		Short: "Commit staged state without closing",
		Long: `Sync commits the handle's staged writes atomically and re-pins
to the committed state, keeping the handle open.`,
		Args: usageArgs(cobra.NoArgs),
		RunE: a.runSessionSync,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionSync(cmd *cobra.Command, _ []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.SyncSession(cmd.Context(), handle); err != nil {
		return err
	}
	if err := a.drainSessionIfSyncRequested(cmd, hub, handle); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "synced %s\n", handle)
	return nil
}

func (a *App) newSessionLinkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "link [flags] <path>",
		Short: "Name an unlinked session handle",
		Long: `Link names an unlinked session handle (opened without a path),
staging the creation so a later close persists it.`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runSessionLink,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionLink(cmd *cobra.Command, args []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.LinkSession(cmd.Context(), handle, args[0]); err != nil {
		return err
	}
	if err := a.drainSessionIfSyncRequested(cmd, hub, handle); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "linked %s to %s\n", handle, args[0])
	return nil
}

func (a *App) newSessionRelinkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "relink [flags] <path>",
		Short: "Retarget a handle whose linked path was taken",
		Long: `Relink retargets a handle to a new path: the rescue for a
close that failed because a concurrent writer took the linked target.
The staged bytes commit at the new path on close.`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runSessionRelink,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionRelink(cmd *cobra.Command, args []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.RelinkSession(cmd.Context(), handle, args[0]); err != nil {
		return err
	}
	if err := a.drainSessionIfSyncRequested(cmd, hub, handle); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "relinked %s to %s\n", handle, args[0])
	return nil
}

func (a *App) newSessionCloseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "close [flags]",
		Short: "Commit staged state and destroy the handle",
		Long: `Close commits staged state atomically and destroys the handle.
Closing an unlinked session without a prior link discards it.`,
		Args: usageArgs(cobra.NoArgs),
		RunE: a.runSessionClose,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSessionClose(cmd *cobra.Command, _ []string) error {
	handle, err := sessionHandle(cmd)
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	var project string
	if want, _ := cmd.Flags().GetBool("sync"); want {
		stat, err := hub.StatSession(cmd.Context(), handle)
		if err != nil {
			return err
		}
		project = stat.Project
	}
	if err := hub.CloseSession(cmd.Context(), handle); err != nil {
		return err
	}
	if project != "" {
		if err := a.drainIfSyncRequested(cmd.Context(), cmd, project); err != nil {
			return err
		}
	}
	_, _ = fmt.Fprintf(a.stderr, "closed %s\n", handle)
	return nil
}
