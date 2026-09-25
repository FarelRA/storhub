package cli

import (
	"fmt"

	shlog "github.com/FarelRA/storhub/internal/logging"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

func (a *App) newMountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount [flags] <project> <mountpoint>",
		Short: "FUSE mount a project",
		Long: `Mount exposes the project as a local filesystem over FUSE.
Press Ctrl+C to unmount; the unmount is retried while files stay open.

Examples:
  storhub mount docs-project ./mnt`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runMount,
	}
	addFUSEFlags(cmd)
	return cmd
}

// fuseOptsFromFlags is the single FUSE-options constructor for mount and
// serve: one definition so the two surfaces cannot drift on wording,
// defaults, or umask semantics. The FUSE protocol never transmits the
// caller umask, so an unset --umask stays representable: UmaskSet is true
// only when the flag was explicitly passed, and the default 022 applies
// otherwise without masquerading as an explicit choice.
func fuseOptsFromFlags(cmd *cobra.Command) (storhub.FUSEOptions, error) {
	allowOther, _ := cmd.Flags().GetBool("allowother")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cachedir")
	umaskRaw, _ := cmd.Flags().GetString("umask")
	umask, err := parseMountUmask(umaskRaw)
	if err != nil {
		return storhub.FUSEOptions{}, err
	}
	opts := storhub.DefaultFUSEOptions()
	opts.AllowOther = allowOther
	opts.Debug = debug
	opts.CacheDir = cacheDir
	opts.Umask = umask
	opts.UmaskSet = cmd.Flags().Changed("umask")
	return opts, nil
}

func (a *App) runMount(cmd *cobra.Command, args []string) error {
	token, apiBase := cmdAuth(cmd)
	opts, err := fuseOptsFromFlags(cmd)
	if err != nil {
		return err
	}
	// mount is a long-running interactive surface: it must get the
	// pause-to-reset rate policy, not the one-shot fail-fast default.
	shlog.Debug(a.logger(), "mount start", "command", "mount", "project", args[0], "mountpoint", args[1])
	hub, err := a.newCmdMountHub(cmd.Context(), resolveToken(token), apiBase)
	if err != nil {
		shlog.Error(a.logger(), "mount failed", "command", "mount", "project", args[0], "mountpoint", args[1], "err", err)
		return err
	}
	// Arm signal handling before touching FUSE: an interrupt arriving during
	// mount setup must not fall through to the default disposition and kill
	// the process with a half-attached mount left behind.
	ctx, stop := withSignalContext()
	defer stop()
	fsys, err := a.setupServeMount(ctx, args[0], args[1], opts, hub.NewFUSE)
	if err != nil {
		shlog.Error(a.logger(), "mount failed", "command", "mount", "project", args[0], "mountpoint", args[1], "err", err)
		return err
	}
	defer func() {
		if err := fsys.Close(); err != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: closing filesystem session: %v\n", err)
		}
	}()
	_, _ = fmt.Fprintf(a.stderr, "mounted %s at %s\n", args[0], args[1])
	_, _ = fmt.Fprintln(a.stderr, "press Ctrl+C to unmount")
	shlog.Info(a.logger(), "mount listening", "command", "mount", "project", args[0], "mountpoint", args[1])
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		fsys.Wait()
	}()
	select {
	case <-waitDone:
		// The server stopped on its own (unmounted externally).
		return nil
	case <-ctx.Done():
		// Restore the default signal disposition first: pressing Ctrl+C
		// again force-quits instead of queueing more polite unmounts.
		stop()
		unmountWithRetry(fsys, args[1], a.stderr)
		// fsys.Wait() only returns after a SUCCESSFUL unmount. When the
		// retry budget gave up, an unbounded join here would hang the CLI
		// forever instead of exiting; bound it and report failure loudly.
		if !joinWithin(waitDone, unmountJoinTimeout) {
			_, _ = fmt.Fprintf(a.stderr, "mount session did not end within %s after unmount; %s\n", unmountJoinTimeout, mayStillBeMounted(args[1]))
			return fmt.Errorf("unmount of %s did not complete; giving up on the mount session", args[1])
		}
		return nil
	}
}
