package cli

import (
	"fmt"

	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

func (a *App) newMountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount [flags] <project> <mount-point>",
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

func (a *App) runMount(cmd *cobra.Command, args []string) error {
	token, apiBase := cmdAuth(cmd)
	allowOther, _ := cmd.Flags().GetBool("allow-other")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cache-dir")
	umaskRaw, _ := cmd.Flags().GetString("umask")
	umask, err := parseMountUmask(umaskRaw)
	if err != nil {
		return err
	}
	// mount is a long-running interactive surface: it must get the
	// pause-to-reset rate policy, not the one-shot fail-fast default.
	hub, err := a.newCmdMountHub(resolveToken(token), apiBase)
	if err != nil {
		return err
	}
	opts := storhub.DefaultFUSEOptions()
	opts.AllowOther = allowOther
	opts.Debug = debug
	opts.CacheDir = cacheDir
	opts.Umask = umask
	opts.UmaskSet = true
	// Arm signal handling before touching FUSE: an interrupt arriving during
	// mount setup must not fall through to the default disposition and kill
	// the process with a half-attached mount left behind.
	ctx, stop := withSignalContext()
	defer stop()
	fsys, err := a.setupServeMount(ctx, args[0], args[1], opts, hub.NewFUSE)
	if err != nil {
		return err
	}
	defer func() {
		if err := fsys.Close(); err != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: closing filesystem session: %v\n", err)
		}
	}()
	_, _ = fmt.Fprintf(a.stderr, "mounted %s at %s\n", args[0], args[1])
	_, _ = fmt.Fprintln(a.stderr, "press Ctrl+C to unmount")
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
