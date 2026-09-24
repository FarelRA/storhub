package cli

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

// newProjectCmd groups every project-level operation: health, history,
// reclamation, and lifecycle. Per-path file verbs stay top-level; the
// project group owns the project itself.
func (a *App) newProjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "project",
		Short: "Operate on a project: status, sync, revisions, prune, and lifecycle",
		Long: `Project groups the operations that act on a whole project
rather than a path inside it: status (health), sync (drain), revisions
(history), rollback, prune (reclaim, including chunk orphans), enable
(clear the degraded latch), and delete (destroy the project).`,
		Args: usageArgs(cobra.NoArgs),
	}
	cmd.AddCommand(a.newProjectStatusCmd())
	cmd.AddCommand(a.newProjectSyncCmd())
	cmd.AddCommand(a.newProjectRevisionsCmd())
	cmd.AddCommand(a.newProjectRollbackCmd())
	cmd.AddCommand(a.newProjectPruneCmd())
	cmd.AddCommand(a.newProjectEnableCmd())
	cmd.AddCommand(a.newProjectDeleteCmd())
	return cmd
}

func (a *App) newProjectRevisionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revisions <project>",
		Short: "List metadata revision history",
		Long: `Revisions lists the project's metadata commits, newest last.
An empty history prints nothing, like ls(1).

Examples:
  storhub project revisions docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runProjectRevisions,
	}
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON (array of revisions)")
	return cmd
}

func (a *App) newProjectRollbackCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollback <project> <commitsha>",
		Short: "Rollback metadata to a commit",
		Long: `Rollback restores the project's metadata to a past commit SHA
(7-64 lowercase hex). A malformed SHA is a usage error (exit 2).

The serve-mode admin boundary covers the REST surface only: this
command runs with local-process trust and performs no admin check
(the REST rollback endpoint is admin-gated).

Examples:
  storhub project rollback docs-project abc1234`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runProjectRollback,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) newProjectPruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune [flags] <project> [objects|assets|history|chunks|all]",
		Short: "Reclaim objects, assets, history, or chunk orphans",
		Long: `Prune reclaims storage under the full-history retention policy.

  objects   delete content-addressed index objects referenced by no retained
            manifest (orphans from failed commits or after a history prune)
  assets    delete release assets tracked by no file (interrupted writes,
            manual interference)
  history   collapse old index manifests into a checkpoint (git backend only;
            on the REST backend GitHub owns history and the API cannot delete
            revisions, so this reports honestly instead of pretending)
  chunks    collect orphaned chunk records from the live catalog (records
            no file and no pending edit references; refuses while any
            session holds the project). Works on live state: the only
            scope that needs no flush first.
  all       history (where possible) + objects + assets (the default)

Use --dry-run (--dryrun) to see what would be reclaimed without deleting anything.
--keep bounds history compaction (manifests newer than keep are retained).

The serve-mode admin boundary covers the REST surface only: this
command runs with local-process trust and performs no admin check
(the REST prune endpoint is admin-gated).`,
		Args: usageArgs(cobra.RangeArgs(1, 2)),
		RunE: a.runProjectPrune,
	}
	cmd.Flags().Bool("dryrun", false, "Report what would be reclaimed without deleting (dry_run)")
	cmd.Flags().Bool("dry-run", false, "Alias of --dryrun (dry_run)")
	cmd.Flags().Int("keep", 1, "History: number of recent manifests to retain")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) newProjectStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <project>",
		Short: "Show project health: degraded latch, streaks, pressure",
		Long: `Status reports operability in one place: whether the project
is latched degraded (and its consecutive-failure streak), the pending
op depth, and the hub pressure totals. A degraded project refuses new
mutations until enable clears the latch.

Examples:
  storhub project status docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runProjectStatus,
	}
	return cmd
}

func (a *App) runProjectStatus(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	degraded := hub.DegradedProjects()
	streak := hub.PressureFailureStreak(args[0])
	depth := hub.PressurePendingDepth(args[0])
	snap := hub.PressureSnapshot()
	latched := false
	for _, name := range degraded {
		if name == args[0] {
			latched = true
			break
		}
	}
	state := "healthy"
	if latched {
		state = fmt.Sprintf("degraded (streak %d; re-enable with: storhub project enable %s)", streak, args[0])
	}
	_, _ = fmt.Fprintf(a.stderr, "%s: %s, pending ops %d, commits %d ok / %d failed / %d rebased, cap-crosses %d\n",
		args[0], state, depth, snap.CommitSuccesses, snap.CommitFailures, snap.Rebases, snap.CapCrosses)
	return nil
}

func (a *App) newProjectSyncCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sync <project>",
		Short: "Drain a project until published state is durable",
		Long: `Sync drains the project's journal until everything published
before the call is durably committed (the storage fsync primitive). It is
the standalone form of the --sync flag every mutating command accepts.

Examples:
  storhub sync docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runProjectSync,
	}
}

func (a *App) runProjectSync(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DrainProjectContext(cmd.Context(), args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "synced %s\n", args[0])
	return nil
}

func (a *App) newProjectEnableCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "enable <project>",
		Short: "Clear a project's degraded latch",
		Long: `Enable clears the degraded latch and admits mutations
again. It is the ONLY path back to healthy: commit successes never
clear the latch, so a sick backend cannot silently recover. Enabling
a healthy project succeeds as a no-op.

Examples:
  storhub project enable docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runProjectEnable,
	}
	return cmd
}

func (a *App) runProjectEnable(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.ReEnableProject(args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "enabled %s\n", args[0])
	return nil
}

func (a *App) newProjectDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <project>",
		Short: "Delete an entire project repository",
		Long: `Delete-project removes the project's GitHub repository outright: every file,
directory, release, asset, and metadata revision is gone. This cannot be undone.

The serve-mode admin boundary covers the REST surface only: this
command runs with local-process trust and performs no admin check
(the REST project-delete endpoint is admin-gated).

The --yes flag is mandatory so a typo can never destroy a project.`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runDeleteProject,
	}
	cmd.Flags().Bool("yes", false, "Confirm deletion of the whole project")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runDeleteProject(cmd *cobra.Command, args []string) error {
	confirmed, _ := cmd.Flags().GetBool("yes")
	if !confirmed {
		return &usageError{fmt.Errorf("deleting project %q removes its repository, releases, and every file; pass --yes to confirm", args[0])}
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DeleteProject(args[0]); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "deleted project %s\n", args[0])
	return nil
}

func (a *App) runProjectRevisions(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	revs, err := hub.ListMetadataRevisionsContext(cmd.Context(), args[0])
	if err != nil {
		return err
	}
	if v, _ := cmd.Flags().GetBool("json"); v {
		if revs == nil {
			revs = []storhub.MetadataRevision{}
		}
		return json.NewEncoder(a.stdout).Encode(revs)
	}
	printRevisions(a.stdout, revs)
	return nil
}

// commitSHAPattern matches git object ids: lowercase hex, 7 (shortest
// unambiguous abbreviation) through 64 characters : the same shape REST
// requireCommitSHA enforces, so a CLI typo is a usage error (exit 2),
// not a runtime failure (exit 1) deep inside storage.
var commitSHAPattern = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

func (a *App) runProjectRollback(cmd *cobra.Command, args []string) error {
	if !commitSHAPattern.MatchString(strings.TrimSpace(args[1])) {
		return &usageError{fmt.Errorf("invalid commit SHA %q: must be 7-64 lowercase hex characters", args[1])}
	}
	hub, ctx, stop, err := a.mustCmdHubCtx(cmd, 0, false)
	if err != nil {
		return err
	}
	defer stop()
	if err := hub.RollbackMetadataContext(ctx, args[0], args[1]); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(ctx, cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "rolled back %s to %s\n", args[0], args[1])
	return nil
}

func (a *App) runProjectPrune(cmd *cobra.Command, args []string) error {
	scope := "all"
	if len(args) >= 2 {
		scope = args[1]
	}
	switch storhub.PruneScope(scope) {
	case storhub.PruneObjects, storhub.PruneAssets, storhub.PruneHistory, storhub.PruneChunks, storhub.PruneAll:
	default:
		return &usageError{fmt.Errorf("invalid prune scope %q (known: objects, assets, history, chunks, all)", scope)}
	}
	dryRun, _ := cmd.Flags().GetBool("dryrun")
	if dashDryRun, _ := cmd.Flags().GetBool("dry-run"); dashDryRun {
		dryRun = true
	}
	keep, _ := cmd.Flags().GetInt("keep")
	if keep < 1 {
		return &usageError{fmt.Errorf("--keep must retain at least 1 manifest, got %d", keep)}
	}
	// Pruning can run for minutes; a Ctrl+C must cancel it between delete
	// units instead of killing the process mid-loop.
	hub, ctx, stop, err := a.mustCmdHubCtx(cmd, 0, false)
	if err != nil {
		return err
	}
	defer stop()
	result, err := hub.PruneContext(ctx, args[0], scope, keep, dryRun)
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(ctx, cmd, args[0]); err != nil {
		return err
	}
	verb := "pruned"
	if dryRun {
		verb = "would prune"
	}
	if result.Scope == storhub.PruneChunks {
		_, _ = fmt.Fprintf(a.stderr, "%s %s (chunks): %d orphan chunks, %d bytes of %d scanned\n",
			verb, args[0], result.OrphanChunks, result.OrphanBytes, result.ScannedChunks)
		return nil
	}
	_, _ = fmt.Fprintf(a.stderr, "%s %s (%s): %d objects, %d releases, %d assets",
		verb, args[0], result.Scope, result.DeletedObjects, result.DeletedReleases, result.DeletedAssets)
	if result.HistoryCompacted {
		_, _ = fmt.Fprint(a.stderr, ", history compacted")
	}
	_, _ = fmt.Fprintln(a.stderr)
	for _, note := range result.Notes {
		_, _ = fmt.Fprintf(a.stderr, "  note: %s\n", note)
	}
	return nil
}
