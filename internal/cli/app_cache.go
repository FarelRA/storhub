package cli

import (
	"fmt"

	shlog "github.com/FarelRA/storhub/internal/logging"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/spf13/cobra"
)

func (a *App) newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Manage local cache directories",
		Args:  usageArgs(cobra.NoArgs),
	}
	cmd.AddCommand(a.newCachePurgeCmd())
	return cmd
}

func (a *App) newCachePurgeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "purge",
		Short: "Reclaim cache directories left by crashed processes",
		Long: `Cache purge removes storhub's local cache leftovers: per-project git worktrees
whose owning process is gone, and legacy pre-XDG temp roots. Directories held by
live processes are never touched. No network access, no token required.

This reclaims the shared cache base (see STORHUB_CACHE_DIR), not any single
command's --cachedir FUSE overlay dir: purging never deletes the overlay
cache a mounted filesystem is actively using.`,
		Args: usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			logger := shlog.NewLogger(shlog.Options{Level: shlog.LevelWarn, Format: shlog.FormatText, Output: a.stderr})
			reaped := storage.ReapOrphanedCaches(logger)
			unit := "entries"
			if reaped == 1 {
				unit = "entry"
			}
			_, _ = fmt.Fprintf(a.stderr, "reclaimed %d orphaned cache %s\n", reaped, unit)
			return nil
		},
	}
}
