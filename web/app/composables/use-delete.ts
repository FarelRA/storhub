import type { AnyEntry, DirEntry } from '~/utils/api-types'
import { sharedState } from './console-state'

export interface DeleteDeps {
  run: <T>(label: string, fn: () => Promise<T>, quiet?: boolean) => Promise<T | null>
  getJSON: <T>(path: string) => Promise<T>
  postJSON: <T>(path: string, body: unknown) => Promise<T>
  projectURL: (suffix: string) => string
  url: (path: string, params?: Record<string, string | undefined>) => string
  refreshAll: () => Promise<boolean>
  clearSelection: () => void
}

/**
 * Single client-side delete service (recursive-delete stays client-side: no
 * server recursive op exists, so the N+1 children-listing fallback chain
 * lives here, in one place, instead of sprawled across the facade).
 */
export function useDeleteService(deps: DeleteDeps) {
  const { entries, selectedPath, selectedPaths } = sharedState()
  const toasts = useToasts()

  async function removeRecursive(path: string): Promise<boolean> {
    // List children and delete them first (depth-first)
    try {
      const payload = await deps.getJSON<{ entries?: DirEntry[] }>(
        deps.url(deps.projectURL('/children'), { path }),
      )
      const kids = payload.entries ?? []
      for (const kid of kids) {
        const ok = await removeRecursive(kid.path)
        if (!ok) return false
      }
    } catch {
      // If we can't list, try to unlink as file
      const res = await deps.run(`Remove ${path}`, () => deps.postJSON(deps.projectURL('/ops/unlink'), { path }), true)
      return res !== null
    }
    // Now the directory should be empty, try rmdir; if it fails because it's a file, try unlink
    let res = await deps.run(`Remove ${path}`, () => deps.postJSON(deps.projectURL('/ops/rmdir'), { path }), true)
    if (res !== null) return true
    res = await deps.run(`Remove ${path}`, () => deps.postJSON(deps.projectURL('/ops/unlink'), { path }), true)
    return res !== null
  }

  async function removeMany(paths: string[]): Promise<boolean> {
    const results = await Promise.all(
      paths.map(async (p) => {
        const entry = entries.value.find(e => e.path === p)
        // If we know it's a dir, use recursive; otherwise try recursive which handles both
        if (entry?.is_dir) return removeRecursive(p)
        // For files or unknown, try direct unlink, fallback to recursive
        const res = await deps.run(`Remove ${p}`, () => deps.postJSON(deps.projectURL('/ops/unlink'), { path: p }), true)
        if (res !== null) return true
        return removeRecursive(p)
      }),
    )
    const ok = results.every(Boolean)
    if (!ok) toasts.error(`Failed to remove ${results.filter(v => !v).length}/${paths.length} items`)
    for (const p of paths) selectedPaths.value.delete(p)
    selectedPaths.value = new Set(selectedPaths.value)
    const remaining = [...selectedPaths.value].pop()
    if (remaining === undefined) deps.clearSelection()
    else selectedPath.value = remaining
    // refreshAll keeps the (now re-selected) path and re-inspects it, so the
    // details pane survives the reload instead of being wiped by it.
    await deps.refreshAll()
    return ok
  }

  async function removeSelected(entry: AnyEntry): Promise<boolean> {
    // For directories, use recursive path via removeMany
    if (entry.is_dir) return removeMany([entry.path])
    const done = await deps.run(`Remove ${entry.path}`, () =>
      deps.postJSON(deps.projectURL('/ops/unlink'), { path: entry.path }),
    )
      .then(ok => ok !== null)
    if (done) {
      selectedPaths.value.delete(entry.path)
      selectedPaths.value = new Set(selectedPaths.value)
      if (selectedPath.value === entry.path) deps.clearSelection()
      if (selectedPaths.value.size === 0) deps.clearSelection()
      await deps.refreshAll()
    }
    return done
  }

  return { removeRecursive, removeMany, removeSelected }
}
