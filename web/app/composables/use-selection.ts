import type { AnyEntry } from '~/utils/api-types'
import { useConsoleState } from './console-state'
import { clearPreviewState } from './use-preview'

/**
 * Selection model slice of the console composable: single/multi/range select
 * over the current directory listing. Operates on the shared state; the
 * useConsole facade re-exports these under their historic names.
 */
export function useSelection() {
  const {
    entries,
    selectedEntry,
    selectedPath,
    selectedPaths,
    lastSelected,
    editorContent,
    editorDirty,
    editorETag,
    editorIsText,
    xattrs,
    xattrsError,
  } = useConsoleState()

  function clearSelection(): void {
    selectedPath.value = ''
    selectedEntry.value = null
    selectedPaths.value = new Set()
    lastSelected.value = null
    editorContent.value = ''
    editorDirty.value = false
    editorETag.value = ''
    xattrs.value = []
    xattrsError.value = false
    editorIsText.value = true
    clearPreviewState()
  }

  function isSelected(path: string): boolean {
    return selectedPaths.value.has(path)
  }

  function selectSingle(path: string): void {
    selectedPaths.value = new Set([path])
    lastSelected.value = path
    selectedPath.value = path
  }

  function toggleSelect(path: string): void {
    const next = new Set(selectedPaths.value)
    if (next.has(path)) next.delete(path)
    else next.add(path)
    selectedPaths.value = next
    lastSelected.value = path
    if (next.size === 1) {
      const only = [...next][0]
      if (only !== undefined) selectedPath.value = only
    } else if (next.size === 0) {
      clearSelection()
    } else {
      selectedPath.value = path
    }
  }

  function selectRange(from: string, to: string): void {
    const idxFrom = entries.value.findIndex(e => e.path === from)
    const idxTo = entries.value.findIndex(e => e.path === to)
    if (idxFrom === -1 || idxTo === -1) {
      selectSingle(to)
      return
    }
    const [a, b] = idxFrom < idxTo ? [idxFrom, idxTo] : [idxTo, idxFrom]
    const range = entries.value.slice(a, b + 1).map(e => e.path)
    selectedPaths.value = new Set(range)
    lastSelected.value = to
    selectedPath.value = to
  }

  function selectAll(): void {
    selectedPaths.value = new Set(entries.value.map(e => e.path))
    const lastEntry = entries.value.at(-1)
    if (lastEntry) {
      lastSelected.value = lastEntry.path
      selectedPath.value = lastEntry.path
    }
  }

  return { clearSelection, isSelected, selectSingle, toggleSelect, selectRange, selectAll }
}

export interface SingleSelection {
  path: string
  entry: AnyEntry | null
}

/**
 * The one focused entry, whether it arrives via the multi-select set or the
 * single focus path: null while a multi-selection is active. Shared by every
 * surface that acts on "the selected entry" (Shares panel buttons, revert
 * actions). EntryList's menuTargets is deliberately separate: it derives
 * row-menu targets from the open menu's entry, a different concept.
 *
 * Reader rule: action decisions on "the entry" (share, revert, panel buttons)
 * read through this helper. Direct selectedPath reads stay inside the console
 * facade and the selection internals (form prefills, stat bookkeeping) plus
 * read-only display of the focus path. The dual focus-plus-set model stays:
 * migrating it would churn every consumer for no behavior gain.
 */
export function useSingleSelection() {
  const { entries, selectedEntry, selectedPath, selectedPaths } = useConsoleState()
  return computed<SingleSelection | null>(() => {
    if (selectedPaths.value.size === 1) {
      const [only] = [...selectedPaths.value]
      if (only === undefined) return null
      const entry = entries.value.find(e => e.path === only) ?? selectedEntry.value
      return { path: only, entry }
    }
    if (selectedPaths.value.size === 0 && selectedPath.value) {
      return { path: selectedPath.value, entry: selectedEntry.value }
    }
    return null
  })
}
