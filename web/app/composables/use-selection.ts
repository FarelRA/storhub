import { sharedState } from './console-state'
import { clearPreviewState } from './use-preview'

/**
 * Selection model slice of the god-composable: single/multi/range select
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
  } = sharedState()

  function clearSelection(): void {
    selectedPath.value = ''
    selectedEntry.value = null
    selectedPaths.value = new Set()
    lastSelected.value = null
    editorContent.value = ''
    editorDirty.value = false
    editorETag.value = ''
    xattrs.value = []
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
