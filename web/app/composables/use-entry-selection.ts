import type { DirEntry } from '~/utils/api-types'
import { TIMEOUTS } from '~/utils/limits'
import { useBreakpoint } from './use-breakpoint'

/**
 * Row-selection slice of EntryList: desktop/mobile oracles, shift-anchor
 * range select, full keyboard nav, and touch long-press. The list component
 * keeps rendering + the menu portal; everything stateful lives here.
 */
export function useEntrySelection(opts: {
  getEntries: () => DirEntry[]
  openEntry: (entry: DirEntry) => void
}) {
  const consoleStore = useConsole()
  const { isDesktop, isCoarsePointer, hasNoHover, hasTouch } = useBreakpoint()

  // Desktop = click selects, double-click opens, arrows work.
  // Mobile = tap opens when nothing selected, otherwise toggles.
  // Many desktop browsers now report maxTouchPoints>0 (touchpad) or
  // pointer:coarse, so mobile additionally requires a small viewport.
  const isMobile = computed(
    () => !isDesktop.value && (isCoarsePointer.value || hasNoHover.value || hasTouch.value),
  )

  const selectedSet = computed(() => consoleStore.selectedPaths.value)
  const listRef = ref<HTMLElement | null>(null)

  onMounted(() => {
    window.addEventListener('keydown', handleKeydown)
    // Focus list for desktop keyboard navigation
    if (!isMobile.value && listRef.value) {
      nextTick(() => listRef.value?.focus({ preventScroll: true }))
    }
  })

  // Unmounting with a pending long-press must not fire a stray select.
  let pressTimer: ReturnType<typeof setTimeout> | null = null

  onUnmounted(() => {
    window.removeEventListener('keydown', handleKeydown)
    if (pressTimer !== null) {
      clearTimeout(pressTimer)
      pressTimer = null
    }
  })

  // Keep keyboard focus on the list after selection changes so arrows keep working
  watch(() => opts.getEntries().length, () => {
    if (!isMobile.value && listRef.value && document.activeElement?.closest('[data-entry-list]')) {
      listRef.value.focus({ preventScroll: true })
    }
  })

  function isSelected(entry: DirEntry): boolean {
    return selectedSet.value.has(entry.path)
  }

  let shiftAnchor: string | null = null

  function handleSelect(entry: DirEntry, event: MouseEvent) {
    const e = event as MouseEvent & { metaKey: boolean; ctrlKey: boolean; shiftKey: boolean }
    if (e.shiftKey) {
      // Keep anchor at the point where Shift was first held; extend from there
      if (!shiftAnchor) shiftAnchor = consoleStore.lastSelected.value ?? consoleStore.selectedPath.value ?? entry.path
      const anchor = shiftAnchor ?? entry.path
      consoleStore.selectRange(anchor, entry.path)
    } else {
      shiftAnchor = null
      if (e.metaKey || e.ctrlKey) {
        consoleStore.toggleSelect(entry.path)
      } else {
        consoleStore.selectSingle(entry.path)
      }
    }
  }

  function handleRowClick(entry: DirEntry, event: MouseEvent) {
    if (isMobile.value) {
      // Mobile: click is open when nothing selected, else toggle select
      if (selectedSet.value.size === 0) opts.openEntry(entry)
      else handleSelect(entry, event)
      return
    }
    handleSelect(entry, event)
  }

  function handleRowDblClick(entry: DirEntry) {
    if (!isMobile.value) opts.openEntry(entry)
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.defaultPrevented) return
    const rows = opts.getEntries()
    if (!rows.length) return
    const active = document.activeElement as HTMLElement | null
    if (active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA' || active.isContentEditable)) return
    // Only handle when focus is inside the entry list or no input is focused
    const inList = !!active?.closest('[data-entry-list]') || active === listRef.value || !active || active === document.body
    if (!inList && active && active !== document.body) {
      // If focus is outside the list (e.g., sidebar), don't hijack arrows
      const listEl = listRef.value
      if (listEl && !listEl.contains(active)) return
    }
    const idx = rows.findIndex(e => e.path === consoleStore.lastSelected.value)
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      const dir = event.key === 'ArrowDown' ? 1 : -1
      // When nothing is selected, ArrowDown selects first, ArrowUp selects last
      let nextIdx: number
      if (idx === -1) {
        nextIdx = dir === 1 ? 0 : rows.length - 1
      } else {
        nextIdx = Math.max(0, Math.min(rows.length - 1, idx + dir))
      }
      const next = rows[nextIdx]
      if (!next) return
      if (event.shiftKey) {
        if (!shiftAnchor) shiftAnchor = consoleStore.lastSelected.value ?? consoleStore.selectedPath.value ?? rows[idx]?.path ?? next.path
        const anchor = shiftAnchor ?? next.path
        consoleStore.selectRange(anchor, next.path)
      } else {
        shiftAnchor = null
        consoleStore.selectSingle(next.path)
      }
      // Keep the newly selected row visible and keep keyboard focus on the list
      nextTick(() => {
        const row = document.querySelector<HTMLElement>(`[data-path="${CSS.escape(next.path)}"]`)
        row?.scrollIntoView({ block: 'nearest' })
      })
    } else if (event.key === 'Enter') {
      event.preventDefault()
      shiftAnchor = null
      const targetPath = consoleStore.lastSelected.value || consoleStore.selectedPath.value
      const target = rows.find(e => e.path === targetPath) ?? (idx >= 0 ? rows[idx] : null)
      if (target) opts.openEntry(target)
    } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'a') {
      event.preventDefault()
      shiftAnchor = null
      consoleStore.selectAll()
    } else if (event.key === 'Escape') {
      shiftAnchor = null
      consoleStore.clearSelection()
    } else if (event.key === 'Home' || (event.key === 'ArrowUp' && (event.ctrlKey || event.metaKey))) {
      event.preventDefault()
      shiftAnchor = null
      const first = rows[0]
      if (!first) return
      if (event.shiftKey) {
        if (!shiftAnchor) shiftAnchor = consoleStore.lastSelected.value ?? rows[idx]?.path ?? first.path
        consoleStore.selectRange(shiftAnchor, first.path)
      } else {
        consoleStore.selectSingle(first.path)
      }
    } else if (event.key === 'End' || (event.key === 'ArrowDown' && (event.ctrlKey || event.metaKey))) {
      event.preventDefault()
      shiftAnchor = null
      const last = rows.at(-1)
      if (!last) return
      if (event.shiftKey) {
        if (!shiftAnchor) shiftAnchor = consoleStore.lastSelected.value ?? rows[idx]?.path ?? last.path
        consoleStore.selectRange(shiftAnchor, last.path)
      } else {
        consoleStore.selectSingle(last.path)
      }
    }
  }

  // Long-press for mobile select
  function onTouchStart(entry: DirEntry) {
    if (!isMobile.value) return
    pressTimer = setTimeout(() => {
      consoleStore.toggleSelect(entry.path)
      if (navigator.vibrate) navigator.vibrate(20)
      pressTimer = null
    }, TIMEOUTS.LONG_PRESS_MS)
  }

  function onTouchEnd() {
    if (pressTimer) {
      clearTimeout(pressTimer)
      pressTimer = null
    }
  }

  function onTouchMove() {
    onTouchEnd()
  }

  return {
    isMobile,
    listRef,
    selectedSet,
    isSelected,
    handleRowClick,
    handleRowDblClick,
    handleKeydown,
    onTouchStart,
    onTouchEnd,
    onTouchMove,
  }
}
