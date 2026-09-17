import type { DirEntry } from '~/utils/api-types'
import { ENTRY_MENU } from '~/utils/limits'
import { useEscape } from './use-escape'

export interface EntryMenuState {
  entry: DirEntry
  style: Record<string, string>
}

/**
 * Kebab-menu slice of EntryList: one open menu at a time, rendered in a
 * body portal so pane scrolling can never clip it. Flips above the anchor
 * when space below runs out. Escape handling reuses useEscape.
 */
export function useEntryMenu() {
  const openMenu = ref<EntryMenuState | null>(null)
  const kebabButtons = new Map<string, HTMLElement>()

  function setKebabRef(path: string, el: Element | { $el?: unknown } | null) {
    if (el instanceof HTMLElement) kebabButtons.set(path, el)
  }

  function toggleMenu(entry: DirEntry) {
    if (openMenu.value?.entry.path === entry.path) {
      closeMenu()
      return
    }
    const button = kebabButtons.get(entry.path)
    if (!button) return
    const rect = button.getBoundingClientRect()
    const vw = window.innerWidth
    const vh = window.innerHeight
    let left = rect.right
    // Clamp horizontally so the menu (right-aligned to the button) never leaves the viewport
    if (left > vw - ENTRY_MENU.MARGIN_PX) left = vw - ENTRY_MENU.MARGIN_PX
    if (left - ENTRY_MENU.WIDTH_PX < ENTRY_MENU.MARGIN_PX) left = ENTRY_MENU.WIDTH_PX + ENTRY_MENU.MARGIN_PX
    const spaceBelow = vh - rect.bottom - ENTRY_MENU.MARGIN_PX
    const spaceAbove = rect.top - ENTRY_MENU.MARGIN_PX
    // Flip only if the menu wouldn't fit below and there's more room above
    const flipUp = spaceBelow < ENTRY_MENU.MAX_HEIGHT_PX && spaceAbove > spaceBelow
    const available = flipUp ? spaceAbove : spaceBelow
    // Only limit height to what the viewport actually offers - the menu
    // will scroll internally (overflowY) only when its natural height
    // exceeds this available space.
    const maxHeight = Math.max(ENTRY_MENU.MIN_HEIGHT_PX, available - 4)
    const y = flipUp ? rect.top : rect.bottom + 4
    openMenu.value = {
      entry,
      style: {
        left: `${left}px`,
        ...(flipUp ? { bottom: `${vh - y + 4}px` } : { top: `${y}px` }),
        transform: 'translateX(-100%)',
        maxHeight: `${maxHeight}px`,
        overflowY: 'auto',
      },
    }
  }

  function closeMenu() {
    openMenu.value = null
  }

  function onGlobalPointer(event: MouseEvent) {
    const target = event.target as HTMLElement
    if (!target.closest('[data-entry-menu]')) closeMenu()
  }

  function addMenuListeners() {
    window.addEventListener('click', onGlobalPointer, true)
    window.addEventListener('resize', closeMenu)
  }

  function removeMenuListeners() {
    window.removeEventListener('click', onGlobalPointer, true)
    window.removeEventListener('resize', closeMenu)
  }

  watch(openMenu, (open) => {
    if (import.meta.client) {
      if (open) addMenuListeners()
      else removeMenuListeners()
    }
  })

  // Unmounting with the menu open must not leak the global listeners (the
  // watcher above may not fire during unmount).
  onUnmounted(() => {
    if (import.meta.client) removeMenuListeners()
  })

  useEscape(closeMenu, computed(() => openMenu.value !== null))

  return { openMenu, setKebabRef, toggleMenu, closeMenu }
}
