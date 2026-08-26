/**
 * Draggable panel widths for the desktop three-column shell.
 * Module-scoped singleton, persisted to localStorage; every value is
 * clamped so no drag can push a pane off-screen.
 */
export type PanelKey = 'sidebar' | 'directory'

const STORAGE_KEY = 'storhub.panels.v1'

const LIMITS: Record<PanelKey, { min: number; max: number; def: number }> = {
  sidebar: { min: 220, max: 480, def: 288 },
  directory: { min: 340, max: 900, def: 560 },
}

function clampFor(key: PanelKey, px: number): number {
  const { min, max } = LIMITS[key]
  // The directory pane must leave room for the preview column.
  const effectiveMax = key === 'directory' && typeof window !== 'undefined'
    ? Math.min(max, window.innerWidth - 560)
    : max
  return Math.min(Math.max(Math.round(px), min), Math.max(effectiveMax, min))
}

function load(): Record<PanelKey, number> {
  const fallback = {
    sidebar: LIMITS.sidebar.def,
    directory: LIMITS.directory.def,
  }
  if (typeof window === 'undefined') return fallback
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return fallback
    const parsed = JSON.parse(raw) as Partial<Record<PanelKey, number>>
    return {
      sidebar: clampFor('sidebar', parsed.sidebar ?? fallback.sidebar),
      directory: clampFor('directory', parsed.directory ?? fallback.directory),
    }
  } catch {
    return fallback
  }
}

const panels = ref(load())

function setWidth(key: PanelKey, px: number): void {
  panels.value = { ...panels.value, [key]: clampFor(key, px) }
}

function resetPanel(key: PanelKey): void {
  setWidth(key, LIMITS[key].def)
}

function persist(): void {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(panels.value))
  } catch {
    // Storage may be unavailable (private mode); resizing still works.
  }
}

export function usePanelWidths() {
  return {
    panels,
    setWidth,
    resetPanel,
    persist,

  }
}
