/**
 * Central budgets for the magic durations and pixel constants that used to
 * hide inline across the console. Every entry carries its WHY so the next
 * reader never has to re-derive a number from a symptom.
 */
export const TIMEOUTS = {
  /** One file PUT must not pin the progress bar forever on a stalled uplink. */
  UPLOAD_MS: 15 * 60_000,
  /** Errors need reading time; plain confirmations should get out of the way. */
  TOAST_ERROR_MS: 8000,
  TOAST_MS: 4000,
  /** Touch-and-hold before a row counts as a selection gesture on mobile. */
  LONG_PRESS_MS: 500,
} as const

export const ENTRY_MENU = {
  /** min-w-56 (224px) plus padding and viewport margin: covers the longest labels ("Copy direct link"). */
  WIDTH_PX: 256,
  /** Viewport edge margin so the menu never kisses the screen border. */
  MARGIN_PX: 8,
  /** Flip threshold: the natural height a full single-entry menu needs. */
  MAX_HEIGHT_PX: 380,
  /** Minimum usable height when the viewport itself is tiny. */
  MIN_HEIGHT_PX: 160,
} as const

/** Minimum px reserved for the preview column when clamping directory width. */
export const PREVIEW_COLUMN_RESERVE_PX = 560
