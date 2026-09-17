// WHY two lifetimes: the EntryList kebab "Share" mints a short-lived link
// (5 min, just long enough to paste into a chat), while the Shares panel
// mints week-long links for durable hand-off. The split is deliberate:
// keep it, and keep each surface's label honest about its own TTL.
export const SHARE_TTL_7D = 604800
export const SHARE_TTL_5M = 300

/** Human labels for the TTLs above: toast text derives from these, never from magic strings. */
export const SHARE_TTL_7D_LABEL = '7 days'
export const SHARE_TTL_5M_LABEL = '5 min'

export function shareTtlLabel(seconds: number): string {
  if (seconds === SHARE_TTL_5M) return SHARE_TTL_5M_LABEL
  if (seconds === SHARE_TTL_7D) return SHARE_TTL_7D_LABEL
  return `${seconds}s`
}

export function shareLink(share: { token?: string; id: string }): string {
  return `${window.location.origin}${window.location.pathname}?share=${encodeURIComponent(share.token ?? share.id)}`
}

export function directLink(share: { download_url?: string }): string {
  if (!share.download_url) return ''
  return new URL(share.download_url, window.location.origin).toString()
}
