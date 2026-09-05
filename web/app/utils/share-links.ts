export const SHARE_TTL_7D = 604800
export const SHARE_TTL_5M = 300

export function shareLink(share: { token?: string; id: string }): string {
  return `${window.location.origin}${window.location.pathname}?share=${encodeURIComponent(share.token ?? share.id)}`
}

export function directLink(share: { download_url?: string }): string {
  if (!share.download_url) return ''
  return new URL(share.download_url, window.location.origin).toString()
}
