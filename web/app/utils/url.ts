/**
 * Join the server-configured REST base path with an API route.
 *
 * Callers may pass either a bare route ("/auth/login") or an already-prefixed
 * one ("/api/v1/projects/a" produced by the url() helper); both resolve to a
 * single correct URL. An empty or "/" base means the API lives at the root.
 *
 * The trailing-slash strip here mirrors the plugin normalizer
 * (config.client.ts): both ends normalize so dev-injected and
 * server-injected configs agree. Idempotent by design, covered in url.spec.
 */
export function joinApiPath(base: string, path: string): string {
  const cleanBase = !base || base === '/' ? '' : base.replace(/\/+$/, '')
  if (!path.startsWith('/')) return path
  // A bare "/" means the API root itself.
  if (path === '/') return cleanBase || '/'
  if (cleanBase && (path === cleanBase || path.startsWith(`${cleanBase}/`))) return path
  return `${cleanBase}${path}`
}

/**
 * Single encoder for path and token segments in routes and share links.
 * Attribute-selector escaping (CSS.escape in the entry keyboard) is a
 * separate domain and stays out of this helper.
 */
export function encodeSegment(value: string): string {
  return encodeURIComponent(value)
}
