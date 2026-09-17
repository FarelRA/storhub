/**
 * Path string helpers (pure, DOM-free): canonicalization for routing,
 * traversal guards, and mutation payloads.
 *
 * Display formatting (formatBytes, relativeTime, ...) stays in
 * `~/composables/use-format`, which re-exports these two so existing
 * import sites keep working.
 */
export function normalizePath(path: string): string {
  const clean: string[] = []
  for (const part of path.split('/')) {
    if (!part || part === '.') continue
    if (part === '..') {
      clean.pop()
      continue
    }
    clean.push(part)
  }
  return clean.join('/')
}

export function parentPath(path: string): string {
  const current = normalizePath(path)
  return current.split('/').slice(0, -1).join('/')
}
