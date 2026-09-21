import type { UiConfig } from '~/plugins/config.client'
import { ApiError } from '~/utils/api-types'
import { joinApiPath } from '~/utils/url'

export interface ApiResult<T = unknown> {
  status: number
  payload: T
  /** ETag response header ('' when absent), for If-Match CAS on writes. */
  etag: string
}

/**
 * Thin fetch wrapper over the REST API: bearer auth, JSON/text decoding,
 * and typed errors. Client-only by construction (ssr:false).
 */
export function useApi() {
  const { $uiConfig } = useNuxtApp()
  const config = $uiConfig as UiConfig
  const token = useState<string>('auth-token', () => '')

  function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
    return token.value ? { ...extra, Authorization: `Bearer ${token.value}` } : extra
  }

  async function request<T = unknown>(
    path: string,
    options: RequestInit & { rawBody?: boolean; binary?: boolean } = {},
  ): Promise<ApiResult<T>> {
    const { rawBody, headers: extraHeaders, binary, ...rest } = options
    // Single choke point for basepath resolution: callers pass either bare
    // routes or url()-prefixed paths and both land on the right URL.
    const response = await fetch(joinApiPath(config.basePath, path), {
      ...rest,
      headers: authHeaders((extraHeaders as Record<string, string>) ?? {}),
    })
    let payload: unknown
    if (binary) {
      payload = await response.arrayBuffer()
    } else if (rawBody) {
      // File content must survive the round trip untouched: never let a
      // JSON content-type trigger a parse-and-reformat of raw bytes.
      payload = await response.text()
    } else {
      const contentType = response.headers.get('content-type') ?? ''
      payload = contentType.includes('application/json')
        ? await response.json().catch(() => null)
        : await response.text()
    }
    if (!response.ok) {
      const message =
        payload && typeof payload === 'object' && 'error' in payload
          ? String((payload as { error?: { message?: string } }).error?.message ?? response.statusText)
          : typeof payload === 'string' && payload
            ? payload.slice(0, 200)
            : response.statusText
      throw new ApiError(response.status, message || `HTTP ${response.status}`, payload)
    }
    return { status: response.status, payload: payload as T, etag: response.headers.get('etag') ?? '' }
  }

  function url(path: string, params: Record<string, string | undefined> = {}): string {
    const query = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) query.set(key, value)
    }
    const text = query.toString()
    // Delegate to the same normalizer request() uses: callers pass bare
    // routes here and the result feeds back into request(), so both must
    // agree on basepath resolution (no silent double-prefixing).
    return joinApiPath(config.basePath, text ? `${path}?${text}` : path)
  }

  async function getJSON<T>(path: string): Promise<T> {
    return (await request<T>(path)).payload
  }

  async function postJSON<T>(path: string, body: unknown): Promise<T> {
    return (
      await request<T>(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
    ).payload
  }

  return { config, token, url, request, getJSON, postJSON }
}
