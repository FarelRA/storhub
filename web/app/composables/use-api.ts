import type { UiConfig } from '~/plugins/config.client'
import { ApiError } from '~/utils/api-types'
import { joinApiPath } from '~/utils/url'

export interface ApiResult<T = unknown> {
  status: number
  payload: T
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
    options: RequestInit & { rawBody?: boolean } = {},
  ): Promise<ApiResult<T>> {
    const { rawBody, headers: extraHeaders, ...rest } = options
    // Single choke point for base-path resolution: callers pass either bare
    // routes or url()-prefixed paths and both land on the right URL.
    const response = await fetch(joinApiPath(config.basePath, path), {
      ...rest,
      headers: authHeaders((extraHeaders as Record<string, string>) ?? {}),
    })
    const contentType = response.headers.get('content-type') ?? ''
    const payload: unknown = contentType.includes('application/json')
      ? await response.json().catch(() => null)
      : await response.text()
    if (!response.ok) {
      const message =
        payload && typeof payload === 'object' && 'error' in payload
          ? String((payload as { error?: { message?: string } }).error?.message ?? response.statusText)
          : typeof payload === 'string' && payload
            ? payload.slice(0, 200)
            : response.statusText
      throw new ApiError(response.status, message || `HTTP ${response.status}`, payload)
    }
    return { status: response.status, payload: payload as T }
  }

  function url(path: string, params: Record<string, string | undefined> = {}): string {
    const query = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) query.set(key, value)
    }
    const text = query.toString()
    return `${config.basePath}${path}${text ? `?${text}` : ''}`
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

  async function sendText<T>(method: string, path: string, body?: string): Promise<ApiResult<T>> {
    return request<T>(path, { method, body })
  }

  return { config, token, url, request, getJSON, postJSON, sendText }
}
