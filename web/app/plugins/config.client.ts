export interface UiConfig {
  basePath: string
  authEnabled: boolean
}

declare global {
  interface Window {
    STORHUB_UI_CONFIG?: UiConfig
  }
}

/**
 * The Go server injects `/config.js` (window.STORHUB_UI_CONFIG) so the same
 * static bundle works under any --base-path. In `nuxt dev` there is no such
 * script, so fall back to the default REST prefix.
 */
export default defineNuxtPlugin(() => {
  const config = window.STORHUB_UI_CONFIG ?? { basePath: '/api/v1', authEnabled: true }
  // "/" or "" means the API is mounted at the root; anything else loses its
  // trailing slash. joinApiPath() consumes the result verbatim.
  const raw = config.basePath ?? '/api/v1'
  const basePath = !raw || raw === '/' ? '' : raw.replace(/\/+$/, '')
  return { provide: { uiConfig: { basePath, authEnabled: config.authEnabled } } }
})
