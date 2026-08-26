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
  const normalized = { ...config, basePath: config.basePath.replace(/\/+$/, '') || '/api/v1' }
  return { provide: { uiConfig: normalized } }
})
