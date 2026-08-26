export interface UiConfig {
  basePath: string
  authEnabled: boolean
  /** Pinned by `storhub serve <project>`; empty for plain `storhub rest`. */
  project?: string
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
  const raw0 = window.STORHUB_UI_CONFIG ?? { basePath: '/api/v1', authEnabled: true }
  const base = !raw0.basePath || raw0.basePath === '/' ? '' : raw0.basePath.replace(/\/+$/, '')
  return {
    provide: {
      uiConfig: { basePath: base, authEnabled: raw0.authEnabled !== false, project: raw0.project ?? '' },
    },
  }
})
