/**
 * Payload-construction tests for the mutation helpers in use-console.ts.
 *
 * vitest runs without the Nuxt build plugin, so the auto-imports the
 * composable relies on (ref/computed/useState/useApi/useToasts and the
 * use-format helpers) are installed as globals BEFORE the module is
 * dynamically imported. The REST contract (route + JSON body field names)
 * is asserted against a stubbed fetch: a renamed or dropped field fails.
 */
import { computed, ref } from 'vue'
import type { Ref } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  formatBytes,
  formatCount,
  formatDateTime,
  formatMode,
  normalizePath,
  parentPath,
  relativeTime,
  toDatetimeLocal,
} from '../app/composables/use-format'

const stateStore = new Map<string, { value: unknown }>()
function useState<T>(key: string, init: () => T): Ref<T> {
  let slot = stateStore.get(key)
  if (!slot) {
    slot = ref(init())
    stateStore.set(key, slot)
  }
  return slot as Ref<T>
}

const uiConfig = { basePath: '/api/v1', authEnabled: true, project: '' }

interface FetchCall {
  url: string
  method: string
  body: unknown
}
const calls: FetchCall[] = []

function jsonResponse(body: unknown) {
  return {
    ok: true,
    status: 200,
    headers: {
      get: (name: string) => (name.toLowerCase() === 'content-type' ? 'application/json' : null),
    },
    json: async () => body,
    text: async () => JSON.stringify(body),
    arrayBuffer: async () => new ArrayBuffer(0),
  }
}

vi.stubGlobal('ref', ref)
vi.stubGlobal('computed', computed)
vi.stubGlobal('useState', useState)
vi.stubGlobal('useNuxtApp', () => ({ $uiConfig: uiConfig }))
vi.stubGlobal('useToasts', () => ({
  toasts: ref([]),
  dismiss: () => {},
  success: () => {},
  error: () => {},
  info: () => {},
}))
for (const helper of [
  formatBytes,
  formatCount,
  formatDateTime,
  formatMode,
  normalizePath,
  parentPath,
  relativeTime,
  toDatetimeLocal,
]) {
  vi.stubGlobal(helper.name, helper)
}

const { useApi } = await import('../app/composables/use-api')
vi.stubGlobal('useApi', useApi)
vi.stubGlobal(
  'fetch',
  vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({
      url: String(input),
      method: init?.method ?? 'GET',
      body: init?.body,
    })
    return jsonResponse({})
  }),
)
const { useConsole } = await import('../app/composables/use-console')

function bodyOf(urlPart: string): Record<string, unknown> {
  const call = calls.find((c) => c.url.includes(urlPart) && c.method === 'POST')
  if (!call) throw new Error(`no POST to ${urlPart}; saw ${calls.map((c) => `${c.method} ${c.url}`).join(', ')}`)
  return JSON.parse(String(call.body)) as Record<string, unknown>
}

beforeEach(() => {
  calls.length = 0
  const c = useConsole()
  c.project.value = 'demo'
  c.currentPath.value = ''
  c.selectedPath.value = ''
  c.selectedEntry.value = null
})

describe('revertPath payload', () => {
  it('POSTs exactly {path, commit_sha} to /ops/revert-path', async () => {
    const c = useConsole()
    const ok = await c.revertPath('docs/readme.md', 'abc123def456')
    expect(ok).toBe(true)
    const call = calls.find((x) => x.url.includes('/projects/demo/ops/revert-path'))
    expect(call, 'revert-path route').toBeDefined()
    expect(bodyOf('/ops/revert-path')).toEqual({ path: 'docs/readme.md', commit_sha: 'abc123def456' })
  })
})

describe('rollbackRevision payload', () => {
  it('POSTs exactly {commit_sha} to /ops/rollback', async () => {
    const c = useConsole()
    const ok = await c.rollbackRevision('deadbeefcafe')
    expect(ok).toBe(true)
    expect(bodyOf('/ops/rollback')).toEqual({ commit_sha: 'deadbeefcafe' })
  })
})

describe('purge payload', () => {
  it('POSTs {scope, keep, dry_run:true} and skips the refresh for dry runs', async () => {
    const c = useConsole()
    await c.purge('history', 1, true)
    expect(bodyOf('/ops/purge')).toEqual({ scope: 'history', keep: 1, dry_run: true })
    expect(calls.some((x) => x.url.includes('/children'))).toBe(false)
  })

  it('POSTs {scope, keep, dry_run:false} and refreshes after a real purge', async () => {
    const c = useConsole()
    await c.purge('objects', 1, false)
    expect(bodyOf('/ops/purge')).toEqual({ scope: 'objects', keep: 1, dry_run: false })
    expect(calls.some((x) => x.url.includes('/projects/demo/children'))).toBe(true)
  })
})
