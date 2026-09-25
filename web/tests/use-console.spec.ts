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
  it('POSTs exactly {path, commit_sha} to /ops/revert', async () => {
    const c = useConsole()
    const ok = await c.revertPath('docs/readme.md', 'abc123def456')
    expect(ok).toBe(true)
    const call = calls.find((x) => x.url.includes('/projects/demo/ops/revert'))
    expect(call, 'revert-path route').toBeDefined()
    expect(bodyOf('/ops/revert')).toEqual({ path: 'docs/readme.md', commit_sha: 'abc123def456' })
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

describe('project health payload', () => {
  it('GETs /ops/status for health', async () => {
    const c = useConsole()
    await c.projectStatus()
    expect(calls.some((x) => x.url.includes('/ops/status') && x.method === 'GET')).toBe(true)
  })

  it('POSTs {} to /ops/enable to clear the latch', async () => {
    const c = useConsole()
    await c.enableProject()
    expect(bodyOf('/ops/enable')).toEqual({})
  })
})

describe('inspectPath failure', () => {
  it('clears the stale entry instead of keeping the previous file', async () => {
    const c = useConsole()
    c.selectedEntry.value = {
      path: 'old.txt',
      is_dir: false,
      size: 3,
      modified_at: 1,
      created_at: 1,
    }
    const fetchMock = globalThis.fetch as unknown as { mockRejectedValueOnce: (error: unknown) => void }
    fetchMock.mockRejectedValueOnce(new Error('boom'))
    await c.inspectPath('new.txt')
    expect(c.selectedEntry.value).toBeNull()
  })
})

describe('loadXattrs error flag', () => {
  it('marks a total list failure instead of reporting an empty list', async () => {
    const c = useConsole()
    c.selectedPath.value = 'f.txt'
    c.xattrsError.value = false
    const fetchMock = globalThis.fetch as unknown as { mockRejectedValueOnce: (error: unknown) => void }
    fetchMock.mockRejectedValueOnce(new Error('denied'))
    await c.loadXattrs()
    expect(c.xattrs.value).toEqual([])
    expect(c.xattrsError.value).toBe(true)
  })

  it('clears the flag on the next success', async () => {
    const c = useConsole()
    c.selectedPath.value = 'f.txt'
    c.xattrsError.value = true
    await c.loadXattrs()
    expect(c.xattrsError.value).toBe(false)
  })
})

describe('truncate validation', () => {
  it('rejects exponential, empty, and fractional sizes without PATCHing', async () => {
    const c = useConsole()
    c.selectedPath.value = 'f.txt'
    c.modalKind.value = 'truncate'
    for (const bad of ['1e3', '', '  ', '-5', '12.5']) {
      calls.length = 0
      c.modalForm.value.text = bad
      await c.submitModal()
      expect(c.modalError.value).toContain('non-negative integer')
      expect(calls.some(x => x.method === 'PATCH')).toBe(false)
    }
  })

  it('accepts a plain digit string and PATCHes it verbatim', async () => {
    const c = useConsole()
    c.selectedPath.value = 'f.txt'
    c.modalKind.value = 'truncate'
    c.modalForm.value.text = '1024'
    await c.submitModal()
    expect(c.modalError.value).toBe('')
    const call = calls.find(x => x.method === 'PATCH')
    expect(call?.url).toContain('size=1024')
  })
})

describe('utimes validation', () => {
  it('requires both fields instead of defaulting a cleared field to now', async () => {
    const c = useConsole()
    c.selectedPath.value = 'f.txt'
    c.modalKind.value = 'utimes'
    c.modalForm.value.atime = '2026-01-01T00:00'
    c.modalForm.value.mtime = ''
    await c.submitModal()
    expect(c.modalError.value).toContain('both atime and mtime')
    expect(calls.some(x => x.url.includes('/ops/utimes'))).toBe(false)
  })
})

describe('loadDirectory normalization', () => {
  it('canonicalizes raw typed paths at the single choke point', async () => {
    const c = useConsole()
    await c.loadDirectory('//a/./b/../c//')
    expect(c.currentPath.value).toBe('a/c')
  })
})

describe('prune payload', () => {
  it('defaults keep to 1, the only value the backend accepts', async () => {
    const c = useConsole()
    await c.prune('assets', undefined as unknown as number, true)
    expect(bodyOf('/ops/prune')).toEqual({ scope: 'assets', keep: 1, dry_run: true })
  })

  it('POSTs {scope, keep, dry_run:true} and skips the refresh for dry runs', async () => {
    const c = useConsole()
    await c.prune('history', 1, true)
    expect(bodyOf('/ops/prune')).toEqual({ scope: 'history', keep: 1, dry_run: true })
    expect(calls.some((x) => x.url.includes('/children'))).toBe(false)
  })

  it('POSTs {scope, keep, dry_run:false} and refreshes after a real purge', async () => {
    const c = useConsole()
    await c.prune('objects', 1, false)
    expect(bodyOf('/ops/prune')).toEqual({ scope: 'objects', keep: 1, dry_run: false })
    expect(calls.some((x) => x.url.includes('/projects/demo/children'))).toBe(true)
  })
})
