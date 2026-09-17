/**
 * Guard specs for the wave-1 web fixes: the safety-critical predicates are
 * pure and pinned here, not behind the 1350-line composable harness.
 * saveFile's truncated-preview guard is pinned through usePreview with
 * stubbed deps (no fetch may fire for an incomplete preview).
 */
import { computed, ref } from 'vue'
import type { Ref } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { bulkRename } from '~/composables/use-modals'
import { usePreview } from '~/composables/use-preview'
import { sharedState } from '~/composables/console-state'
import { canDownloadWithoutToken } from '~/utils/download'
import { modalGroupOf } from '~/utils/modal-kinds'
import { normalizePath } from '~/utils/path'
import { isPreviewSaveable } from '~/utils/preview'
import { shareTtlLabel } from '~/utils/share-links'
import { joinApiPath } from '~/utils/url'

describe('isPreviewSaveable', () => {
  it('allows only complete text previews with a CAS token', () => {
    expect(isPreviewSaveable({ editorIsText: true, etag: 'e', shown: 10, total: 10 })).toBe(true)
    expect(isPreviewSaveable({ editorIsText: true, etag: 'e', shown: 0, total: 0 })).toBe(true)
  })
  it('refuses truncated sniffs, tokenless previews, and non-text', () => {
    expect(isPreviewSaveable({ editorIsText: true, etag: '', shown: 64, total: 200 })).toBe(false)
    expect(isPreviewSaveable({ editorIsText: true, etag: '', shown: 10, total: 10 })).toBe(false)
    expect(isPreviewSaveable({ editorIsText: false, etag: 'e', shown: 10, total: 10 })).toBe(false)
    expect(isPreviewSaveable({ editorIsText: false, etag: '', shown: 0, total: 0 })).toBe(false)
  })
})

describe('canDownloadWithoutToken', () => {
  it('refuses only authed mode without a token outside shared views', () => {
    expect(canDownloadWithoutToken(true, false, false)).toBe(false)
    expect(canDownloadWithoutToken(false, false, false)).toBe(true)
    expect(canDownloadWithoutToken(true, false, true)).toBe(true)
    expect(canDownloadWithoutToken(true, true, false)).toBe(true)
    expect(canDownloadWithoutToken(undefined, false, false)).toBe(false)
  })
})

describe('bulkRename', () => {
  it('builds rename payloads with destDir join', async () => {
    const posts: Array<{ url: string; body: unknown }> = []
    const ok = await bulkRename(
      async (_label, fn) => fn(),
      async (url, body) => {
        posts.push({ url, body })
        return { ok: true }
      },
      (suffix) => `/api/v1/projects/demo${suffix}`,
      ['a/x.txt', 'b/y.txt'],
      'dest',
      'rename',
    )
    expect(ok).toBe(true)
    expect(posts).toEqual([
      { url: '/api/v1/projects/demo/ops/rename', body: { old_path: 'a/x.txt', new_path: 'dest/x.txt' } },
      { url: '/api/v1/projects/demo/ops/rename', body: { old_path: 'b/y.txt', new_path: 'dest/y.txt' } },
    ])
  })
  it('builds copy payloads and reports partial failure', async () => {
    const posts: Array<{ url: string; body: unknown }> = []
    let n = 0
    const ok = await bulkRename(
      async (_label, fn) => fn(),
      async (url, body) => {
        posts.push({ url, body })
        n += 1
        return n === 1 ? null : { ok: true }
      },
      (suffix) => `/api/v1/projects/demo${suffix}`,
      ['a/x.txt', 'b/y.txt'],
      '',
      'copy',
    )
    expect(ok).toBe(false)
    expect(posts).toEqual([
      { url: '/api/v1/projects/demo/ops/copy', body: { src_path: 'a/x.txt', dst_path: 'x.txt' } },
      { url: '/api/v1/projects/demo/ops/copy', body: { src_path: 'b/y.txt', dst_path: 'y.txt' } },
    ])
  })
})

describe('normalizePath', () => {
  it('collapses separators, dots, and escape-to-root', () => {
    expect(normalizePath('a//b/../c/./')).toBe('a/c')
    expect(normalizePath('../../etc')).toBe('etc')
    expect(normalizePath('')).toBe('')
    expect(normalizePath('/a/b')).toBe('a/b')
    expect(normalizePath('a/b')).toBe('a/b')
  })
})

describe('joinApiPath', () => {
  it('joins bare routes and leaves prefixed ones alone', () => {
    expect(joinApiPath('/api/v1', '/auth/login')).toBe('/api/v1/auth/login')
    expect(joinApiPath('/api/v1', '/api/v1/projects/a')).toBe('/api/v1/projects/a')
    expect(joinApiPath('', '/x')).toBe('/x')
    expect(joinApiPath('/api/v1', '/content?path=a/b')).toBe('/api/v1/content?path=a/b')
  })
})

describe('modalGroupOf', () => {
  it('covers all 15 kinds', () => {
    expect(modalGroupOf('mkdir')).toBe('path')
    expect(modalGroupOf('create-file')).toBe('path')
    for (const k of ['rename', 'move', 'copy', 'link', 'symlink'] as const) {
      expect(modalGroupOf(k)).toBe('newPath')
    }
    for (const k of ['chmod', 'chown', 'utimes', 'xattr-set', 'xattr-remove'] as const) {
      expect(modalGroupOf(k)).toBe('meta')
    }
    for (const k of ['append', 'patch', 'truncate'] as const) {
      expect(modalGroupOf(k)).toBe('textOp')
    }
  })
})

describe('shareTtlLabel', () => {
  it('labels the two lifetimes and falls back to seconds', () => {
    expect(shareTtlLabel(300)).toBe('5 min')
    expect(shareTtlLabel(604800)).toBe('7 days')
    expect(shareTtlLabel(60)).toBe('60s')
  })
})

describe('saveFile truncated guard', () => {
  const stateStore = new Map<string, { value: unknown }>()
  function useState<T>(key: string, init: () => T): Ref<T> {
    let slot = stateStore.get(key)
    if (!slot) {
      slot = ref(init())
      stateStore.set(key, slot)
    }
    return slot as Ref<T>
  }
  vi.stubGlobal('useState', useState)
  vi.stubGlobal('computed', computed)
  vi.stubGlobal('useToasts', () => ({ success: vi.fn(), error: vi.fn() }))

  const requests: Array<{ path: string; headers: Record<string, string> }> = []
  const deps = {
    run: async <T>(_label: string, fn: () => Promise<T>): Promise<T | null> => fn(),
    request: async <T>(path: string, options?: { headers?: Record<string, string> }): Promise<{ payload: T } | null> => {
      requests.push({ path, headers: options?.headers ?? {} })
      return { payload: { etag: 'new' } as T }
    },
    projectURL: (suffix: string) => `/api/v1/projects/demo${suffix}`,
    url: (path: string, params?: Record<string, string | undefined>) => {
      const q = new URLSearchParams(params as Record<string, string>).toString()
      return q ? `${path}?${q}` : path
    },
    refreshAll: async () => true,
    canEditFile: () => true,
  }

  beforeEach(() => {
    requests.length = 0
    stateStore.clear()
  })

  it('never PUTs a truncated preview', async () => {
    const st = sharedState()
    st.selectedPath.value = 'big.txt'
    st.editorContent.value = 'prefix'
    st.editorIsText.value = true
    st.editorETag.value = ''
    st.previewMeta.value = { shown: 64, total: 200 }
    const { saveFile } = usePreview(deps)
    await saveFile()
    expect(requests).toEqual([])
  })

  it('PUTs a complete preview with If-Match', async () => {
    const st = sharedState()
    st.selectedPath.value = 'small.txt'
    st.editorContent.value = 'full'
    st.editorIsText.value = true
    st.editorETag.value = 'e0'
    st.previewMeta.value = { shown: 10, total: 10 }
    const { saveFile } = usePreview(deps)
    await saveFile()
    expect(requests).toHaveLength(1)
    expect(requests[0].headers['If-Match']).toBe('e0')
  })
})
