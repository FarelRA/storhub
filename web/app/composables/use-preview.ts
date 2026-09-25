import type { AnyEntry } from '~/utils/api-types'
import {
  PREVIEW_MAX_BYTES,
  SNIFF_BYTES,
  classify,
  extOf,
  isPreviewSaveable,
  kindFromExtension,
  mimeForKind,
  toHexDump,
} from '~/utils/preview'
import type { ApiResult } from './use-api'
import { useConsoleState } from './console-state'

export interface PreviewDeps {
  run: <T>(label: string, fn: () => Promise<T>, quiet?: boolean) => Promise<T | null>
  request: <T>(
    path: string,
    options?: RequestInit & { rawBody?: boolean; binary?: boolean },
  ) => Promise<ApiResult<T>>
  projectURL: (suffix: string) => string
  url: (path: string, params?: Record<string, string | undefined>) => string
  refreshAll: () => Promise<boolean>
  canEditFile: () => boolean
}

/** Drop any object URL and reset the preview panes. Standalone (no deps) so
 * the selection slice can clear previews without a dependency cycle. */
export function clearPreviewState(): void {
  const { previewUrl, previewHex, previewMeta } = useConsoleState()
  if (previewUrl.value) URL.revokeObjectURL(previewUrl.value)
  previewUrl.value = ''
  previewHex.value = ''
  previewMeta.value = { shown: 0, total: 0 }
}

/**
 * Preview pipeline slice of the console composable: ranged sniff
 * classification, media blob URLs, full-text reads, and the dual-gated save.
 */
export function usePreview(deps: PreviewDeps) {
  const {
    selectedPath,
    editorContent,
    editorDirty,
    editorETag,
    editorIsText,
    previewKind,
    previewUrl,
    previewHex,
    previewMeta,
    previewLoading,
  } = useConsoleState()
  const toasts = useToasts()

  // DUAL save gate: genuine text AND a CAS token AND a complete fetch.
  const isSaveable = computed(() =>
    isPreviewSaveable({
      editorIsText: editorIsText.value,
      etag: editorETag.value,
      shown: previewMeta.value.shown,
      total: previewMeta.value.total,
    }),
  )

  async function readFile(path: string): Promise<void> {
    previewLoading.value = true
    try {
      await deps.run('Read', async () => {
        // rawBody: the bytes are file content, not an API envelope. Parsing
        // and re-stringifying JSON here would corrupt the file on save.
        const result = await deps.request<string>(deps.url(deps.projectURL('/content'), { path }), {
          rawBody: true,
        })
        editorContent.value = result.payload
        editorETag.value = result.etag
        editorIsText.value = true
        editorDirty.value = false
        previewKind.value = 'text'
        clearPreviewState()
      })
    } finally {
      previewLoading.value = false
    }
  }

  /**
   * Decide how to preview the selected file and fetch only what that kind
   * needs: media types get a full blob (they cannot render partially), text
   * and binary get one ranged sniff window. Files above PREVIEW_MAX_BYTES
   * are never fetched - the range bar covers targeted reads.
   */
  async function loadPreview(entry: AnyEntry): Promise<void> {
    clearPreviewState()
    editorContent.value = ''
    editorDirty.value = false
    editorETag.value = ''
    editorIsText.value = false
    previewLoading.value = true
    try {
      const ext = extOf(entry.path)
      if (entry.size > PREVIEW_MAX_BYTES) {
        previewKind.value = 'too-large'
        previewMeta.value = { shown: 0, total: entry.size }
        return
      }
      const mediaKind = kindFromExtension(entry.path)
      if (mediaKind === 'image' || mediaKind === 'video' || mediaKind === 'audio' || mediaKind === 'pdf') {
        // Route through run() like every other fetch: an expired session
        // must take the shared 401 sign-out path, not a generic toast.
        const media = await deps.run('Preview', () =>
          deps.request<ArrayBuffer>(deps.url(deps.projectURL('/content'), { path: entry.path }), {
            binary: true,
          }),
        )
        if (media === null) {
          previewKind.value = 'error'
          return
        }
        const blob = new Blob([media.payload], { type: mimeForKind(mediaKind, ext) })
        previewUrl.value = URL.createObjectURL(blob)
        previewKind.value = mediaKind
        previewMeta.value = { shown: entry.size, total: entry.size }
        return
      }
      // Sniff window: one ranged request, then classify by magic / UTF-8.
      const windowLen = Math.min(entry.size, SNIFF_BYTES)
      const end = windowLen > 0 ? windowLen - 1 : 0
      const result = await deps.run('Preview', () =>
        deps.request<ArrayBuffer>(deps.url(deps.projectURL('/content'), { path: entry.path }), {
          binary: true,
          headers: { Range: `bytes=0-${end}` },
        }),
      )
      if (result === null) {
        // A failed sniff must not leave the previous kind behind: the pane
        // would render stale content against the new selection.
        previewKind.value = 'error'
        return
      }
      const bytes = new Uint8Array(result.payload)
      previewMeta.value = { shown: bytes.byteLength, total: entry.size }
      const kind = classify(bytes)
      previewKind.value = kind
      if (kind === 'text') {
        editorContent.value = new TextDecoder().decode(bytes)
        editorIsText.value = true
        // Only a complete fetch is safe to PUT back; a truncated sniff
        // window keeps no CAS token so the save gate stays honest about it.
        if (bytes.byteLength === entry.size) editorETag.value = result.etag
      } else if (kind === 'binary') {
        previewHex.value = toHexDump(bytes, { maxRows: 4096 })
      }
    } finally {
      previewLoading.value = false
    }
  }

  async function saveFile(): Promise<void> {
    // Only genuine text loads may be PUT back: saving over a file we merely
    // hex-dumped or never fetched would destroy data.
    const target = selectedPath.value
    if (!target || !deps.canEditFile() || !isSaveable.value) return
    const headers: Record<string, string> = {}
    if (editorETag.value) headers['If-Match'] = editorETag.value
    const res = await deps.run('Save', () =>
      deps.request<{ etag?: string }>(deps.url(deps.projectURL('/content'), { path: target }), {
        method: 'PUT',
        headers,
        body: editorContent.value,
      }),
    )
    if (res !== null) {
      editorDirty.value = false
      // The PUT answers with the fresh node; chain its ETag into the next save.
      editorETag.value = res.payload.etag ?? res.etag
      toasts.success(`Saved ${target}`)
      await deps.refreshAll()
    }
  }

  return { clearPreview: clearPreviewState, readFile, loadPreview, saveFile, isSaveable }
}
