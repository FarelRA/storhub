import { ApiError } from '~/utils/api-types'
import type { EntryInfo, ProjectStats, Principal, Revision, Share, XattrEntry } from '~/utils/api-types'
import { copyText } from '~/utils/clipboard'
import {
  PREVIEW_MAX_BYTES,
  SNIFF_BYTES,
  classify,
  kindFromExtension,
  mimeForKind,
  toHexDump,
} from '~/utils/preview'
import type { PreviewKind } from '~/utils/preview'

export type ModalKind =
  | 'mkdir'
  | 'create-file'
  | 'rename'
  | 'link'
  | 'symlink'
  | 'chmod'
  | 'chown'
  | 'utimes'
  | 'xattr-set'
  | 'xattr-remove'
  | 'append'
  | 'patch'
  | 'truncate'

export interface ModalForm {
  path: string
  newPath: string
  target: string
  mode: string
  uid: number
  gid: number
  atime: string
  mtime: string
  name: string
  value: string
  offset: number
  deleteSize: number
  text: string
}

const MODAL_TITLES: Record<ModalKind, string> = {
  mkdir: 'New directory',
  'create-file': 'New file',
  rename: 'Rename',
  link: 'New hard link',
  symlink: 'New symlink',
  chmod: 'Change mode',
  chown: 'Change owner',
  utimes: 'Update timestamps',
  'xattr-set': 'Set extended attribute',
  'xattr-remove': 'Remove extended attribute',
  append: 'Append text',
  patch: 'Patch bytes',
  truncate: 'Truncate file',
}

// ---- Module-scoped singletons (the console is a single-page tool) ----------

const project = ref('')
const currentPath = ref('')
const selectedPath = ref('')
const selectedEntry = ref<EntryInfo | null>(null)
const selectedPaths = ref<Set<string>>(new Set())
const lastSelected = ref<string | null>(null)
const entries = ref<EntryInfo[]>([])
const stats = ref<ProjectStats>({})
const shares = ref<Share[]>([])
const revisions = ref<Revision[]>([])
const xattrs = ref<XattrEntry[]>([])
const editorContent = ref('')
const editorDirty = ref(false)
// Preview pipeline: what the editor pane is currently showing and whether
// its content may be PUT back (only genuine text reads are saveable).
const previewKind = ref<PreviewKind>('text')
const previewUrl = ref('')
const previewHex = ref('')
const previewMeta = ref({ shown: 0, total: 0 })
const editorIsText = ref(true)
const previewLoading = ref(false)
const busy = ref(false)

const token = useState<string>('auth-token', () => '')
const principal = useState<Principal | null>('auth-principal', () => null)

const shareRequested = ref(false)
const shareToken = ref('')
const shareRootPath = ref('')
const shareDownloadAllowed = ref(true)

const modalOpen = ref(false)
const modalKind = ref<ModalKind>('mkdir')

// ---- Uploads ---------------------------------------------------------------
// Sequential (slow-network doctrine): one PUT at a time, parent directories
// ensured once per session, progress exposed for the pane UI.
interface UploadProgress {
  active: boolean
  done: number
  failed: number
  total: number
  current: string
  bytesDone: number
  bytesTotal: number
}

const uploadProgress = ref<UploadProgress>({
  active: false,
  done: 0,
  failed: 0,
  total: 0,
  current: '',
  bytesDone: 0,
  bytesTotal: 0,
})
const uploadedDirs = new Set<string>()
const modalForm = ref<ModalForm>(blankForm())
const modalError = ref('')

function blankForm(): ModalForm {
  return {
    path: '',
    newPath: '',
    target: '',
    mode: '0644',
    uid: 0,
    gid: 0,
    atime: '',
    mtime: '',
    name: '',
    value: '',
    offset: 0,
    deleteSize: 0,
    text: '',
  }
}

function enc(value: string): string {
  return encodeURIComponent(value)
}

export function useConsole() {
  const { config, url, getJSON, postJSON, request } = useApi()
  const toasts = useToasts()

  const authEnabled = computed(() => config.authEnabled !== false)
  const sharedMode = computed(() => !!shareToken.value)
  const isSharedView = computed(() => sharedMode.value || shareRequested.value)
  const isAdmin = computed(() => (!authEnabled.value ? true : principal.value?.admin === true))
  const canWrite = computed(() => !isSharedView.value)
  const canEditFile = computed(
    () => canWrite.value && !!selectedEntry.value && !selectedEntry.value.is_dir && !selectedEntry.value.is_symlink,
  )

  function projectURL(suffix: string): string {
    return `/projects/${enc(project.value)}${suffix}`
  }

  async function run<T>(label: string, fn: () => Promise<T>, quiet = false): Promise<T | null> {
    busy.value = true
    try {
      return await fn()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      // An expired session must not produce endless 401 toasts: sign out
      // once and tell the user exactly what happened.
      if (error instanceof ApiError && error.status === 401) {
        if (isSharedView.value) {
          toasts.error('This share link has expired or was revoked')
          return null
        }
        if (token.value) {
          logout()
          toasts.error('Session expired — please sign in again')
          return null
        }
      }
      if (!quiet) toasts.error(`${label}: ${message}`)
      return null
    } finally {
      busy.value = false
    }
  }

  async function del(target: string, label: string): Promise<boolean> {
    const ok = await run(label, () => request(target, { method: 'DELETE' }))
    return ok !== null
  }

  // ---- Loading -------------------------------------------------------------

  async function loadDirectory(path: string): Promise<void> {
    let nextPath = normalizePath(path)
    if (sharedMode.value && shareRootPath.value && !withinShareRoot(nextPath)) nextPath = shareRootPath.value
    currentPath.value = nextPath
    clearSelection()
    await run('Directory', async () => {
      const payload = await getJSON<{ entries?: EntryInfo[] }>(url(projectURL('/children'), { path: nextPath }))
      entries.value = payload.entries ?? []
    })
  }

  async function loadXattrs(): Promise<void> {
    if (!selectedPath.value) {
      xattrs.value = []
      return
    }
    const target = selectedPath.value
    await run('XAttrs', async () => {
      const payload = await getJSON<{ names?: string[] }>(url(projectURL('/xattrs'), { path: target }))
      xattrs.value = await Promise.all(
        (payload.names ?? []).map(async (name) => {
          try {
            const result = await request<string>(
              url(projectURL('/xattrs/value'), { path: target, name }),
            )
            return { name, value: typeof result.payload === 'string' ? result.payload : JSON.stringify(result.payload) }
          } catch {
            return { name, value: '[unavailable]' }
          }
        }),
      )
    }, true)
  }

  async function inspectPath(path: string): Promise<void> {
    await run('Stat', async () => {
      const payload = await getJSON<{ entry?: EntryInfo }>(url(projectURL('/nodes'), { path }))
      selectedEntry.value = payload.entry ?? null
      selectedPath.value = path
      editorDirty.value = false
    })
    await loadXattrs()
  }

  async function readFile(path: string, range?: { offset: number; length: number }): Promise<void> {
    const headers: Record<string, string> = {}
    let label = 'Read'
    if (range) {
      if (!Number.isInteger(range.offset) || range.offset < 0 || !Number.isInteger(range.length) || range.length < 1) {
        toasts.error('Range: offset must be ≥ 0 and length ≥ 1')
        return
      }
      headers.Range = `bytes=${range.offset}-${range.offset + range.length - 1}`
      label = 'Read range'
    }
    previewLoading.value = true
    try {
      await run(label, async () => {
        const result = await request<unknown>(url(projectURL('/content'), { path }), { headers })
        editorContent.value =
          typeof result.payload === 'string' ? result.payload : JSON.stringify(result.payload, null, 2)
        editorIsText.value = true
        previewKind.value = 'text'
        clearPreview()
      })
    } finally {
      previewLoading.value = false
    }
  }
  async function loadStats(): Promise<void> {
    if (sharedMode.value) {
      stats.value = {}
      return
    }
    await run('Stats', async () => {
      const payload = await getJSON<{ stats?: ProjectStats }>(projectURL(''))
      stats.value = payload.stats ?? {}
    })
  }

  async function loadRevisions(): Promise<void> {
    if (sharedMode.value || !project.value) {
      revisions.value = []
      return
    }
    await run('Revisions', async () => {
      const payload = await getJSON<{ revisions?: Revision[] }>(url(projectURL('/revisions')))
      revisions.value = payload.revisions ?? []
    })
  }

  async function loadShares(): Promise<void> {
    if (sharedMode.value || !project.value) {
      shares.value = []
      return
    }
    await run('Shares', async () => {
      const payload = await getJSON<{ shares?: Share[] }>(url(projectURL('/shares')))
      shares.value = payload.shares ?? []
    })
  }

  async function refreshAll(): Promise<void> {
    await Promise.all([loadStats(), loadDirectory(currentPath.value), loadRevisions(), loadShares()])
    if (selectedPath.value) await inspectPath(selectedPath.value)
  }

  // ---- Session & project ---------------------------------------------------

  async function login(username: string, password: string): Promise<boolean> {
    try {
      const payload = await postJSON<{ token: string; principal: Principal }>('/auth/login', {
        username,
        password,
      })
      token.value = payload.token
      principal.value = payload.principal
      sessionStorage.setItem('storhub.token', payload.token)
      sessionStorage.setItem('storhub.principal', JSON.stringify(payload.principal))
      toasts.success(`Signed in as ${payload.principal.username}`)
      if (lockedProject && project.value !== lockedProject) {
        await loadProject(lockedProject)
        return true
      }
      if (project.value) await refreshAll()
      return true
    } catch (error) {
      toasts.error(error instanceof Error ? error.message : 'Login failed')
      return false
    }
  }

  function logout(): void {
    if (sharedMode.value) return
    token.value = ''
    principal.value = null
    sessionStorage.removeItem('storhub.token')
    sessionStorage.removeItem('storhub.principal')
    toasts.info('Signed out')
  }

  function restoreSession(): void {
    const saved = sessionStorage.getItem('storhub.token')
    if (saved) token.value = saved
    // Restore identity too - otherwise a reload leaves the session valid but
    // the UI blind to who it belongs to (empty "Signed in as", lost admin).
    const savedPrincipal = sessionStorage.getItem('storhub.principal')
    if (savedPrincipal) {
      try {
        principal.value = JSON.parse(savedPrincipal) as Principal
      } catch {
        sessionStorage.removeItem('storhub.principal')
      }
    }
  }

  async function loadProject(name: string): Promise<boolean> {
    const trimmed = name.trim()
    if (!trimmed) {
      toasts.error('Enter a project name first')
      return false
    }
    project.value = trimmed
    currentPath.value = ''
    selectedPath.value = ''
    selectedPaths.value = new Set()
    lastSelected.value = null
    selectedEntry.value = null
    editorContent.value = ''
    editorDirty.value = false
    xattrs.value = []
    editorIsText.value = true
    clearPreview()
    const failed = (await run('Load project', refreshAll)) === null
    if (failed) {
      reset()
      return false
    }
    return true
  }

  async function bootstrapShare(shareParam: string): Promise<boolean> {
    shareRequested.value = true
    try {
      const payload = await getJSON<{
        id?: string
        token?: string
        project: string
        path: string
        download?: boolean
      }>(`/shares/${enc(shareParam)}`)
      // The signed token is the credential - never the short registry id
      // (which the server may echo alongside it).
      shareToken.value = payload.token ?? payload.id ?? shareParam
      token.value = shareToken.value
      project.value = payload.project
      shareRootPath.value = normalizePath(payload.path)
      shareDownloadAllowed.value = payload.download !== false
      const root = shareRootPath.value
      const statResult = await run('Shared resource', () =>
        getJSON<{ entry?: EntryInfo }>(url(projectURL('/nodes'), { path: root })),
      )
      const entry = statResult?.entry
      if (entry && !entry.is_dir) {
        currentPath.value = root
        entries.value = [entry]
        selectedPath.value = root
        selectedEntry.value = entry
        await readFile(root)
        await loadXattrs()
      } else {
        await loadDirectory(root)
      }
      return true
    } catch {
      shareToken.value = ''
      token.value = ''
      project.value = ''
      shareRootPath.value = ''
      toasts.error('This share link is invalid or has expired')
      return false
    }
  }

  function withinShareRoot(path: string): boolean {
    if (!sharedMode.value || !shareRootPath.value) return true
    const target = normalizePath(path)
    return target === shareRootPath.value || target.startsWith(`${shareRootPath.value}/`)
  }

  function goUp(): void {
    if (!currentPath.value) return
    void loadDirectory(parentPath(currentPath.value))
  }

  function clearPreview(): void {
    if (previewUrl.value) URL.revokeObjectURL(previewUrl.value)
    previewUrl.value = ''
    previewHex.value = ''
    previewMeta.value = { shown: 0, total: 0 }
  }

  /**
   * Decide how to preview the selected file and fetch only what that kind
   * needs: media types get a full blob (they cannot render partially), text
   * and binary get one ranged sniff window. Files above PREVIEW_MAX_BYTES
   * are never fetched - the range bar covers targeted reads.
   */
  async function loadPreview(entry: EntryInfo): Promise<void> {
    clearPreview()
    editorContent.value = ''
    editorDirty.value = false
    editorIsText.value = false
    previewLoading.value = true
    try {
      const ext = (entry.path.split('/').pop() ?? '').split('.').pop() ?? ''
      if (entry.size > PREVIEW_MAX_BYTES) {
        previewKind.value = 'too-large'
        previewMeta.value = { shown: 0, total: entry.size }
        return
      }
      const mediaKind = kindFromExtension(entry.path)
      if (mediaKind === 'image' || mediaKind === 'video' || mediaKind === 'audio' || mediaKind === 'pdf') {
        try {
          const result = await request<ArrayBuffer>(url(projectURL('/content'), { path: entry.path }), {
            binary: true,
          })
          const blob = new Blob([result.payload], { type: mimeForKind(mediaKind, ext) })
          previewUrl.value = URL.createObjectURL(blob)
          previewKind.value = mediaKind
          previewMeta.value = { shown: entry.size, total: entry.size }
        } catch (error) {
          previewKind.value = 'too-large'
          toasts.error(`Media preview failed: ${error instanceof Error ? error.message : String(error)}`)
        }
        return
      }
      // Sniff window: one ranged request, then classify by magic / UTF-8.
      const windowLen = Math.min(entry.size, SNIFF_BYTES)
      const end = windowLen > 0 ? windowLen - 1 : 0
      const result = await run('Preview', () =>
        request<ArrayBuffer>(url(projectURL('/content'), { path: entry.path }), {
          binary: true,
          headers: { Range: `bytes=0-${end}` },
        }),
      )
      if (result === null) return
      const bytes = new Uint8Array(result.payload)
      previewMeta.value = { shown: bytes.byteLength, total: entry.size }
      const kind = classify(bytes)
      previewKind.value = kind
      if (kind === 'text') {
        editorContent.value = new TextDecoder().decode(bytes)
        editorIsText.value = true
      } else if (kind === 'binary') {
        previewHex.value = toHexDump(bytes, { maxRows: 4096 })
      }
    } finally {
      previewLoading.value = false
    }
  }

  async function selectEntry(entry: EntryInfo): Promise<void> {
    await inspectPath(entry.path)
    const current = selectedEntry.value
    if (!current) return
    if (current.is_dir) {
      await loadDirectory(entry.path)
      return
    }
    if (current.is_symlink) return
    await loadPreview(entry)
  }

  // ---- Mutations -----------------------------------------------------------

  async function saveFile(): Promise<void> {
    // Only genuine text loads may be PUT back: saving over a file we merely
    // hex-dumped or never fetched would destroy data.
    if (!canEditFile.value || !editorIsText.value || !selectedPath.value) return
    const ok = await run('Save', () =>
      request(url(projectURL('/content'), { path: selectedPath.value! }), {
        method: 'PUT',
        body: editorContent.value,
      }),
    )
    if (ok !== null) {
      editorDirty.value = false
      toasts.success(`Saved ${selectedPath.value}`)
      await refreshAll()
    }
  }

  async function op(label: string, suffix: string, body: unknown): Promise<boolean> {
    const ok = await run(label, () => postJSON(projectURL(`/ops/${suffix}`), body))
    if (ok !== null) await refreshAll()
    return ok !== null
  }

  async function patchOp(label: string, params: Record<string, string>, body?: string): Promise<boolean> {
    const target = selectedPath.value
    const ok = await run(label, () =>
      request(url(projectURL('/content'), { path: target!, ...params }), { method: 'PATCH', body }),
    )
    if (ok !== null) {
      await readFile(target!)
      await inspectPath(target!)
      await loadRevisions()
    }
    return ok !== null
  }

  async function removeSelected(entry: EntryInfo): Promise<boolean> {
    // For directories, use recursive path via removeMany
    if (entry.is_dir) return removeMany([entry.path])
    const done = await op(`Remove ${entry.path}`, 'unlink', { path: entry.path })
    if (done) {
      selectedPaths.value.delete(entry.path)
      selectedPaths.value = new Set(selectedPaths.value)
      if (selectedPath.value === entry.path) clearSelection()
      if (selectedPaths.value.size === 0) clearSelection()
    }
    return done
  }

  async function removeRecursive(path: string): Promise<boolean> {
    // List children and delete them first (depth-first)
    try {
      const payload = await getJSON<{ entries?: EntryInfo[] }>(url(projectURL('/children'), { path }))
      const kids = payload.entries ?? []
      for (const kid of kids) {
        const ok = await removeRecursive(kid.path)
        if (!ok) return false
      }
    } catch {
      // If we can't list, try to unlink as file
      const res = await run(`Remove ${path}`, () => postJSON(projectURL('/ops/unlink'), { path }), true)
      return res !== null
    }
    // Now the directory should be empty, try rmdir; if it fails because it's a file, try unlink
    let res = await run(`Remove ${path}`, () => postJSON(projectURL('/ops/rmdir'), { path }), true)
    if (res !== null) return true
    res = await run(`Remove ${path}`, () => postJSON(projectURL('/ops/unlink'), { path }), true)
    return res !== null
  }

  async function removeMany(paths: string[]): Promise<boolean> {
    const results = await Promise.all(
      paths.map(async (p) => {
        const entry = entries.value.find((e) => e.path === p)
        // If we know it's a dir, use recursive; otherwise try recursive which handles both
        if (entry?.is_dir) return removeRecursive(p)
        // For files or unknown, try direct unlink, fallback to recursive
        const res = await run(`Remove ${p}`, () => postJSON(projectURL('/ops/unlink'), { path: p }), true)
        if (res !== null) return true
        return removeRecursive(p)
      }),
    )
    const ok = results.every(Boolean)
    if (!ok) toasts.error(`Failed to remove ${results.filter((v) => !v).length}/${paths.length} items`)
    for (const p of paths) selectedPaths.value.delete(p)
    selectedPaths.value = new Set(selectedPaths.value)
    if (selectedPaths.value.size === 0) clearSelection()
    else {
      const last = [...selectedPaths.value].pop()!
      selectedPath.value = last
      await inspectPath(last)
    }
    await refreshAll()
    return ok
  }

  function clearSelection(): void {
    selectedPath.value = ''
    selectedEntry.value = null
    selectedPaths.value = new Set()
    lastSelected.value = null
    editorContent.value = ''
    editorDirty.value = false
    xattrs.value = []
    editorIsText.value = true
    clearPreview()
  }

  function isSelected(path: string): boolean {
    return selectedPaths.value.has(path)
  }

  function selectSingle(path: string): void {
    selectedPaths.value = new Set([path])
    lastSelected.value = path
    selectedPath.value = path
  }

  function toggleSelect(path: string): void {
    const next = new Set(selectedPaths.value)
    if (next.has(path)) next.delete(path)
    else next.add(path)
    selectedPaths.value = next
    lastSelected.value = path
    if (next.size === 1) {
      selectedPath.value = [...next][0]!
    } else if (next.size === 0) {
      clearSelection()
      return
    } else {
      selectedPath.value = path
    }
  }

  function selectRange(from: string, to: string): void {
    const idxFrom = entries.value.findIndex((e) => e.path === from)
    const idxTo = entries.value.findIndex((e) => e.path === to)
    if (idxFrom === -1 || idxTo === -1) {
      selectSingle(to)
      return
    }
    const [a, b] = idxFrom < idxTo ? [idxFrom, idxTo] : [idxTo, idxFrom]
    const range = entries.value.slice(a, b + 1).map((e) => e.path)
    selectedPaths.value = new Set(range)
    lastSelected.value = to
    selectedPath.value = to
  }

  function selectAll(): void {
    selectedPaths.value = new Set(entries.value.map((e) => e.path))
    if (entries.value.length) {
      lastSelected.value = entries.value[entries.value.length - 1]!.path
      selectedPath.value = lastSelected.value!
    }
  }

  async function deleteProject(): Promise<boolean> {
    const done = await del(url(`/projects/${enc(project.value)}`), 'Delete project')
    if (done) {
      reset()
      toasts.success('Project deleted')
    }
    return done
  }

  async function rollbackRevision(sha: string): Promise<boolean> {
    return op(`Rollback to ${sha.slice(0, 10)}`, 'rollback', { commit_sha: sha })
  }

  async function purgeUntracked(): Promise<boolean> {
    return op('Purge untracked', 'purge', {})
  }

  async function createShare(path: string, download: boolean, expiresInSeconds?: number): Promise<Share | null> {
    return run('Create share', async () => {
      const body: Record<string, unknown> = { path, download }
      if (expiresInSeconds) body.expires_in_seconds = expiresInSeconds
      const payload = await postJSON<Share>(projectURL('/shares'), body)
      await loadShares()
      return payload
    })
  }

  async function deleteShare(share: Share): Promise<boolean> {
    const done = await del(url(projectURL(`/shares/${enc(share.id)}`)), 'Delete share')
    if (done) await loadShares()
    return done
  }

  async function setXattr(path: string, name: string, value: string): Promise<boolean> {
    const ok = await run('Set xattr', () =>
      request(url(projectURL('/xattrs/value'), { path, name }), { method: 'PUT', body: value }),
    )
    if (ok !== null) await loadXattrs()
    return ok !== null
  }

  async function removeXattr(path: string, name: string): Promise<boolean> {
    const ok = await run('Remove xattr', () =>
      request(url(projectURL('/xattrs/value'), { path, name }), { method: 'DELETE' }),
    )
    if (ok !== null) await loadXattrs()
    return ok !== null
  }

  function reset(): void {
    project.value = ''
    currentPath.value = ''
    selectedPath.value = ''
    selectedPaths.value = new Set()
    lastSelected.value = null
    selectedEntry.value = null
    entries.value = []
    stats.value = {}
    shares.value = []
    revisions.value = []
    xattrs.value = []
    editorContent.value = ''
    editorDirty.value = false
    editorIsText.value = true
    uploadedDirs.clear()
    clearPreview()
  }

  /**
   * Native browser download: mint a short-lived signed URL server-side, then
   * hand the plain link to the browser - its download manager owns streaming,
   * resume, and disk I/O. No JS buffering, no size cap. The same URL works
   * with curl/wget.
   */
  async function downloadEntry(entry: EntryInfo): Promise<void> {
    const share = await createShare(entry.path, true, 300)
    if (!share?.download_url) {
      toasts.error('Failed to create download link')
      return
    }
    const anchor = document.createElement('a')
    anchor.href = share.download_url
    anchor.rel = 'noopener'
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
  }

  async function copyDirectLink(entry: EntryInfo): Promise<void> {
    const share = await createShare(entry.path, true, 300)
    if (!share?.download_url) {
      toasts.error('Failed to create direct link')
      return
    }
    const ok = await copyText(share.download_url)
    if (ok) toasts.success('Direct link copied (valid 5 min)')
  }

  /** Select an entry non-navigatively: stat it so detail panes follow along. */
  async function focusEntry(entry: EntryInfo): Promise<void> {
    await inspectPath(entry.path)
  }

  // ---- Uploads ---------------------------------------------------------------

  /** mkdir -p against the REST API, memoized per project session. */
  async function ensureDir(dir: string): Promise<boolean> {
    const parts = normalizePath(dir).split('/').filter(Boolean)
    let cur = ''
    for (const part of parts) {
      cur = cur ? `${cur}/${part}` : part
      const key = `${project.value}/${cur}`
      if (uploadedDirs.has(key)) continue
      try {
        await postJSON(projectURL('/ops/mkdir'), { path: cur })
      } catch (error) {
        if (error instanceof ApiError && error.status === 409) {
          // Already exists — treat as success for mkdir -p
        } else {
          return false
        }
      }
      uploadedDirs.add(key)
    }
    return true
  }

  /**
   * Upload one file with byte-level progress via XHR (its upload.onprogress
   * is the only browser API reporting real bytes while keeping an explicit
   * Content-Length, which the server's size enforcement requires).
   */
  function putFileWithProgress(fullPath: string, file: File, onBytes: (loaded: number) => void): Promise<void> {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest()
      xhr.open('PUT', url(projectURL('/content'), { path: fullPath }))
      if (token.value) xhr.setRequestHeader('Authorization', `Bearer ${token.value}`)
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) onBytes(event.loaded)
      }
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve()
          return
        }
        let message = `HTTP ${xhr.status}`
        try {
          const parsed = JSON.parse(xhr.responseText) as { error?: { message?: string } }
          message = parsed.error?.message ?? message
        } catch {
          /* non-JSON error body */
        }
        reject(new ApiError(xhr.status, message, null))
      }
      xhr.onerror = () => reject(new ApiError(0, 'network error during upload', null))
      xhr.send(file)
    })
  }

  /**
   * Upload a batch of files into baseDir. Each item carries its relative
   * path (from drag-and-drop traversal or webkitRelativePath), so dropped
   * folders land with their structure intact. Byte progress is cumulative
   * across the whole batch.
   */
  async function uploadFiles(items: Array<{ file: File; relPath: string }>, baseDir: string): Promise<void> {
    if (!items.length || !canWrite.value) return
    const bytesTotal = items.reduce((sum, item) => sum + item.file.size, 0)
    uploadProgress.value = {
      active: true,
      done: 0,
      failed: 0,
      total: items.length,
      current: '',
      bytesDone: 0,
      bytesTotal,
    }
    let firstError = ''
    let baseBytes = 0
    for (const { file, relPath } of items) {
      const cleanRel = normalizePath(relPath)
      if (!cleanRel) continue
      const fullPath = normalizePath(`${normalizePath(baseDir)}/${cleanRel}`)
      uploadProgress.value = { ...uploadProgress.value, current: cleanRel }
      const dir = parentPath(cleanRel)
      if (dir && !(await ensureDir(`${normalizePath(baseDir)}/${dir}`))) {
        firstError ||= `could not create ${dir}`
        baseBytes += file.size
        uploadProgress.value = {
          ...uploadProgress.value,
          bytesDone: baseBytes,
          failed: uploadProgress.value.failed + 1,
          done: uploadProgress.value.done + 1,
        }
        continue
      }
      try {
        await putFileWithProgress(fullPath, file, (loaded) => {
          uploadProgress.value = { ...uploadProgress.value, bytesDone: baseBytes + loaded }
        })
        baseBytes += file.size
      } catch (error) {
        firstError ||= error instanceof Error ? error.message : String(error)
        uploadProgress.value = { ...uploadProgress.value, failed: uploadProgress.value.failed + 1 }
      }
      uploadProgress.value = {
        ...uploadProgress.value,
        bytesDone: baseBytes,
        done: uploadProgress.value.done + 1,
      }
    }
    uploadProgress.value = { ...uploadProgress.value, active: false, current: '' }
    await refreshAll()
    const { done, failed, total } = uploadProgress.value
    if (failed === 0) toasts.success(`Uploaded ${done}/${total} · ${formatBytes(bytesTotal)}`)
    else toasts.error(`Uploaded ${done - failed}/${total} — ${firstError}`)
  }

  // ---- Modal ---------------------------------------------------------------

  function openModal(kind: ModalKind, contextDir?: string, targetPath?: string): void {
    const form = blankForm()
    form.path = contextDir !== undefined
      ? `${normalizePath(contextDir)}/`
      : currentPath.value ? `${currentPath.value}/` : ''
    // Symlinks: the clicked entry is the TARGET; the link is born in contextDir.
    if (kind === 'symlink') {
      form.target = targetPath ?? selectedPath.value ?? ''
      form.newPath = `${normalizePath(contextDir || currentPath.value)}/`
    } else {
      form.newPath = selectedPath.value ?? ''
    }
    if (selectedEntry.value?.mode !== undefined) form.mode = formatMode(selectedEntry.value.mode)
    form.uid = selectedEntry.value?.uid ?? 0
    form.gid = selectedEntry.value?.gid ?? 0
    form.atime = toDatetimeLocal(selectedEntry.value?.accessed_at ?? selectedEntry.value?.modified_at)
    form.mtime = toDatetimeLocal(selectedEntry.value?.modified_at)
    form.name = xattrs.value[0]?.name ?? ''
    modalKind.value = kind
    modalForm.value = form
    modalError.value = ''
    modalOpen.value = true
  }

  function closeModal(): void {
    modalOpen.value = false
    modalError.value = ''
  }

  async function submitModal(): Promise<void> {
    const f = modalForm.value
    const kind = modalKind.value
    try {
      switch (kind) {
        case 'mkdir':
          await op('mkdir', 'mkdir', { path: f.path })
          break
        case 'create-file':
          await op('create file', 'create-file', { path: f.path })
          break
        case 'rename':
          await op('rename', 'rename', { old_path: selectedPath.value, new_path: f.newPath })
          break
        case 'link':
          await op('hard link', 'link', { existing_path: selectedPath.value, new_path: f.newPath })
          break
        case 'symlink':
          await op('symlink', 'symlink', { target: f.target, link_path: f.newPath })
          break
        case 'chmod': {
          const mode = parseInt(f.mode, 8)
          if (Number.isNaN(mode)) throw new Error(`invalid octal mode: ${f.mode}`)
          await op('chmod', 'chmod', { path: selectedPath.value, mode })
          break
        }
        case 'chown':
          await op('chown', 'chown', { path: selectedPath.value, uid: Number(f.uid), gid: Number(f.gid) })
          break
        case 'utimes': {
          const atime = f.atime ? new Date(f.atime) : null
          const mtime = f.mtime ? new Date(f.mtime) : null
          for (const [name, date] of [['atime', atime], ['mtime', mtime]] as const) {
            if (date && Number.isNaN(date.getTime())) throw new Error(`invalid ${name} timestamp`)
          }
          await op('timestamps', 'utimes', {
            path: selectedPath.value,
            atime: (atime ?? new Date()).toISOString(),
            mtime: (mtime ?? new Date()).toISOString(),
          })
          break
        }
        case 'xattr-set':
          if (!f.name.trim()) throw new Error('attribute name is required')
          await setXattr(selectedPath.value!, f.name.trim(), f.value)
          break
        case 'xattr-remove':
          if (!f.name.trim()) throw new Error('attribute name is required')
          await removeXattr(selectedPath.value!, f.name.trim())
          break
        case 'append':
          await patchOp('Append text', { op: 'append' }, f.text)
          break
        case 'patch': {
          if (!Number.isInteger(f.offset) || f.offset < 0) throw new Error('offset must be a non-negative integer')
          if (!Number.isInteger(f.deleteSize) || f.deleteSize < 0) throw new Error('delete size must be ≥ 0')
          await patchOp(
            'Patch bytes',
            { op: 'patch', offset: String(f.offset), delete_size: String(f.deleteSize) },
            f.text,
          )
          break
        }
        case 'truncate': {
          if (!Number.isInteger(Number(f.text)) || Number(f.text) < 0)
            throw new Error('size must be a non-negative integer')
          await patchOp('Truncate', { op: 'truncate', size: f.text })
          break
        }
      }
      closeModal()
    } catch (error) {
      modalError.value = error instanceof Error ? error.message : String(error)
    }
  }

  // Pinned project (`storhub serve <project>`): auto-loaded, selector hidden.
  const lockedProject = config.project ?? ''

  async function init(): Promise<void> {
    restoreSession()
    const shareParam = new URLSearchParams(window.location.search).get('share')
    if (shareParam) {
      await bootstrapShare(shareParam)
      return
    }
    // A pinned project must never fire unauthenticated requests: defer the
    // auto-load until login succeeds (login() picks it up).
    if (lockedProject) {
      if (!authEnabled.value || token.value) await loadProject(lockedProject)
      return
    }
  }

  return {
    lockedProject,
    init,
    // state
    project,
    currentPath,
    selectedPath,
    selectedPaths,
    lastSelected,
    selectedEntry,
    entries,
    stats,
    shares,
    revisions,
    xattrs,
    editorContent,
    editorDirty,
    busy,
    token,
    principal,
    authEnabled,
    isAdmin,
    isSharedView,
    sharedMode,
    shareToken,
    shareRootPath,
    shareDownloadAllowed,
    canWrite,
    canEditFile,

    // modal
    modalOpen,
    modalKind,
    modalTitle: (kind: ModalKind) => MODAL_TITLES[kind],
    modalForm,
    modalError,
    openModal,
    closeModal,
    submitModal,
    // preview
    previewKind,
    previewUrl,
    previewHex,
    previewMeta,
    editorIsText,
    uploadProgress,
    previewLoading,
    loadPreview,
    clearPreview,
    // actions
    loadProject,
    loadDirectory,
    selectEntry,
    inspectPath,
    readFile,
    saveFile,
    refreshAll,
    login,
    logout,
    restoreSession,
    bootstrapShare,
    goUp,
    removeSelected,
    removeMany,
    clearSelection,
    isSelected,
    selectSingle,
    toggleSelect,
    selectRange,
    selectAll,
    deleteProject,
    rollbackRevision,
    purgeUntracked,
    downloadEntry,
    copyDirectLink,
    focusEntry,
    createShare,
    deleteShare,
    loadXattrs,
    uploadFiles,
    setXattr,
    removeXattr,
  }
}
