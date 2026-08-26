import type { EntryInfo, ProjectStats, Principal, Revision, Share, XattrEntry } from '~/utils/api-types'

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
const entries = ref<EntryInfo[]>([])
const stats = ref<ProjectStats>({})
const shares = ref<Share[]>([])
const revisions = ref<Revision[]>([])
const xattrs = ref<XattrEntry[]>([])
const editorContent = ref('')
const editorDirty = ref(false)
const busy = ref(false)

const token = useState<string>('auth-token', () => '')
const principal = useState<Principal | null>('auth-principal', () => null)

const shareRequested = ref(false)
const shareToken = ref('')
const shareRootPath = ref('')
const shareDownloadAllowed = ref(true)

const modalOpen = ref(false)
const modalKind = ref<ModalKind>('mkdir')
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
  const canReadRanges = computed(() => {
    const entry = selectedEntry.value
    return !!entry && !entry.is_dir && !entry.is_symlink
  })

  function projectURL(suffix: string): string {
    return `/projects/${enc(project.value)}${suffix}`
  }

  async function run<T>(label: string, fn: () => Promise<T>, quiet = false): Promise<T | null> {
    busy.value = true
    try {
      return await fn()
    } catch (error) {
      if (!quiet) toasts.error(`${label}: ${error instanceof Error ? error.message : String(error)}`)
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
    await run(label, async () => {
      const result = await request<unknown>(url(projectURL('/content'), { path }), { headers })
      editorContent.value =
        typeof result.payload === 'string' ? result.payload : JSON.stringify(result.payload, null, 2)
    })
  }

  async function loadStats(): Promise<void> {
    if (sharedMode.value) {
      stats.value = {}
      return
    }
    await run('Stats', async () => {
      stats.value = await getJSON<ProjectStats>(projectURL(''))
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
      toasts.success(`Signed in as ${payload.principal.username}`)
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
    toasts.info('Signed out')
  }

  function restoreSession(): void {
    const saved = sessionStorage.getItem('storhub.token')
    if (saved) token.value = saved
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
    selectedEntry.value = null
    editorContent.value = ''
    editorDirty.value = false
    xattrs.value = []
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
      shareToken.value = payload.id ?? payload.token ?? shareParam
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

  async function selectEntry(entry: EntryInfo): Promise<void> {
    await inspectPath(entry.path)
    const current = selectedEntry.value
    if (!current) return
    if (current.is_dir) {
      await loadDirectory(entry.path)
      return
    }
    if (!current.is_symlink) await readFile(entry.path)
  }

  // ---- Mutations -----------------------------------------------------------

  async function saveFile(): Promise<void> {
    if (!canEditFile.value || !selectedPath.value) return
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
    const done = await op(`Remove ${entry.path}`, entry.is_dir ? 'rmdir' : 'unlink', { path: entry.path })
    if (done && selectedPath.value === entry.path) clearSelection()
    return done
  }

  function clearSelection(): void {
    selectedPath.value = ''
    selectedEntry.value = null
    editorContent.value = ''
    editorDirty.value = false
    xattrs.value = []
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

  async function createShare(path: string, download: boolean): Promise<Share | null> {
    return run('Create share', async () => {
      const payload = await postJSON<Share>(projectURL('/shares'), { path, download })
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
    selectedEntry.value = null
    entries.value = []
    stats.value = {}
    shares.value = []
    revisions.value = []
    xattrs.value = []
    editorContent.value = ''
    editorDirty.value = false
  }

  // ---- Modal ---------------------------------------------------------------

  function openModal(kind: ModalKind): void {
    const form = blankForm()
    form.path = currentPath.value ? `${currentPath.value}/` : ''
    form.newPath = selectedPath.value ?? ''
    form.target = selectedPath.value ?? ''
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

  return {
    // state
    project,
    currentPath,
    selectedPath,
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
    canReadRanges,
    // modal
    modalOpen,
    modalKind,
    modalTitle: (kind: ModalKind) => MODAL_TITLES[kind],
    modalForm,
    modalError,
    openModal,
    closeModal,
    submitModal,
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
    deleteProject,
    rollbackRevision,
    purgeUntracked,
    createShare,
    deleteShare,
    loadXattrs,
    setXattr,
    removeXattr,
  }
}
