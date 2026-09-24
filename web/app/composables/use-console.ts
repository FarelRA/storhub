import { ApiError } from '~/utils/api-types'
import type {
  AnyEntry,
  DirEntry,
  EntryInfo,
  Principal,
  ProjectStats,
  PruneResult,
  StatusResult,
  Revision,
  Share,
} from '~/utils/api-types'
import { copyText } from '~/utils/clipboard'
import { canDownloadWithoutToken } from '~/utils/download'
import { blankForm } from '~/utils/modal-kinds'
import { directLink, SHARE_TTL_5M, SHARE_TTL_5M_LABEL } from '~/utils/share-links'
import { sharedState } from './console-state'
import { useSelection } from './use-selection'
import { usePreview } from './use-preview'
import { useUploads, abortUploads, clearUploadCache } from './use-uploads'
import type { UploadItem } from './use-uploads'
import { useModals } from './use-modals'
import { useDeleteService } from './use-delete'

// Reference count of in-flight run() calls: busy must stay true until the
// LAST sibling request settles, not the first.
let inflight = 0

// Generation guard: a slow response for an older selection must never
// overwrite a newer one.
let inspectSeq = 0

// Generation guard for directory listings: concurrent refreshAll calls (nav
// vs refresh) resolve last-writer-wins without this, repainting an older
// listing over a newer one.
let directorySeq = 0

function encodeSegment(value: string): string {
  return encodeURIComponent(value)
}

export function useConsole() {
  const { config, url, getJSON, postJSON, request } = useApi()
  const toasts = useToasts()
  const {
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
    xattrsError,
    editorContent,
    editorDirty,
    editorETag,
    busy,
    token,
    principal,
    shareRequested,
    shareToken,
    shareRootPath,
    shareId,
    previewKind,
    previewUrl,
    previewHex,
    previewMeta,
    editorIsText,
    previewLoading,
    uploadProgress,
    modalOpen,
    modalKind,
    modalForm,
    modalError,
  } = sharedState()

  const authEnabled = computed(() => config.authEnabled !== false)
  const sharedMode = computed(() => !!shareToken.value)
  const isSharedView = computed(() => sharedMode.value || shareRequested.value)
  const isAdmin = computed(() => (!authEnabled.value ? true : principal.value?.admin === true))
  const canWrite = computed(() => !isSharedView.value)
  const canEditFile = computed(
    () => canWrite.value && !!selectedEntry.value && !selectedEntry.value.is_dir && !selectedEntry.value.is_symlink,
  )

  function projectURL(suffix: string): string {
    return `/projects/${encodeSegment(project.value)}${suffix}`
  }

  async function run<T>(label: string, fn: () => Promise<T>, quiet = false): Promise<T | null> {
    busy.value = true
    inflight += 1
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
          toasts.error('Session expired. Please sign in again.')
          return null
        }
      }
      if (!quiet) toasts.error(`${label}: ${message}`)
      return null
    } finally {
      inflight = Math.max(0, inflight - 1)
      busy.value = inflight > 0
    }
  }

  async function deleteResource(target: string, label: string): Promise<boolean> {
    const ok = await run(label, () => request(target, { method: 'DELETE' }))
    return ok !== null
  }

  // ---- Slices (console-state split; this module stays a thin facade) -----

  const selection = useSelection()
  const preview = usePreview({
    run,
    request,
    projectURL,
    url,
    refreshAll,
    canEditFile: () => canEditFile.value,
  })
  const uploads = useUploads({
    postJSON,
    projectURL,
    url,
    canWrite: () => canWrite.value,
    refreshAll,
  })

  async function postOp(label: string, suffix: string, body: unknown): Promise<boolean> {
    const ok = await run(label, () => postJSON(projectURL(`/ops/${suffix}`), body))
    if (ok !== null) await refreshAll()
    return ok !== null
  }

  async function patchOp(label: string, params: Record<string, string>, body?: string): Promise<boolean> {
    const target = selectedPath.value
    if (!target) return false
    const ok = await run(label, () =>
      request(url(projectURL('/content'), { path: target, ...params }), { method: 'PATCH', body }),
    )
    if (ok !== null) {
      await preview.readFile(target)
      await inspectPath(target)
      await loadRevisions()
    }
    return ok !== null
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

  const modals = useModals({
    run,
    postJSON,
    projectURL,
    postOp,
    patchOp,
    setXattr,
    removeXattr,
    refreshAll,
    clearSelection: selection.clearSelection,
  })
  const remover = useDeleteService({
    run,
    getJSON,
    postJSON,
    projectURL,
    url,
    refreshAll,
    clearSelection: selection.clearSelection,
  })

  // ---- Loading -------------------------------------------------------------

  async function loadDirectory(path: string): Promise<boolean> {
    const seq = ++directorySeq
    let nextPath = normalizePath(path)
    if (sharedMode.value && shareRootPath.value && !withinShareRoot(nextPath)) nextPath = shareRootPath.value
    // Navigating away abandons the old location: abort its uploads and drop
    // its selection so the panes cannot act on the previous directory.
    if (nextPath !== currentPath.value) {
      abortUploads()
      selection.clearSelection()
    }
    currentPath.value = nextPath
    // Never leave the previous directory's rows under the new path: a failed
    // fetch must show an empty pane, not stale entries the user could act on.
    entries.value = []
    const ok = await run('Directory', async () => {
      const payload = await getJSON<{ entries?: DirEntry[] }>(url(projectURL('/children'), { path: nextPath }))
      // A superseded listing resolves after its replacement: drop it instead
      // of repainting older rows over the newer directory.
      if (seq !== directorySeq) return false
      entries.value = payload.entries ?? []
      return true
    })
    return ok === true
  }

  async function loadXattrs(): Promise<void> {
    if (!selectedPath.value) {
      xattrs.value = []
      xattrsError.value = false
      return
    }
    const target = selectedPath.value
    xattrs.value = []
    xattrsError.value = false
    const ok = await run('XAttrs', async () => {
      const payload = await getJSON<{ names?: string[] }>(url(projectURL('/xattrs'), { path: target }))
      const resolved = await Promise.all(
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
      // Drop the result if the selection moved on while these requests ran.
      if (selectedPath.value === target) xattrs.value = resolved
    }, true)
    // The outer list runs quiet, so a total failure would otherwise print
    // "No attributes on this entry", indistinguishable from genuinely empty.
    if (ok === null && selectedPath.value === target) xattrsError.value = true
  }

  async function inspectPath(path: string): Promise<void> {
    const seq = ++inspectSeq
    const ok = await run('Stat', async () => {
      const payload = await getJSON<{ entry?: EntryInfo }>(url(projectURL('/nodes'), { path }))
      return payload.entry ?? null
    })
    if (seq !== inspectSeq) return
    if (ok === null) {
      // A failed stat must not leave the previous entry behind: selectEntry
      // would then open/preview that stale entry (the wrong file).
      selectedEntry.value = null
      return
    }
    selectedEntry.value = ok
    selectedPath.value = path
    editorDirty.value = false
    await loadXattrs()
  }

  async function loadStats(): Promise<boolean> {
    if (sharedMode.value) {
      // For shared view, stats are not available via /projects/{p} (read-only).
      // Hide the grid instead of showing dashes.
      stats.value = {}
      return true
    }
    return (
      (await run('Stats', async () => {
        const payload = await getJSON<{ stats?: ProjectStats }>(projectURL(''))
        stats.value = payload.stats ?? {}
      })) !== null
    )
  }

  async function loadRevisions(): Promise<boolean> {
    if (sharedMode.value || !project.value) {
      revisions.value = []
      return true
    }
    return (
      (await run('Revisions', async () => {
        const payload = await getJSON<{ revisions?: Revision[] }>(url(projectURL('/revisions')))
        revisions.value = payload.revisions ?? []
      })) !== null
    )
  }

  async function loadShares(): Promise<boolean> {
    if (sharedMode.value || !project.value) {
      shares.value = []
      return true
    }
    return (
      (await run('Shares', async () => {
        const payload = await getJSON<{ shares?: Share[] }>(url(projectURL('/shares')))
        shares.value = payload.shares ?? []
      })) !== null
    )
  }

  async function refreshAll(): Promise<boolean> {
    // Capture the selection: loadDirectory keeps it for same-path refreshes,
    // and inspectPath below re-reads the entry so the panes stay in sync.
    const keep = selectedPath.value
    const results = await Promise.all([loadStats(), loadDirectory(currentPath.value), loadRevisions(), loadShares()])
    if (keep) await inspectPath(keep)
    return results.every(Boolean)
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
    // Drop every trace of the last project: on a shared machine the drawer
    // must not keep listing its files and shares after sign-out.
    reset()
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
    // A project switch abandons the old location: stop its uploads before
    // the first listing for the new project fires.
    abortUploads()
    project.value = trimmed
    currentPath.value = ''
    selectedPath.value = ''
    selectedPaths.value = new Set()
    lastSelected.value = null
    selectedEntry.value = null
    editorContent.value = ''
    editorDirty.value = false
    editorETag.value = ''
    xattrs.value = []
    editorIsText.value = true
    preview.clearPreview()
    // refreshAll reports whether the underlying loads succeeded; a typo'd
    // project must not leave the console parked on an empty phantom.
    if (!(await refreshAll())) {
      reset()
      return false
    }
    return true
  }

  async function bootstrapShare(shareParam: string): Promise<boolean> {
    shareRequested.value = true
    // bootstrapShare overwrites the bearer with the share token below; a
    // dead link must restore the prior session, not strand it hijacked.
    const priorToken = token.value
    const priorPrincipal = principal.value
    try {
      const payload = await getJSON<{
        id?: string
        token?: string
        project: string
        path: string
        expires_at?: string
        is_dir?: boolean
      }>(`/shares/${encodeSegment(shareParam)}`)
      // The signed token is the credential - never the short registry id
      // (which the server may echo alongside it).
      shareToken.value = payload.token ?? payload.id ?? shareParam
      token.value = shareToken.value
      project.value = payload.project
      shareRootPath.value = normalizePath(payload.path)
      shareId.value = payload.id ?? ''
      // Try to extract jti from JWT if id not in payload.
      if (!shareId.value && shareToken.value.includes('.')) {
        try {
          const body = JSON.parse(atob(shareToken.value.split('.')[1] ?? '')) as { jti?: string; id?: string }
          shareId.value = body.jti ?? body.id ?? ''
        } catch {
          void 0
        }
      }
      const root = shareRootPath.value
      const statResult = await run('Shared resource', () =>
        getJSON<{ entry?: EntryInfo }>(url(projectURL('/nodes'), { path: root })),
      )
      const entry = statResult?.entry
      if (entry && !entry.is_dir) {
        // Route single-file shares through the preview pipeline: media
        // kinds render natively and oversized files hit the size cap
        // instead of buffering the whole body as garbled text.
        currentPath.value = root
        entries.value = [{ ...entry, name: entry.path.split('/').pop() ?? entry.path }]
        const first = entries.value[0]
        if (first) await selectEntry(first)
      } else {
        await loadDirectory(root)
      }
      return true
    } catch {
      // A dead link must not strand the UI in shared mode: reset() drops
      // every piece of console state including the share fields, then the
      // prior bearer is restored so the login card and project input come
      // back.
      reset()
      token.value = priorToken
      principal.value = priorPrincipal
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

  async function selectEntry(entry: AnyEntry): Promise<void> {
    await inspectPath(entry.path)
    const current = selectedEntry.value
    if (!current) return
    if (current.is_dir) {
      await loadDirectory(entry.path)
      return
    }
    if (current.is_symlink) return
    await preview.loadPreview(entry)
  }

  /** Select an entry non-navigatively: stat it so detail panes follow along. */
  async function focusEntry(entry: AnyEntry): Promise<void> {
    await inspectPath(entry.path)
  }

  async function deleteProject(): Promise<boolean> {
    const done = await deleteResource(url(`/projects/${encodeSegment(project.value)}`), 'Delete project')
    if (done) {
      reset()
      toasts.success('Project deleted')
    }
    return done
  }

  async function rollbackRevision(sha: string): Promise<boolean> {
    return postOp(`Rollback to ${sha.slice(0, 10)}`, 'rollback', { commit_sha: sha })
  }

  // Granular prune: reclaim orphaned index objects, untracked assets,
  // (git backend) collapsed history, or live-catalog chunk orphans.
  // Returns the typed result for display.
  async function prune(scope = 'assets', keep = 0, dryRun = false): Promise<PruneResult | null> {
    return run(`Prune ${scope}`, async () => {
      const payload = await postJSON<PruneResult>(projectURL('/ops/prune'), {
        scope,
        keep,
        dry_run: dryRun,
      })
      if (!dryRun) await refreshAll()
      return payload
    })
  }

  // Project health: degraded latch, streaks, and pressure totals.
  async function projectStatus(): Promise<StatusResult | null> {
    return run('Project status', async () => {
      const payload = await getJSON<StatusResult>(projectURL('/ops/status'))
      return payload
    })
  }

  // Clear the degraded latch. The only path back to healthy.
  async function enableProject(): Promise<boolean> {
    const result = await run('Enable project', async () => {
      const payload = await postJSON<{ status: string }>(projectURL('/ops/enable'), {})
      await refreshAll()
      return payload
    })
    return result !== null
  }

  // Revert a single path (file or directory subtree) to a historical revision,
  // leaving the rest of the tree untouched. A revert is a new commit.
  async function revertPath(path: string, sha: string): Promise<boolean> {
    return postOp(`Revert ${path}`, 'revert', { path, commit_sha: sha })
  }

  async function createShare(path: string, expiresInSeconds?: number): Promise<Share | null> {
    return run('Create share', async () => {
      const body: Record<string, unknown> = { path }
      if (expiresInSeconds) body.expires_in_seconds = expiresInSeconds
      const payload = await postJSON<Share>(projectURL('/shares'), body)
      await loadShares()
      return payload
    })
  }

  async function deriveShare(path: string): Promise<Share | null> {
    if (!shareId.value || !shareToken.value) return null
    return run('Derive share', async () => {
      const body: Record<string, unknown> = { path }
      const payload = await postJSON<Share>(`/shares/${encodeSegment(shareId.value)}/derive?token=${encodeSegment(shareToken.value)}`, body)
      return payload as Share
    })
  }

  async function deleteShare(share: Share): Promise<boolean> {
    const done = await deleteResource(url(projectURL(`/shares/${encodeSegment(share.id)}`)), 'Delete share')
    if (done) await loadShares()
    return done
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
    xattrsError.value = false
    editorContent.value = ''
    editorDirty.value = false
    editorETag.value = ''
    editorIsText.value = true
    previewLoading.value = false
    uploadProgress.value = {
      active: false,
      done: 0,
      failed: 0,
      total: 0,
      current: '',
      bytesDone: 0,
      bytesTotal: 0,
    }
    // Share, modal, and progress state live here too: every sign-out,
    // project switch, and dead-link recovery funnels through reset, so no
    // caller needs ad-hoc cleanup for these fields afterwards.
    shareRequested.value = false
    shareToken.value = ''
    shareRootPath.value = ''
    shareId.value = ''
    modalOpen.value = false
    modalKind.value = 'mkdir'
    modalForm.value = blankForm()
    modalError.value = ''
    clearUploadCache()
    preview.clearPreview()
  }

  // Downloads fetch the bytes with the bearer header (the REST layer
  // rejects auth tokens on the query string) and hand them to the browser
  // through a same-origin blob URL + [download], so the SPA never navigates
  // away and inline types (.txt, .png, ...) still save as files.
  async function downloadEntry(entry: AnyEntry): Promise<void> {
    if (entry.is_dir) {
      toasts.error('Directory download not yet implemented')
      return
    }
    // Open servers and shared views serve bytes without a token; only gate
    // when the server would actually 401.
    if (!canDownloadWithoutToken(config.authEnabled, !!token.value, sharedMode.value)) {
      toasts.error('Not authenticated')
      return
    }
    const fileName = entry.path.split('/').pop() ?? 'file'
    const href = await run('Download', async () => {
      const result = await request<ArrayBuffer>(url(projectURL('/content'), { path: entry.path }), {
        binary: true,
      })
      return URL.createObjectURL(new Blob([result.payload]))
    })
    if (!href) return
    const anchor = document.createElement('a')
    anchor.href = href
    anchor.download = fileName
    anchor.rel = 'noopener'
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    // Revoke on the next task: immediate revocation races with the download
    // start in some browsers.
    setTimeout(() => URL.revokeObjectURL(href), 1000)
  }

  // A direct link is a share's download_url: in shared view, derive a child
  // share from the active one (the server caps its expiry to the parent's
  // remaining time); otherwise create a fresh 5-minute share (see
  // share-links.ts for WHY the kebab TTL differs from the panel's).
  async function copyDirectLink(entry: AnyEntry): Promise<void> {
    if (entry.is_dir) {
      toasts.error('Directory direct links not yet implemented')
      return
    }
    if (isSharedView.value) {
      const share = await deriveShare(entry.path)
      if (!share?.download_url) {
        toasts.error('Failed to create direct link')
        return
      }
      const absolute = directLink(share)
      const ok = await copyText(absolute)
      if (ok) toasts.success('Direct link copied')
      return
    }
    const share = await createShare(entry.path, SHARE_TTL_5M)
    if (!share?.download_url) {
      toasts.error('Failed to create direct link')
      return
    }
    const absolute = directLink(share)
    const ok = await copyText(absolute)
    if (ok) toasts.success(`Direct link copied (valid ${SHARE_TTL_5M_LABEL})`)
  }

  async function uploadFiles(items: UploadItem[], baseDir: string): Promise<void> {
    await uploads.uploadFiles(items, baseDir)
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
    xattrsError,
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
    shareId,
    canWrite,
    canEditFile,
    isSaveable: preview.isSaveable,

    // modal
    modalOpen,
    modalKind,
    modalTitle: modals.modalTitle,
    modalForm,
    modalError,
    openModal: modals.openModal,
    closeModal: modals.closeModal,
    submitModal: modals.submitModal,
    // preview
    previewKind,
    previewUrl,
    previewHex,
    previewMeta,
    editorIsText,
    uploadProgress,
    previewLoading,
    loadPreview: preview.loadPreview,
    clearPreview: preview.clearPreview,
    // actions
    loadProject,
    loadDirectory,
    selectEntry,
    inspectPath,
    readFile: preview.readFile,
    saveFile: preview.saveFile,
    refreshAll,
    login,
    logout,
    restoreSession,
    bootstrapShare,
    goUp,
    removeSelected: remover.removeSelected,
    removeMany: remover.removeMany,
    clearSelection: selection.clearSelection,
    isSelected: selection.isSelected,
    selectSingle: selection.selectSingle,
    toggleSelect: selection.toggleSelect,
    selectRange: selection.selectRange,
    selectAll: selection.selectAll,
    deleteProject,
    rollbackRevision,
    revertPath,
    prune,
    projectStatus,
    enableProject,
    downloadEntry,
    copyDirectLink,
    focusEntry,
    createShare,
    deriveShare,
    deleteShare,
    loadXattrs,
    uploadFiles,
    setXattr,
    removeXattr,
  }
}
