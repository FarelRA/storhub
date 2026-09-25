import { ApiError } from '~/utils/api-types'
import { TIMEOUTS } from '~/utils/limits'
import { useConsoleState } from './console-state'

export interface UploadItem {
  file: File
  relPath: string
}

export interface UploadDeps {
  postJSON: <T>(path: string, body: unknown) => Promise<T>
  projectURL: (suffix: string) => string
  url: (path: string, params?: Record<string, string | undefined>) => string
  canWrite: () => boolean
  refreshAll: () => Promise<boolean>
}

// Sequential (slow-network doctrine): one PUT at a time, parent directories
// ensured once per session. Module-level cache, not reactive state.
const uploadedDirs = new Set<string>()

// In-flight upload tracking: navigating away or switching projects aborts
// the active XHR and bumps the generation so the stale batch drops its
// trailing refresh instead of repainting the new location.
let activeXHR: XMLHttpRequest | null = null
let uploadGen = 0

/** Abort any in-flight upload batch (navigation, project switch). */
export function abortUploads(): void {
  uploadGen += 1
  activeXHR?.abort()
  activeXHR = null
}

/** Forget ensured directories (project switch, sign-out, share recovery). */
export function clearUploadCache(): void {
  uploadedDirs.clear()
}

/** XHR upload slice of the console composable. */
export function useUploads(deps: UploadDeps) {
  const { project, uploadProgress } = useConsoleState()
  const { token } = useConsoleState()
  const toasts = useToasts()

  /** mkdir -p against the REST API, memoized per project session. */
  async function ensureDir(dir: string): Promise<boolean> {
    const parts = normalizePath(dir).split('/').filter(Boolean)
    let cur = ''
    for (const part of parts) {
      cur = cur ? `${cur}/${part}` : part
      const key = `${project.value}/${cur}`
      if (uploadedDirs.has(key)) continue
      try {
        await deps.postJSON(deps.projectURL('/ops/mkdir'), { path: cur })
      } catch (error) {
        if (error instanceof ApiError && error.status === 409) {
          // Already exists - treat as success for mkdir -p
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
      activeXHR = xhr
      xhr.open('PUT', deps.url(deps.projectURL('/content'), { path: fullPath }))
      if (token.value) xhr.setRequestHeader('Authorization', `Bearer ${token.value}`)
      // A half-open connection must not pin the progress bar forever: give
      // the transfer a generous ceiling and fail loudly when it is hit.
      xhr.timeout = TIMEOUTS.UPLOAD_MS
      xhr.ontimeout = () => reject(new ApiError(408, 'upload timed out', null))
      xhr.onabort = () => reject(new ApiError(0, 'upload aborted', null))
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) onBytes(event.loaded)
      }
      xhr.onload = () => {
        if (activeXHR === xhr) activeXHR = null
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
      xhr.onerror = () => {
        if (activeXHR === xhr) activeXHR = null
        reject(new ApiError(0, 'network error during upload', null))
      }
      xhr.send(file)
    })
  }

  /**
   * Upload a batch of files into baseDir. Each item carries its relative
   * path (from drag-and-drop traversal or webkitRelativePath), so dropped
   * folders land with their structure intact. Byte progress is cumulative
   * across the whole batch.
   */
  async function uploadFiles(items: UploadItem[], baseDir: string): Promise<void> {
    if (!items.length || !deps.canWrite()) return
    const gen = uploadGen
    const startProject = project.value
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
      // The batch was aborted (navigation) or the project switched
      // mid-batch: stop feeding files into the old location.
      if (gen !== uploadGen || project.value !== startProject) break
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
        if (gen !== uploadGen) break
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
    // A superseded batch must not repaint the new location: drop the
    // trailing refresh and its toasts when navigation won the race.
    if (gen !== uploadGen || project.value !== startProject) return
    await deps.refreshAll()
    const { done, failed, total } = uploadProgress.value
    if (failed === 0) toasts.success(`Uploaded ${done}/${total} · ${formatBytes(bytesTotal)}`)
    else toasts.error(`Uploaded ${done - failed}/${total}. ${firstError}`)
  }

  return { uploadProgress, uploadFiles }
}
