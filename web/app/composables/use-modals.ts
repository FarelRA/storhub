import { MODAL_TITLES, blankForm } from '~/utils/modal-kinds'
import type { ModalKind } from '~/utils/modal-kinds'
import { sharedState } from './console-state'

export interface ModalDeps {
  run: <T>(label: string, fn: () => Promise<T>, quiet?: boolean) => Promise<T | null>
  postJSON: <T>(path: string, body: unknown) => Promise<T>
  projectURL: (suffix: string) => string
  postOp: (label: string, suffix: string, body: unknown) => Promise<boolean>
  patchOp: (label: string, params: Record<string, string>, body?: string) => Promise<boolean>
  setXattr: (path: string, name: string, value: string) => Promise<boolean>
  removeXattr: (path: string, name: string) => Promise<boolean>
  refreshAll: () => Promise<boolean>
  clearSelection: () => void
}

/**
 * Bulk move/copy as ONE loop: the only difference between the ops is the
 * op name and the payload field names ({old_path/new_path} vs
 * {src_path/dst_path}).
 */
export async function bulkRename(
  run: ModalDeps['run'],
  postJSON: ModalDeps['postJSON'],
  projectURL: ModalDeps['projectURL'],
  srcs: string[],
  destDir: string,
  op: 'rename' | 'copy',
): Promise<boolean> {
  const moving = op === 'rename'
  let ok = true
  for (const src of srcs) {
    const base = src.split('/').pop() ?? src
    const dst = destDir ? `${destDir}/${base}` : base
    const body = moving ? { old_path: src, new_path: dst } : { src_path: src, dst_path: dst }
    const res = await run(`${moving ? 'Move' : 'Copy'} ${src}`, () => postJSON(projectURL(`/ops/${op}`), body), true)
    if (res === null) ok = false
  }
  return ok
}

/** 15-kind modal state-machine slice of the god-composable. */
export function useModals(deps: ModalDeps) {
  const {
    currentPath,
    selectedEntry,
    selectedPath,
    selectedPaths,
    modalOpen,
    modalKind,
    modalForm,
    modalError,
    xattrs,
  } = sharedState()

  function openModal(kind: ModalKind, contextDir?: string, targetPath?: string): void {
    const form = blankForm()
    form.path = contextDir !== undefined
      ? `${normalizePath(contextDir)}/`
      : currentPath.value ? `${currentPath.value}/` : ''
    // Symlinks: the clicked entry is the TARGET; the link is born in contextDir.
    if (kind === 'symlink') {
      form.target = targetPath ?? selectedPath.value ?? ''
      form.newPath = `${normalizePath(contextDir || currentPath.value)}/`
    } else if (kind === 'move' || kind === 'copy') {
      const paths = [...selectedPaths.value]
      if (paths.length > 1) {
        // Bulk: destination is a directory, default to current dir
        form.newPath = currentPath.value ? `${currentPath.value}/` : ''
      } else {
        form.newPath = selectedPath.value ?? (paths[0] ?? '')
      }
    } else if (kind === 'rename') {
      form.path = selectedPath.value ?? ''
      form.newPath = selectedPath.value ?? ''
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
          await deps.postOp('mkdir', 'mkdir', { path: f.path })
          break
        case 'create-file':
          await deps.postOp('create file', 'create-file', { path: f.path })
          break
        case 'rename': {
          const oldPath = (f.path || selectedPath.value || '').trim()
          const newPath = f.newPath.trim()
          if (!oldPath) throw new Error('original path is required')
          if (!newPath) throw new Error('new path is required')
          if (oldPath === newPath) throw new Error('new path must be different')
          // postOp() already surfaces a toast on failure; keep the modal open
          // with the user's input instead of reporting the same error twice.
          if (!(await deps.postOp('rename', 'rename', { old_path: oldPath, new_path: newPath }))) return
          break
        }
        case 'move': {
          const paths = [...selectedPaths.value]
          const srcs = paths.length ? paths : selectedPath.value ? [selectedPath.value] : []
          if (!srcs.length) throw new Error('no selection')
          const dest = f.newPath.trim()
          if (!dest) throw new Error('destination is required')
          if (srcs.length === 1) {
            const first = srcs[0]
            if (first === undefined) throw new Error('no selection')
            await deps.postOp('move', 'rename', { old_path: first, new_path: dest })
          } else {
            const destDir = normalizePath(dest)
            if (!(await bulkRename(deps.run, deps.postJSON, deps.projectURL, srcs, destDir, 'rename')))
              throw new Error('some moves failed')
            await deps.refreshAll()
            deps.clearSelection()
          }
          break
        }
        case 'copy': {
          const paths = [...selectedPaths.value]
          const srcs = paths.length ? paths : selectedPath.value ? [selectedPath.value] : []
          if (!srcs.length) throw new Error('no selection')
          const dest = f.newPath.trim()
          if (!dest) throw new Error('destination is required')
          if (srcs.length === 1) {
            const first = srcs[0]
            if (first === undefined) throw new Error('no selection')
            await deps.postOp('copy', 'copy', { src_path: first, dst_path: dest })
          } else {
            const destDir = normalizePath(dest)
            if (!(await bulkRename(deps.run, deps.postJSON, deps.projectURL, srcs, destDir, 'copy')))
              throw new Error('some copies failed')
            await deps.refreshAll()
          }
          break
        }
        case 'link':
          await deps.postOp('hard link', 'link', { existing_path: selectedPath.value, new_path: f.newPath })
          break
        case 'symlink':
          await deps.postOp('symlink', 'symlink', { target: f.target, link_path: f.newPath })
          break
        case 'chmod': {
          const mode = parseInt(f.mode, 8)
          if (Number.isNaN(mode)) throw new Error(`invalid octal mode: ${f.mode}`)
          await deps.postOp('chmod', 'chmod', { path: selectedPath.value, mode })
          break
        }
        case 'chown': {
          // A cleared number input yields '' (v-model.number), and
          // Number('') === 0 would silently chown the entry to root.
          const uid = Number(f.uid)
          const gid = Number(f.gid)
          if (f.uid === '' || !Number.isInteger(uid) || uid < 0)
            throw new Error('uid must be a non-negative integer')
          if (f.gid === '' || !Number.isInteger(gid) || gid < 0)
            throw new Error('gid must be a non-negative integer')
          await deps.postOp('chown', 'chown', { path: selectedPath.value, uid, gid })
          break
        }
        case 'utimes': {
          const atime = f.atime ? new Date(f.atime) : null
          const mtime = f.mtime ? new Date(f.mtime) : null
          for (const [name, date] of [['atime', atime], ['mtime', mtime]] as const) {
            if (date && Number.isNaN(date.getTime())) throw new Error(`invalid ${name} timestamp`)
          }
          await deps.postOp('timestamps', 'utimes', {
            path: selectedPath.value,
            atime: (atime ?? new Date()).toISOString(),
            mtime: (mtime ?? new Date()).toISOString(),
          })
          break
        }
        case 'xattr-set': {
          const target = selectedPath.value
          if (!target) throw new Error('select an entry first')
          if (!f.name.trim()) throw new Error('attribute name is required')
          await deps.setXattr(target, f.name.trim(), f.value)
          break
        }
        case 'xattr-remove': {
          const target = selectedPath.value
          if (!target) throw new Error('select an entry first')
          if (!f.name.trim()) throw new Error('attribute name is required')
          await deps.removeXattr(target, f.name.trim())
          break
        }
        case 'append':
          await deps.patchOp('Append text', { op: 'append' }, f.text)
          break
        case 'patch': {
          if (!Number.isInteger(f.offset) || f.offset < 0) throw new Error('offset must be a non-negative integer')
          if (!Number.isInteger(f.deleteSize) || f.deleteSize < 0) throw new Error('delete size must be ≥ 0')
          await deps.patchOp(
            'Patch bytes',
            { op: 'patch', offset: String(f.offset), delete_size: String(f.deleteSize) },
            f.text,
          )
          break
        }
        case 'truncate': {
          if (!Number.isInteger(Number(f.text)) || Number(f.text) < 0)
            throw new Error('size must be a non-negative integer')
          await deps.patchOp('Truncate', { op: 'truncate', size: f.text })
          break
        }
      }
      closeModal()
    } catch (error) {
      modalError.value = error instanceof Error ? error.message : String(error)
    }
  }

  return {
    modalOpen,
    modalKind,
    modalTitle: (kind: ModalKind) => MODAL_TITLES[kind],
    modalForm,
    modalError,
    openModal,
    closeModal,
    submitModal,
  }
}
