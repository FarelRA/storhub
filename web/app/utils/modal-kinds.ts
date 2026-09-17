/**
 * The console's 15-kind modal state machine, factored out of use-console so
 * the taxonomy lives in exactly one place. ActionModals derives its form
 * sections from MODAL_GROUPS and useConsole derives submit from it: adding a
 * kind means touching this file, not three string lists.
 */
export type ModalKind =
  | 'mkdir'
  | 'create-file'
  | 'rename'
  | 'move'
  | 'copy'
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
  /** `v-model.number` yields '' when the input is cleared. */
  uid: number | ''
  gid: number | ''
  atime: string
  mtime: string
  name: string
  value: string
  offset: number
  deleteSize: number
  text: string
}

export function blankForm(): ModalForm {
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

export const MODAL_TITLES: Record<ModalKind, string> = {
  mkdir: 'New directory',
  'create-file': 'New file',
  rename: 'Rename',
  move: 'Move',
  copy: 'Copy',
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

/** Form-shape groups: exactly one section renders per open modal. */
export const MODAL_GROUPS = {
  path: ['mkdir', 'create-file'],
  newPath: ['rename', 'move', 'copy', 'link', 'symlink'],
  meta: ['chmod', 'chown', 'utimes', 'xattr-set', 'xattr-remove'],
  textOp: ['append', 'patch', 'truncate'],
} as const satisfies Record<string, readonly ModalKind[]>

export type ModalGroup = keyof typeof MODAL_GROUPS

export function modalGroupOf(kind: ModalKind): ModalGroup {
  for (const [group, kinds] of Object.entries(MODAL_GROUPS)) {
    if ((kinds as readonly ModalKind[]).includes(kind)) return group as ModalGroup
  }
  return 'meta'
}
