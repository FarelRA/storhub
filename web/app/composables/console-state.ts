import type {
  DirEntry,
  EntryInfo,
  Principal,
  ProjectStats,
  Revision,
  Share,
  XattrEntry,
} from '~/utils/api-types'
import type { ModalForm, ModalKind } from '~/utils/modal-kinds'
import type { PreviewKind } from '~/utils/preview'

/** Byte-level progress for one sequential upload batch. */
export interface UploadProgress {
  active: boolean
  done: number
  failed: number
  total: number
  current: string
  bytesDone: number
  bytesTotal: number
}

/**
 * Every piece of shared console state, unified on useState (SSR-safe store)
 * instead of the previous two systems (module ref() singletons plus a
 * lazy authRefs() exception for token/principal). One mechanism, one
 * comment, no hidden dependencies.
 *
 * useState needs a Nuxt context, so this resolves lazily inside useConsole()
 * (useState itself memoizes per key, keeping the single-page singleton
 * semantics the console relies on). Keys keep their historic names
 * ('auth-token', 'auth-principal') so useApi() shares the same slots:
 * renaming the keys would orphan stored sessions.
 */
export interface ConsoleSharedState {
  project: Ref<string>
  currentPath: Ref<string>
  selectedPath: Ref<string>
  selectedEntry: Ref<EntryInfo | null>
  selectedPaths: Ref<Set<string>>
  lastSelected: Ref<string | null>
  entries: Ref<DirEntry[]>
  stats: Ref<ProjectStats>
  shares: Ref<Share[]>
  revisions: Ref<Revision[]>
  xattrs: Ref<XattrEntry[]>
  /** True when the last xattr list fetch failed (vs genuinely empty). */
  xattrsError: Ref<boolean>
  editorContent: Ref<string>
  editorDirty: Ref<boolean>
  /** ETag of the exact bytes in the editor: sent as If-Match on save. */
  editorETag: Ref<string>
  previewKind: Ref<PreviewKind>
  previewUrl: Ref<string>
  previewHex: Ref<string>
  previewMeta: Ref<{ shown: number; total: number }>
  editorIsText: Ref<boolean>
  previewLoading: Ref<boolean>
  busy: Ref<boolean>
  token: Ref<string>
  principal: Ref<Principal | null>
  shareRequested: Ref<boolean>
  shareToken: Ref<string>
  shareRootPath: Ref<string>
  shareId: Ref<string>
  modalOpen: Ref<boolean>
  modalKind: Ref<ModalKind>
  modalForm: Ref<ModalForm>
  modalError: Ref<string>
  uploadProgress: Ref<UploadProgress>
}

let shared: ConsoleSharedState | null = null

export function useConsoleState(): ConsoleSharedState {
  if (!shared) {
    shared = {
      project: useState<string>('console-project', () => ''),
      currentPath: useState<string>('console-current-path', () => ''),
      selectedPath: useState<string>('console-selected-path', () => ''),
      selectedEntry: useState<EntryInfo | null>('console-selected-entry', () => null),
      selectedPaths: useState<Set<string>>('console-selected-paths', () => new Set()),
      lastSelected: useState<string | null>('console-last-selected', () => null),
      entries: useState<DirEntry[]>('console-entries', () => []),
      stats: useState<ProjectStats>('console-stats', () => ({})),
      shares: useState<Share[]>('console-shares', () => []),
      revisions: useState<Revision[]>('console-revisions', () => []),
      xattrs: useState<XattrEntry[]>('console-xattrs', () => []),
      xattrsError: useState<boolean>('console-xattrs-error', () => false),
      editorContent: useState<string>('console-editor-content', () => ''),
      editorDirty: useState<boolean>('console-editor-dirty', () => false),
      editorETag: useState<string>('console-editor-etag', () => ''),
      previewKind: useState<PreviewKind>('console-preview-kind', () => 'text'),
      previewUrl: useState<string>('console-preview-url', () => ''),
      previewHex: useState<string>('console-preview-hex', () => ''),
      previewMeta: useState<{ shown: number; total: number }>('console-preview-meta', () => ({ shown: 0, total: 0 })),
      editorIsText: useState<boolean>('console-editor-is-text', () => true),
      previewLoading: useState<boolean>('console-preview-loading', () => false),
      busy: useState<boolean>('console-busy', () => false),
      token: useState<string>('auth-token', () => ''),
      principal: useState<Principal | null>('auth-principal', () => null),
      shareRequested: useState<boolean>('console-share-requested', () => false),
      shareToken: useState<string>('console-share-token', () => ''),
      shareRootPath: useState<string>('console-share-root', () => ''),
      shareId: useState<string>('console-share-id', () => ''),
      modalOpen: useState<boolean>('console-modal-open', () => false),
      modalKind: useState<ModalKind>('console-modal-kind', () => 'mkdir'),
      modalForm: useState<ModalForm>('console-modal-form', () => ({
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
      })),
      modalError: useState<string>('console-modal-error', () => ''),
      uploadProgress: useState<UploadProgress>('console-upload-progress', () => ({
        active: false,
        done: 0,
        failed: 0,
        total: 0,
        current: '',
        bytesDone: 0,
        bytesTotal: 0,
      })),
    }
  }
  return shared
}
