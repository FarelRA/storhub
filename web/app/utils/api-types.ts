/**
 * Auto-imported by Nuxt as `ApiError`; also imported explicitly by use-api.
 */
export class ApiError extends Error {
  status: number
  payload: unknown

  constructor(status: number, message: string, payload: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.payload = payload
  }
}

/**
 * Wire mirror of `shfs.DirEntry` (internal/fs/types.go): the row shape
 * returned by GET /children. It carries NO uid/gid/timestamps/symlink_target;
 * those exist only on EntryInfo (GET /nodes).
 */
export interface DirEntry {
  name: string
  path: string
  kind?: string
  is_dir: boolean
  is_symlink?: boolean
  size: number
  inode?: number
  mode?: number
  nlink?: number
}

export interface EntryInfo {
  path: string
  kind?: string
  is_dir: boolean
  is_symlink?: boolean
  size: number
  inode?: number
  mode?: number
  uid?: number
  gid?: number
  nlink?: number
  modified_at: number
  created_at: number
  accessed_at?: number
  changed_at?: number
  symlink_target?: string
}

/** Anything the list rows or the stat pane hand to the action helpers. */
export type AnyEntry = DirEntry | EntryInfo

/** Wire mirror of `shfs.FSStats` (internal/fs/types.go). */
export interface ProjectStats {
  files?: number
  directories?: number
  inodes?: number
  bytes?: number
  releases?: number
  assets?: number
}

export interface Share {
  id: string
  project: string
  path: string
  url: string
  download_url?: string
  token?: string
  expires_at: string
  is_dir: boolean
}

export interface Revision {
  commit_sha: string
  message?: string
  /** Unix seconds (`metadata.MetadataRevision.CommittedAt` is int64). */
  committed_at?: number
}

export interface PruneResult {
  project: string
  status: string
  scope: string
  dry_run: boolean
  deleted_objects: number
  deleted_releases: number
  deleted_assets: number
  history_compacted: boolean
  notes?: string[]
}

export interface XattrEntry {
  name: string
  value: string
}

export interface Principal {
  username: string
  uid: number
  primary_gid: number
  groups?: number[]
  admin?: boolean
}
