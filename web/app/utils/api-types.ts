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

export interface ProjectStats {
  files?: number
  directories?: number
  bytes?: number
  releases?: number
  last_modified?: string
  created_at?: string
  modified_at?: string
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
  committed_at?: string
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
