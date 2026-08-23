package rest

import "net/http"

const uiDocument = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>StorHub Console</title>
  <link rel="stylesheet" href="/styles.css">
  <script defer src="/alpine.min.js"></script>
  <script src="/config.js"></script>
  <script src="/app.js"></script>
</head>
<body x-data="storhubConsole()" x-init="init()">
  <div class="shell">
    <aside class="sidebar">
      <div class="brand-row">
        <div class="brand">StorHub</div>
        <div class="brand-note">REST console</div>
      </div>

      <div class="section">
        <label class="field">
          <span>Project</span>
          <input x-model.trim="project" @keydown.enter.prevent="loadProject()" type="text" placeholder="demo-project" autocomplete="off">
        </label>
        <div class="row two">
          <button class="button solid" @click="loadProject()" type="button">Load</button>
          <button class="button subtle" @click="refreshAll()" type="button">Refresh</button>
        </div>
      </div>

      <div class="section" x-show="config.authEnabled && !isSharedMode">
        <template x-if="!token">
          <form class="stack" @submit.prevent="login()">
            <label class="field"><span>Username</span><input x-model.trim="auth.username" type="text" autocomplete="username"></label>
            <label class="field"><span>Password</span><input x-model="auth.password" type="password" autocomplete="current-password"></label>
            <button class="button solid" type="submit">Sign In</button>
          </form>
        </template>
        <template x-if="token">
          <div class="stack">
            <div class="meta good">Authenticated session active</div>
            <button class="button subtle" @click="logout()" type="button">Sign Out</button>
          </div>
        </template>
      </div>

      <div class="section" x-show="!isSharedMode">
        <div class="section-title">Project</div>
        <div class="stats">
          <div><span>Files</span><strong x-text="projectStats.files ?? '-' "></strong></div>
          <div><span>Dirs</span><strong x-text="projectStats.directories ?? '-' "></strong></div>
          <div><span>Bytes</span><strong x-text="formatNumber(projectStats.bytes)"></strong></div>
          <div><span>Releases</span><strong x-text="projectStats.releases ?? '-' "></strong></div>
        </div>
        <button class="button danger" @click="deleteProject()" type="button">Delete Project</button>
      </div>

      <div class="section" x-show="!isSharedMode">
        <div class="section-title">Directory Actions</div>
        <div class="stack">
          <button class="button subtle" @click="openModal('mkdir')" type="button">Make Directory</button>
          <button class="button subtle" @click="goUp()" type="button">Up One Level</button>
          <button class="button subtle" @click="removeCurrentDirectory()" type="button">Remove Current Dir</button>
        </div>
      </div>

      <div class="section hint" x-show="!isSharedMode">
        All file, metadata, revision, and maintenance actions use the same REST endpoints exposed by the server.
      </div>

      <div class="section" x-show="project && !isSharedMode">
        <div class="pane-head">
          <div class="section-title">Shares</div>
          <button class="button subtle mini" @click="loadShares()" :disabled="!project" type="button">Reload</button>
        </div>
        <div class="list compact">
          <template x-for="share in shares" :key="share.id">
            <div class="share-item">
              <div class="item-main">
                <strong x-text="share.path || '/'"></strong>
                <small x-text="shareMeta(share)"></small>
              </div>
              <div class="share-actions">
                <button class="button subtle mini" @click="copyShareURL(share)" type="button">Link</button>
                <button class="button subtle mini" @click="copyShareDownloadURL(share)" :disabled="!share.download || share.is_dir" type="button">Direct</button>
                <button class="button danger mini" @click="deleteShare(share)" type="button">Delete</button>
              </div>
            </div>
          </template>
          <div class="meta" x-show="shares.length === 0">No shares</div>
        </div>
      </div>

      <div class="section" x-show="isSharedMode">
        <div class="section-title">Shared Access</div>
        <div class="meta">Project: <span x-text="project"></span></div>
        <div class="meta">Root: <span x-text="shareRootLabel()"></span></div>
        <div class="meta" x-text="shareDownload ? 'Download allowed' : 'Browser-only share'"></div>
      </div>
    </aside>

    <main class="main">
      <header class="toolbar">
        <div>
          <div class="pathbar" x-text="project ? project + ':' + currentPathLabel() : 'No project loaded'"></div>
          <div class="meta" x-text="statusText"></div>
        </div>
        <div class="toolbar-actions">
          <button class="button subtle" @click="copyCurrentShareLink()" :disabled="!canCopyShareLink()" type="button">Copy Share Link</button>
          <button class="button subtle" @click="copyCurrentDirectDownload()" :disabled="!canCopyDirectDownload()" type="button">Copy Direct Download</button>
          <button class="button subtle" @click="openModal('create-file')" :disabled="isReadOnly()" type="button">New File</button>
          <button class="button subtle" @click="openModal('rename')" :disabled="!selectedPath || isReadOnly()" type="button">Rename</button>
          <button class="button subtle" @click="openModal('link')" :disabled="!selectedPath || isDirectory(selectedEntry) || isReadOnly()" type="button">Link</button>
          <button class="button subtle" @click="openModal('symlink')" :disabled="isReadOnly()" type="button">Symlink</button>
          <button class="button solid" @click="saveFile()" :disabled="!canEditFile()" type="button">Save</button>
        </div>
      </header>

      <div class="workspace">
        <section class="pane pane-tree">
          <div class="pane-head">
            <div class="pane-title">Directory</div>
            <div class="meta" x-text="entries.length + ' entries'"></div>
          </div>
          <div class="list tree-list">
            <template x-for="entry in entries" :key="entry.path">
              <button class="item" :class="{'active': selectedPath === entry.path}" @click="selectEntry(entry)" type="button">
                <span class="item-main">
                  <strong x-text="entry.name"></strong>
                  <small x-text="entryKind(entry)"></small>
                </span>
                <span x-text="isDirectory(entry) ? '' : formatNumber(entry.size)"></span>
              </button>
            </template>
            <div class="meta" x-show="entries.length === 0">Empty directory</div>
          </div>
        </section>

        <section class="pane pane-editor">
          <div class="pane-head">
            <div class="pane-title">Editor</div>
            <div class="meta" x-text="selectedPath || 'Select a file or directory'"></div>
          </div>

          <div class="editor-actions">
            <button class="button subtle" @click="readSelected()" :disabled="!selectedPath" type="button">Read</button>
            <button class="button subtle" @click="appendToSelected()" :disabled="!canEditFile()" type="button">Append</button>
            <button class="button subtle" @click="patchSelected()" :disabled="!canEditFile()" type="button">Patch</button>
            <button class="button subtle" @click="truncateSelected()" :disabled="!canEditFile()" type="button">Truncate</button>
            <button class="button danger" @click="removeSelected()" :disabled="!selectedPath || isReadOnly()" type="button">Remove</button>
          </div>

          <textarea x-model="editor" spellcheck="false" placeholder="File content or symlink target will appear here."></textarea>

          <div class="range-bar">
            <label class="field inline"><span>Offset</span><input x-model="range.offset" type="number" min="0"></label>
            <label class="field inline"><span>Length</span><input x-model="range.length" type="number" min="1"></label>
            <button class="button subtle" @click="readRange()" :disabled="!canReadRanges()" type="button">Read Range</button>
          </div>
        </section>

        <section class="pane pane-side">
          <div class="subpanel">
            <div class="pane-head">
              <div class="pane-title">Entry</div>
              <div class="meta" x-text="selectedEntry ? entryKind(selectedEntry) : 'None'"></div>
            </div>
            <dl class="detail-grid">
              <template x-for="row in entryRows()" :key="row.key">
                <div class="detail-row">
                  <dt x-text="row.key"></dt>
                  <dd x-text="row.value"></dd>
                </div>
              </template>
            </dl>
            <div class="row two">
              <button class="button subtle" @click="openModal('chmod')" :disabled="!selectedPath || isReadOnly()" type="button">Chmod</button>
              <button class="button subtle" @click="openModal('chown')" :disabled="!selectedPath || isReadOnly()" type="button">Chown</button>
            </div>
            <button class="button subtle full" @click="openModal('utimes')" :disabled="!selectedPath || isReadOnly()" type="button">Update Timestamps</button>
          </div>

          <div class="subpanel">
            <div class="pane-head">
              <div class="pane-title">XAttrs</div>
              <button class="button subtle mini" @click="loadXAttrs()" :disabled="!selectedPath" type="button">Reload</button>
            </div>
            <div class="list compact">
              <template x-for="item in xattrs" :key="item.name">
                <button class="item" @click="inspectXAttr(item.name)" type="button">
                  <span class="item-main"><strong x-text="item.name"></strong><small x-text="item.value"></small></span>
                </button>
              </template>
              <div class="meta" x-show="xattrs.length === 0">No xattrs</div>
            </div>
            <div class="row two">
              <button class="button subtle" @click="openModal('xattr-set')" :disabled="!selectedPath || isReadOnly()" type="button">Set</button>
              <button class="button danger" @click="openModal('xattr-remove')" :disabled="!selectedPath || xattrs.length === 0 || isReadOnly()" type="button">Remove</button>
            </div>
          </div>

          <div class="subpanel" x-show="!isSharedMode">
            <div class="pane-head">
              <div class="pane-title">Revisions</div>
              <button class="button subtle mini" @click="loadRevisions()" :disabled="!project" type="button">Reload</button>
            </div>
            <div class="list compact">
              <template x-for="revision in revisions" :key="revision.commit_sha">
                <button class="item" @click="rollback(revision.commit_sha)" type="button">
                  <span class="item-main"><strong x-text="revision.commit_sha.slice(0, 10)"></strong><small x-text="revision.message || ''"></small></span>
                </button>
              </template>
              <div class="meta" x-show="revisions.length === 0">No revisions</div>
            </div>
          </div>

          <div class="subpanel">
            <div class="pane-head">
              <div class="pane-title">Response</div>
            </div>
            <pre class="response" x-text="responseText"></pre>
          </div>
        </section>
      </div>
    </main>
  </div>

  <div class="overlay" x-show="modal.open" x-transition.opacity>
    <div class="dialog" @click.outside="closeModal()">
      <div class="pane-head">
        <div class="pane-title" x-text="modal.title"></div>
        <button class="button subtle mini" @click="closeModal()" type="button">Close</button>
      </div>
      <div class="stack" x-show="modal.kind === 'mkdir' || modal.kind === 'create-file' || modal.kind === 'rename' || modal.kind === 'link' || modal.kind === 'symlink'">
        <label class="field" x-show="modal.kind !== 'rename' && modal.kind !== 'link' && modal.kind !== 'symlink'"><span>Path</span><input x-model="modal.form.path" type="text"></label>
        <label class="field" x-show="modal.kind === 'rename'"><span>New path</span><input x-model="modal.form.newPath" type="text"></label>
        <label class="field" x-show="modal.kind === 'link'"><span>New path</span><input x-model="modal.form.newPath" type="text"></label>
        <label class="field" x-show="modal.kind === 'symlink'"><span>Target</span><input x-model="modal.form.target" type="text"></label>
        <label class="field" x-show="modal.kind === 'symlink'"><span>Link path</span><input x-model="modal.form.newPath" type="text"></label>
      </div>
      <div class="stack" x-show="modal.kind === 'chmod' || modal.kind === 'chown' || modal.kind === 'utimes' || modal.kind === 'xattr-set' || modal.kind === 'xattr-remove'">
        <label class="field" x-show="modal.kind === 'chmod'"><span>Mode</span><input x-model="modal.form.mode" type="text" placeholder="0644"></label>
        <label class="field" x-show="modal.kind === 'chown'"><span>UID</span><input x-model="modal.form.uid" type="number" min="0"></label>
        <label class="field" x-show="modal.kind === 'chown'"><span>GID</span><input x-model="modal.form.gid" type="number" min="0"></label>
        <label class="field" x-show="modal.kind === 'utimes'"><span>Atime</span><input x-model="modal.form.atime" type="datetime-local"></label>
        <label class="field" x-show="modal.kind === 'utimes'"><span>Mtime</span><input x-model="modal.form.mtime" type="datetime-local"></label>
        <label class="field" x-show="modal.kind === 'xattr-set' || modal.kind === 'xattr-remove'"><span>Name</span><input x-model="modal.form.name" type="text"></label>
        <label class="field" x-show="modal.kind === 'xattr-set'"><span>Value</span><input x-model="modal.form.value" type="text"></label>
      </div>
      <div class="row two">
        <button class="button solid" @click="submitModal()" type="button">Apply</button>
        <button class="button subtle" @click="closeModal()" type="button">Cancel</button>
      </div>
    </div>
  </div>
</body>
</html>
`

const uiStyles = `:root {
  --bg: #161311;
  --surface: #201c19;
  --surface-2: #2a2521;
  --surface-3: #332d29;
  --line: #453d38;
  --text: #f3eadc;
  --muted: #b9ad9c;
  --accent: #c67a24;
  --accent-2: #889d6c;
  --danger: #9c4c36;
}
* { box-sizing: border-box; }
html, body { margin: 0; min-height: 100%; background: var(--bg); color: var(--text); font: 14px/1.45 "IBM Plex Sans", "Iosevka Aile", sans-serif; }
body { background-image: linear-gradient(180deg, rgba(255,255,255,0.02), transparent 24%), repeating-linear-gradient(0deg, transparent, transparent 31px, rgba(255,255,255,0.028) 32px); }
button, input, textarea { font: inherit; }
.shell { min-height: 100vh; display: grid; grid-template-columns: 252px 1fr; }
.sidebar { background: rgba(0,0,0,0.14); border-right: 1px solid var(--line); padding: 18px; overflow: auto; }
.main { min-width: 0; display: grid; grid-template-rows: 58px 1fr; }
.brand-row { margin-bottom: 18px; }
.brand { font: 600 18px/1.1 "IBM Plex Mono", monospace; }
.brand-note, .meta, .hint, .field span, .pane-title, .section-title, .item small { color: var(--muted); font-size: 12px; }
.section { border-top: 1px solid var(--line); padding-top: 16px; margin-top: 16px; }
.stack { display: grid; gap: 8px; }
.row { display: flex; gap: 8px; }
.row.two > * { flex: 1; }
.field { display: grid; gap: 6px; }
.field.inline { grid-template-columns: 56px 1fr; align-items: center; }
.field input, textarea { width: 100%; background: var(--surface); border: 1px solid var(--line); color: var(--text); border-radius: 8px; padding: 10px 12px; outline: none; }
.field input:focus, textarea:focus { border-color: var(--accent); }
.button { appearance: none; border: 1px solid var(--line); background: var(--surface); color: var(--text); border-radius: 8px; padding: 9px 12px; cursor: pointer; transition: background-color .14s ease, border-color .14s ease; }
.button:hover { border-color: var(--accent); background: var(--surface-2); }
.button:disabled { opacity: .45; cursor: default; }
.button.solid { background: var(--accent); border-color: #d99243; color: #1f1408; font-weight: 600; }
.button.solid:hover { background: #d1842a; }
.button.danger { background: transparent; color: #f0bbb0; border-color: #7b4638; }
.button.danger:hover { background: rgba(156, 76, 54, 0.18); }
.button.mini { padding: 6px 9px; }
.button.full { width: 100%; }
.toolbar { padding: 10px 18px; border-bottom: 1px solid var(--line); display: flex; align-items: center; justify-content: space-between; gap: 10px; }
.pathbar { font: 600 14px/1.2 "IBM Plex Mono", monospace; }
.toolbar-actions, .editor-actions { display: flex; gap: 8px; flex-wrap: wrap; }
.workspace { min-height: 0; display: grid; grid-template-columns: 300px minmax(360px, 1fr) 340px; }
.pane { min-width: 0; min-height: 0; padding: 16px; border-right: 1px solid var(--line); overflow: auto; }
.pane-side { border-right: 0; background: rgba(255,255,255,0.02); }
.pane-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.list { display: grid; gap: 2px; margin-top: 10px; }
.compact { max-height: 220px; overflow: auto; }
.tree-list { max-height: calc(100vh - 180px); overflow: auto; }
.item { width: 100%; border: 1px solid transparent; border-radius: 8px; background: transparent; color: var(--text); text-align: left; padding: 9px 10px; display: flex; align-items: center; justify-content: space-between; gap: 10px; cursor: pointer; }
.item:hover, .item.active { background: var(--surface); border-color: var(--line); }
.item-main { display: grid; gap: 2px; min-width: 0; }
.item strong { font-weight: 500; overflow: hidden; text-overflow: ellipsis; }
.share-item { border: 1px solid var(--line); border-radius: 8px; background: var(--surface); padding: 9px 10px; display: grid; gap: 8px; }
.share-actions { display: flex; gap: 6px; flex-wrap: wrap; }
textarea { min-height: 360px; resize: vertical; margin-top: 10px; }
.range-bar { margin-top: 10px; display: grid; grid-template-columns: 1fr 1fr auto; gap: 8px; }
.subpanel { border-top: 1px solid var(--line); padding-top: 14px; margin-top: 14px; }
.detail-grid { margin: 10px 0 0; display: grid; grid-template-columns: 88px 1fr; gap: 8px 10px; }
.detail-row { display: contents; }
.detail-grid dt { color: var(--muted); }
.detail-grid dd { margin: 0; word-break: break-word; }
.stats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 10px; margin-bottom: 12px; }
.stats div { border: 1px solid var(--line); border-radius: 8px; padding: 10px; background: var(--surface); }
.stats span { display: block; color: var(--muted); font-size: 12px; }
.stats strong { display: block; margin-top: 2px; font: 600 16px/1.1 "IBM Plex Mono", monospace; }
.response { min-height: 160px; max-height: 240px; overflow: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 12px; white-space: pre-wrap; word-break: break-word; }
.good { color: #b9d19a; }
.overlay { position: fixed; inset: 0; background: rgba(10, 8, 7, 0.66); display: flex; align-items: center; justify-content: center; padding: 20px; }
.dialog { width: min(420px, 100%); background: var(--surface-2); border: 1px solid var(--line); border-radius: 10px; padding: 16px; }
@media (max-width: 1120px) {
  .workspace { grid-template-columns: 260px 1fr; }
  .pane-side { grid-column: 1 / -1; border-top: 1px solid var(--line); }
}
@media (max-width: 780px) {
  .shell { grid-template-columns: 1fr; }
  .sidebar { border-right: 0; border-bottom: 1px solid var(--line); }
  .workspace { grid-template-columns: 1fr; }
  .pane { border-right: 0; border-bottom: 1px solid var(--line); }
  .toolbar { flex-direction: column; align-items: stretch; }
  .range-bar { grid-template-columns: 1fr; }
}
`

const uiScript = `window.storhubConsole = function () {
  return {
    config: window.STORHUB_UI_CONFIG || { basePath: '/api/v1', authEnabled: false },
    token: sessionStorage.getItem('storhub.token') || '',
    shareToken: '',
    shareRequested: false,
    shareRootPath: '',
    shareDownload: true,
    project: '',
    currentPath: '',
    selectedPath: '',
    selectedEntry: null,
    projectStats: {},
    entries: [],
    shares: [],
    revisions: [],
    xattrs: [],
    editor: '',
    responseText: 'Ready.',
    statusText: 'Waiting for a project.',
    auth: { username: '', password: '' },
    range: { offset: 0, length: 4096 },
    modal: { open: false, kind: '', title: '', form: {} },
    init() {
      this.responseText = 'Open a project to browse the filesystem.'
      const shareToken = new URLSearchParams(window.location.search).get('share')
      if (shareToken) {
        this.shareRequested = true
        this.token = ''
        this.bootstrapSharedMode(shareToken)
      }
    },
    get isSharedMode() {
      return this.shareRequested || !!this.shareToken
    },
    headers(extra = {}) {
      const headers = { ...extra }
      if (this.token) headers.Authorization = 'Bearer ' + this.token
      return headers
    },
    normalizePath(path) {
      const parts = String(path || '').split('/')
      const clean = []
      for (const part of parts) {
        if (!part || part === '.') continue
        if (part === '..') {
          if (!clean.length) continue
          clean.pop()
          continue
        }
        clean.push(part)
      }
      return clean.join('/')
    },
    parentPath(path) {
      const current = this.normalizePath(path)
      if (!current) return ''
      return current.split('/').slice(0, -1).join('/')
    },
    isWithinShareRoot(path) {
      const target = this.normalizePath(path)
      if (!this.isSharedMode || !this.shareRootPath) return true
      return target === this.shareRootPath || target.startsWith(this.shareRootPath + '/')
    },
    shareRootLabel() {
      return this.shareRootPath || '/'
    },
    rootURL(params) {
      const query = new URLSearchParams(params || {})
      const text = query.toString()
      return '/' + (text ? '?' + text : '')
    },
    shareURL(share) {
      return this.rootURL({ share: share.id || share.token })
    },
    shareDownloadURL(share, targetPath) {
      const params = { share: share.id || share.token, download: '1' }
      if (targetPath) params.path = this.normalizePath(targetPath)
      return this.rootURL(params)
    },
    currentPathLabel() {
      return this.currentPath || '/'
    },
    isDirectory(entry) {
      return !!(entry && (entry.is_dir || entry.isDir))
    },
    isReadOnly() {
      return this.isSharedMode
    },
    canCopyShareLink() {
      if (this.isSharedMode) return !!this.shareToken
      return !!this.project && !!this.selectedPath
    },
    canCopyDirectDownload() {
      if (this.isSharedMode) return !!this.shareToken && !!this.selectedPath && !this.isDirectory(this.selectedEntry) && this.shareDownload
      return !!this.project && !!this.selectedPath && !this.isDirectory(this.selectedEntry)
    },
    formatNumber(value) {
      if (value === undefined || value === null || value === '') return '-'
      return Number(value).toLocaleString()
    },
    entryKind(entry) {
      if (!entry) return '-'
      if (this.isDirectory(entry)) return 'directory'
      if (entry.is_symlink || entry.isSymlink) return 'symlink'
      return 'file'
    },
    shareMeta(share) {
      const mode = share.download ? 'download' : 'browser-only'
      const kind = share.is_dir ? 'folder' : 'file'
      return kind + ' • ' + mode + ' • expires ' + share.expires_at
    },
    async copyText(value, label) {
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(value)
        } else {
          window.prompt('Copy to clipboard:', value)
        }
        this.statusText = (label || 'Copied') + ': ' + value
      } catch (_) {
        window.prompt('Copy to clipboard:', value)
      }
    },
    async bootstrapSharedMode(token) {
	      try {
	        const { payload } = await fetch(this.config.basePath + '/shares/' + encodeURIComponent(token)).then(async (response) => {
	          const contentType = response.headers.get('content-type') || ''
	          const payload = contentType.includes('application/json') ? await response.json().catch(() => null) : await response.text()
	          if (!response.ok) {
	            const message = payload && payload.error ? payload.error.message : response.statusText
	            throw { payload, message }
	          }
	          return { payload }
	        })
	        this.shareRequested = true
	        this.shareToken = payload.id || payload.token || token
	        this.token = this.shareToken
	        this.project = payload.project
	        this.shareRootPath = this.normalizePath(payload.path)
	        this.shareDownload = payload.download !== false
	        this.statusText = 'Shared access ready.'
	        await this.loadSharedResource()
	      } catch (error) {
	        this.shareRequested = true
	        this.shareToken = ''
	        this.token = ''
	        this.project = ''
	        this.shareRootPath = ''
	        this.fail('Shared access failed', error.payload || { message: error.message || 'Invalid share token' })
	      }
    },
    async loadSharedResource() {
      this.projectStats = {}
      this.revisions = []
      if (!this.project) return
      try {
        if (!this.shareRootPath) {
          this.selectedPath = ''
          this.selectedEntry = null
          await this.loadDirectory('')
          return
        }
        const entry = await this.inspectPath(this.shareRootPath)
        if (!entry) return
        if (this.isDirectory(entry)) {
          await this.loadDirectory(this.shareRootPath)
          return
        }
        this.currentPath = this.shareRootPath
        this.entries = [entry]
        await this.readSelected()
      } catch (error) {
        this.fail('Shared load failed', error)
      }
    },
    entryRows() {
      const entry = this.selectedEntry
      if (!entry) return []
      return [
        { key: 'path', value: entry.path || '/' },
        { key: 'kind', value: this.entryKind(entry) },
        { key: 'mode', value: String(entry.mode ?? '-') },
        { key: 'uid', value: String(entry.uid ?? '-') },
        { key: 'gid', value: String(entry.gid ?? '-') },
        { key: 'size', value: this.formatNumber(entry.size) },
        { key: 'inode', value: String(entry.inode ?? '-') },
        { key: 'links', value: String(entry.nLink ?? entry.nlink ?? '-') },
        { key: 'target', value: entry.symlinkTarget || entry.symlink_target || '-' }
      ]
    },
    canEditFile() {
      return !this.isReadOnly() && this.selectedEntry && !this.isDirectory(this.selectedEntry)
    },
    canReadRanges() {
      return this.selectedEntry && !this.isDirectory(this.selectedEntry) && !(this.selectedEntry.is_symlink || this.selectedEntry.isSymlink)
    },
    async api(path, options = {}) {
      const response = await fetch(path, { ...options, headers: this.headers(options.headers || {}) })
      const contentType = response.headers.get('content-type') || ''
      const payload = contentType.includes('application/json') ? await response.json().catch(() => null) : await response.text()
      if (!response.ok) {
        const message = payload && payload.error ? payload.error.message : response.statusText
        throw { status: response.status, payload, message }
      }
      this.responseText = JSON.stringify(payload, null, 2)
      return { response, payload }
    },
    async login() {
      try {
        const { payload } = await this.api(this.config.basePath + '/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username: this.auth.username, password: this.auth.password })
        })
        this.token = payload.token
        sessionStorage.setItem('storhub.token', this.token)
        this.auth.password = ''
        this.statusText = 'Authenticated.'
        if (this.project) await this.refreshAll()
      } catch (error) {
        this.fail('Login failed', error)
      }
    },
    logout() {
      if (this.isSharedMode) return
      this.token = ''
      sessionStorage.removeItem('storhub.token')
      this.statusText = 'Signed out.'
    },
    requireProject() {
      if (!this.project) throw new Error('Project is required')
    },
    fail(label, error) {
      this.statusText = label
      this.responseText = JSON.stringify(error.payload || { error: { message: error.message || String(error) } }, null, 2)
    },
    async loadProject() {
      if (this.isSharedMode) {
        await this.loadSharedResource()
        return
      }
      try {
        this.requireProject()
        this.currentPath = ''
        this.selectedPath = ''
        this.selectedEntry = null
        await this.refreshAll()
      } catch (error) {
        this.fail('Project load failed', error)
      }
    },
    async refreshAll() {
      if (this.isSharedMode) {
        await this.loadSharedResource()
        return
      }
      await Promise.all([this.loadStats(), this.loadDirectory(this.currentPath), this.loadRevisions(), this.loadShares()])
      if (this.selectedPath) await this.inspectPath(this.selectedPath)
    },
    async loadStats() {
      if (this.isSharedMode) {
        this.projectStats = {}
        return
      }
      const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project))
      this.projectStats = payload.stats || {}
    },
    async loadShares() {
      if (!this.project || this.isSharedMode) {
        this.shares = []
        return
      }
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/shares')
        this.shares = payload.shares || []
      } catch (error) {
        this.shares = []
        this.fail('Share load failed', error)
      }
    },
    async loadDirectory(path) {
      const nextPath = this.normalizePath(path)
      if (this.isSharedMode && !this.isWithinShareRoot(nextPath)) {
        this.currentPath = this.shareRootPath
      } else {
        this.currentPath = nextPath
      }
      this.statusText = 'Loading directory ' + this.currentPathLabel()
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/children?path=' + encodeURIComponent(this.currentPath))
        this.entries = payload.entries || []
        this.statusText = 'Loaded ' + this.entries.length + ' entries'
      } catch (error) {
        this.entries = []
        this.fail('Directory request failed', error)
      }
    },
    async selectEntry(entry) {
      this.selectedPath = entry.path
      if (this.isDirectory(entry)) {
        await this.inspectPath(entry.path)
        await this.loadDirectory(entry.path)
        return
      }
      await this.inspectPath(entry.path)
      await this.readSelected()
    },
    async inspectPath(path) {
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/nodes?path=' + encodeURIComponent(path))
        this.selectedEntry = payload.entry
        this.selectedPath = path
        await this.loadXAttrs()
        return payload.entry
      } catch (error) {
        this.fail('Stat failed', error)
        return null
      }
    },
    async readSelected() {
      if (!this.selectedPath) return
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.selectedPath))
        this.editor = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2)
        this.statusText = 'Loaded content for ' + this.selectedPath
      } catch (error) {
        this.fail('Read failed', error)
      }
    },
    async readRange() {
      if (!this.canReadRanges()) return
      try {
        const end = Number(this.range.offset) + Number(this.range.length) - 1
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.selectedPath), {
          headers: { Range: 'bytes=' + this.range.offset + '-' + end }
        })
        this.editor = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2)
        this.statusText = 'Loaded range for ' + this.selectedPath
      } catch (error) {
        this.fail('Range read failed', error)
      }
    },
    async saveFile() {
      if (!this.canEditFile()) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.selectedPath), {
          method: 'PUT',
          body: this.editor
        })
        await this.inspectPath(this.selectedPath)
        await this.loadDirectory(this.currentPath)
        await this.loadRevisions()
        this.statusText = 'Saved ' + this.selectedPath
      } catch (error) {
        this.fail('Save failed', error)
      }
    },
    async appendToSelected() {
      const text = prompt('Append text')
      if (text === null) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.selectedPath) + '&op=append', {
          method: 'PATCH',
          body: text
        })
        await this.readSelected()
        await this.loadRevisions()
      } catch (error) {
        this.fail('Append failed', error)
      }
    },
    async patchSelected() {
      const offset = prompt('Patch offset', '0')
      if (offset === null) return
      const deleteSize = prompt('Delete size', '0')
      if (deleteSize === null) return
      const text = prompt('Replacement text', '')
      if (text === null) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.selectedPath) + '&op=patch&offset=' + encodeURIComponent(offset) + '&delete_size=' + encodeURIComponent(deleteSize), {
          method: 'PATCH',
          body: text
        })
        await this.readSelected()
        await this.loadRevisions()
      } catch (error) {
        this.fail('Patch failed', error)
      }
    },
    async truncateSelected() {
      const size = prompt('New file size', '0')
      if (size === null) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.selectedPath) + '&op=truncate&size=' + encodeURIComponent(size), {
          method: 'PATCH'
        })
        await this.readSelected()
        await this.loadRevisions()
      } catch (error) {
        this.fail('Truncate failed', error)
      }
    },
    async loadRevisions() {
      if (!this.project || this.isSharedMode) {
        this.revisions = []
        return
      }
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/revisions')
        this.revisions = payload.revisions || []
      } catch (error) {
        this.fail('Revision load failed', error)
      }
    },
    async rollback(sha) {
      if (!confirm('Rollback metadata to ' + sha + '?')) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/ops/rollback', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ commit_sha: sha })
        })
        await this.refreshAll()
      } catch (error) {
        this.fail('Rollback failed', error)
      }
    },
    async loadXAttrs() {
      if (!this.selectedPath) { this.xattrs = []; return }
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/xattrs?path=' + encodeURIComponent(this.selectedPath))
        const names = payload.names || []
        this.xattrs = []
        for (const name of names) {
          try {
            const value = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/xattrs/value?path=' + encodeURIComponent(this.selectedPath) + '&name=' + encodeURIComponent(name))
            this.xattrs.push({ name, value: typeof value.payload === 'string' ? value.payload : JSON.stringify(value.payload) })
          } catch (_) {
            this.xattrs.push({ name, value: '[unavailable]' })
          }
        }
      } catch (error) {
        this.xattrs = []
        this.fail('XAttr load failed', error)
      }
    },
    async inspectXAttr(name) {
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/xattrs/value?path=' + encodeURIComponent(this.selectedPath) + '&name=' + encodeURIComponent(name))
        this.responseText = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2)
      } catch (error) {
        this.fail('XAttr read failed', error)
      }
    },
    async createShareForSelected(download) {
      if (!this.project || !this.selectedPath) return null
      try {
        const { payload } = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/shares', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: this.selectedPath, download: download !== false })
        })
        await this.loadShares()
        return payload
      } catch (error) {
        this.fail('Share create failed', error)
        return null
      }
    },
    async copyCurrentShareLink() {
      if (this.isSharedMode) {
        await this.copyText(this.shareURL({ id: this.shareToken }), 'Share link copied')
        return
      }
      const share = await this.createShareForSelected(true)
      if (share) await this.copyText(this.shareURL(share), 'Share link copied')
    },
    async copyCurrentDirectDownload() {
      if (this.isSharedMode) {
        if (!this.canCopyDirectDownload()) return
        await this.copyText(this.shareDownloadURL({ id: this.shareToken }, this.selectedPath === this.shareRootPath ? '' : this.selectedPath), 'Direct download copied')
        return
      }
      const share = await this.createShareForSelected(true)
      if (share) await this.copyText(share.download_url || this.shareDownloadURL(share), 'Direct download copied')
    },
    async copyShareURL(share) {
      await this.copyText(this.shareURL(share), 'Share link copied')
    },
    async copyShareDownloadURL(share) {
      if (!share.download || share.is_dir) return
      await this.copyText(share.download_url || this.shareDownloadURL(share), 'Direct download copied')
    },
    async deleteShare(share) {
      if (!confirm('Delete share ' + (share.path || '/') + '?')) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/shares/' + encodeURIComponent(share.id || share.token), { method: 'DELETE' })
        await this.loadShares()
      } catch (error) {
        this.fail('Share deletion failed', error)
      }
    },
    openModal(kind) {
      this.modal = { open: true, kind, title: this.modalTitle(kind), form: { path: this.currentPath ? this.currentPath + '/' : '', newPath: this.selectedPath || '', target: this.selectedPath || '', mode: this.selectedEntry ? String(this.selectedEntry.mode || '') : '0644', uid: this.selectedEntry ? this.selectedEntry.uid || 0 : 0, gid: this.selectedEntry ? this.selectedEntry.gid || 0 : 0, atime: '', mtime: '', name: this.xattrs[0] ? this.xattrs[0].name : '', value: '' } }
    },
    closeModal() { this.modal.open = false },
    modalTitle(kind) {
      return ({ 'mkdir': 'Create Directory', 'create-file': 'Create File', 'rename': 'Rename Entry', 'link': 'Create Hard Link', 'symlink': 'Create Symlink', 'chmod': 'Change Mode', 'chown': 'Change Owner', 'utimes': 'Update Timestamps', 'xattr-set': 'Set XAttr', 'xattr-remove': 'Remove XAttr' })[kind] || 'Action'
    },
    async submitModal() {
      const f = this.modal.form
      try {
        if (this.modal.kind === 'mkdir') await this.jsonPost('/ops/mkdir', { path: f.path })
        if (this.modal.kind === 'create-file') await this.jsonPost('/ops/create-file', { path: f.path })
        if (this.modal.kind === 'rename') await this.jsonPost('/ops/rename', { old_path: this.selectedPath, new_path: f.newPath })
        if (this.modal.kind === 'link') await this.jsonPost('/ops/link', { existing_path: this.selectedPath, new_path: f.newPath })
        if (this.modal.kind === 'symlink') await this.jsonPost('/ops/symlink', { target: f.target, link_path: f.newPath })
        if (this.modal.kind === 'chmod') await this.jsonPost('/ops/chmod', { path: this.selectedPath, mode: parseInt(f.mode, 8) || Number(f.mode) })
        if (this.modal.kind === 'chown') await this.jsonPost('/ops/chown', { path: this.selectedPath, uid: Number(f.uid), gid: Number(f.gid) })
        if (this.modal.kind === 'utimes') await this.jsonPost('/ops/utimes', { path: this.selectedPath, atime: new Date(f.atime).toISOString(), mtime: new Date(f.mtime).toISOString() })
        if (this.modal.kind === 'xattr-set') await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/xattrs/value?path=' + encodeURIComponent(this.selectedPath) + '&name=' + encodeURIComponent(f.name), { method: 'PUT', body: f.value })
        if (this.modal.kind === 'xattr-remove') await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/xattrs/value?path=' + encodeURIComponent(this.selectedPath) + '&name=' + encodeURIComponent(f.name), { method: 'DELETE' })
        this.closeModal()
        await this.refreshAll()
      } catch (error) {
        this.fail('Action failed', error)
      }
    },
    async jsonPost(suffix, body) {
      return this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + suffix, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
    },
    async removeSelected() {
      if (!this.selectedPath) return
      if (!confirm('Remove ' + this.selectedPath + '?')) return
      try {
        if (this.selectedEntry && this.isDirectory(this.selectedEntry)) {
          await this.jsonPost('/ops/rmdir', { path: this.selectedPath })
        } else {
          await this.jsonPost('/ops/unlink', { path: this.selectedPath })
        }
        this.selectedPath = ''
        this.selectedEntry = null
        this.editor = ''
        await this.refreshAll()
      } catch (error) {
        this.fail('Remove failed', error)
      }
    },
    async removeCurrentDirectory() {
      if (!this.currentPath) return
      if (!confirm('Remove current directory ' + this.currentPath + '?')) return
      try {
        await this.jsonPost('/ops/rmdir', { path: this.currentPath })
        this.goUp()
      } catch (error) {
        this.fail('Directory removal failed', error)
      }
    },
    goUp() {
      if (!this.currentPath) return this.loadDirectory('')
      if (this.isSharedMode && this.currentPath === this.shareRootPath) return this.loadDirectory(this.shareRootPath)
      const next = this.currentPath.split('/').slice(0, -1).join('/')
      if (this.isSharedMode && !this.isWithinShareRoot(next)) return this.loadDirectory(this.shareRootPath)
      this.loadDirectory(next)
    },
    async deleteProject() {
      if (this.isSharedMode) return
      if (!this.project) return
      if (!confirm('Delete project ' + this.project + '?')) return
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project), { method: 'DELETE' })
        this.entries = []
        this.shares = []
        this.revisions = []
        this.projectStats = {}
        this.selectedPath = ''
        this.selectedEntry = null
        this.editor = ''
        this.statusText = 'Project deleted.'
      } catch (error) {
        this.fail('Project deletion failed', error)
      }
    }
  }
}`

func (h *restHandler) writeHTML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
