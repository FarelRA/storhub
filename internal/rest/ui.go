package rest

import "net/http"

const uiDocument = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>StorHub</title>
  <link rel="stylesheet" href="/ui/styles.css">
  <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.9/dist/cdn.min.js"></script>
  <script src="/ui/config.js"></script>
  <script src="/ui/app.js"></script>
</head>
<body x-data="storhubApp()" x-init="init()">
  <div class="app shared-mode" :class="{'is-shared': isSharedMode}">
    <header class="header">
      <div class="header-brand">
        <div class="logo-mark">SH</div>
        <div>
          <div class="eyebrow">Built-In Browser</div>
          <div class="logo-title">StorHub</div>
        </div>
      </div>
      <div class="header-main">
        <nav class="breadcrumb" x-show="project">
          <button class="crumb home" @click="navigateHome()" type="button" x-text="project"></button>
          <template x-for="(part, idx) in breadcrumbParts()" :key="part.path + ':' + idx">
            <span class="crumb-wrap">
              <span class="crumb-sep">/</span>
              <button class="crumb" @click="navigateTo(part.path)" type="button" x-text="part.label"></button>
            </span>
          </template>
        </nav>
        <div class="header-status" x-text="status"></div>
      </div>
      <div class="header-actions">
        <div class="shared-badge" x-show="isSharedMode">Shared View</div>
        <template x-if="!isSharedMode && config.authEnabled && !token">
          <button class="btn btn-primary" @click="openModal('login')" type="button">Sign In</button>
        </template>
        <template x-if="!isSharedMode && config.authEnabled && token">
          <button class="btn btn-ghost" @click="logout()" type="button">Sign Out</button>
        </template>
      </div>
    </header>

    <div class="workspace">
      <aside class="sidebar" x-show="!isSharedMode">
        <section class="panel">
          <div class="panel-title">Open Project</div>
          <label class="field">
            <span>Name</span>
            <input class="input" x-model.trim="project" @keydown.enter.prevent="openProject()" type="text" placeholder="my-project">
          </label>
          <button class="btn btn-primary btn-block" @click="openProject()" type="button">Load Project</button>
        </section>

        <section class="panel" x-show="project">
          <div class="panel-title">Overview</div>
          <div class="stats-grid">
            <div class="stat-card">
              <div class="stat-value" x-text="formatNumber(projectStats.files)"></div>
              <div class="stat-label">Files</div>
            </div>
            <div class="stat-card">
              <div class="stat-value" x-text="formatNumber(projectStats.directories)"></div>
              <div class="stat-label">Folders</div>
            </div>
            <div class="stat-card">
              <div class="stat-value" x-text="formatBytes(projectStats.bytes)"></div>
              <div class="stat-label">Storage</div>
            </div>
            <div class="stat-card">
              <div class="stat-value" x-text="formatNumber(projectStats.assets)"></div>
              <div class="stat-label">Assets</div>
            </div>
          </div>
        </section>

        <section class="panel" x-show="selectedEntry">
          <div class="panel-title">Selection</div>
          <div class="selection-card">
            <div class="selection-name" x-text="selectedEntry ? selectedEntry.name : ''"></div>
            <div class="selection-path mono" x-text="selectedPath"></div>
            <div class="selection-meta">
              <span x-text="selectedEntry && selectedEntry.isDir ? 'Folder' : getFileType(selectedEntry || {})"></span>
              <span x-show="selectedEntry && !selectedEntry.isDir" x-text="selectedEntry ? formatBytes(selectedEntry.size) : ''"></span>
            </div>
          </div>
          <div class="action-stack">
            <button class="btn btn-secondary btn-block" @click="downloadSelected()" :disabled="!canDownload()" type="button">Download</button>
            <button class="btn btn-secondary btn-block" @click="shareSelected()" :disabled="!canShare()" type="button">Create Share Link</button>
            <button class="btn btn-secondary btn-block" @click="openModal('rename')" :disabled="!selectedEntry" type="button">Rename</button>
            <button class="btn btn-danger btn-block" @click="removeSelected()" :disabled="!selectedEntry" type="button">Delete</button>
          </div>
        </section>
      </aside>

      <main class="main">
        <div class="toolbar">
          <div class="toolbar-group">
            <button class="btn btn-icon" @click="goUp()" :disabled="!canGoUp()" type="button" title="Go up">Up</button>
            <button class="btn btn-icon" @click="refreshAll()" type="button" title="Refresh">Refresh</button>
            <div class="view-toggle">
              <button class="toggle" :class="{'active': viewMode === 'list'}" @click="viewMode = 'list'" type="button">List</button>
              <button class="toggle" :class="{'active': viewMode === 'grid'}" @click="viewMode = 'grid'" type="button">Grid</button>
            </div>
          </div>
          <div class="toolbar-group" x-show="!isSharedMode">
            <button class="btn btn-secondary" @click="pickUpload()" type="button">Upload</button>
            <button class="btn btn-secondary" @click="openModal('create-file')" type="button">New File</button>
            <button class="btn btn-secondary" @click="openModal('mkdir')" type="button">New Folder</button>
          </div>
        </div>

        <div class="content-shell">
          <section class="browser-panel">
            <div class="browser-head">
              <div>
                <div class="panel-title">Files</div>
                <div class="panel-subtitle" x-text="locationLabel()"></div>
              </div>
              <div class="count-chip" x-text="entries.length + ' items'"></div>
            </div>

            <div class="file-list" :class="'view-' + viewMode" x-show="entries.length">
              <template x-for="entry in entries" :key="entry.path">
                <button class="file-item" :class="{'selected': selectedPath === entry.path}" @click="select(entry)" @dblclick="openEntry(entry)" type="button">
                  <div class="file-icon" x-text="getFileIcon(entry)"></div>
                  <div class="file-body">
                    <div class="file-name" x-text="entry.name"></div>
                    <div class="file-path mono" x-text="entry.path"></div>
                  </div>
                  <div class="file-meta">
                    <span x-text="entry.isDir ? 'Folder' : getFileType(entry)"></span>
                    <span x-text="entry.isDir ? '--' : formatBytes(entry.size)"></span>
                  </div>
                </button>
              </template>
            </div>

            <div class="empty-state" x-show="project && !entries.length">
              <div class="empty-title">Nothing here yet</div>
              <div class="empty-copy" x-show="!isSharedMode">Create a file or folder in this location.</div>
              <div class="empty-copy" x-show="isSharedMode">This shared location has no visible entries.</div>
            </div>

            <div class="empty-state" x-show="!project && !isSharedMode">
              <div class="empty-title">Open a project</div>
              <div class="empty-copy">Use the left panel to browse StorHub like a normal file manager.</div>
            </div>
          </section>

          <aside class="preview-panel">
            <div class="preview-head">
              <div>
                <div class="panel-title">Preview</div>
                <div class="panel-subtitle" x-text="selectedEntry ? selectedEntry.name : 'No selection'"></div>
              </div>
              <button class="btn btn-ghost" @click="closePreview()" :disabled="!selectedEntry" type="button">Clear</button>
            </div>

            <div class="preview-empty" x-show="!selectedEntry">Pick a file to inspect it.</div>
            <div class="preview-empty" x-show="selectedEntry && selectedEntry.isDir">Open the folder or choose a file inside it.</div>

            <template x-if="selectedEntry && !selectedEntry.isDir && preview.kind === 'image'">
              <img class="preview-image" :src="preview.url" alt="preview">
            </template>
            <template x-if="selectedEntry && !selectedEntry.isDir && preview.kind === 'audio'">
              <audio class="preview-media" :src="preview.url" controls></audio>
            </template>
            <template x-if="selectedEntry && !selectedEntry.isDir && preview.kind === 'video'">
              <video class="preview-media" :src="preview.url" controls></video>
            </template>
            <template x-if="selectedEntry && !selectedEntry.isDir && preview.kind === 'text'">
              <textarea class="editor" x-model="editor" spellcheck="false"></textarea>
            </template>
            <div class="preview-empty" x-show="selectedEntry && !selectedEntry.isDir && preview.kind === 'none'">No inline preview for this file type.</div>

            <div class="preview-actions" x-show="selectedEntry && !selectedEntry.isDir">
              <button class="btn btn-secondary" @click="downloadSelected()" :disabled="!canDownload()" type="button">Download</button>
              <button class="btn btn-primary" @click="saveSelected()" :disabled="!canSave()" type="button">Save Changes</button>
            </div>
          </aside>
        </div>
      </main>
    </div>
  </div>

  <input x-ref="uploader" type="file" class="hidden-input" @change="uploadPicked($event)">

  <div class="modal-backdrop" x-show="modal.open" x-transition.opacity @click="closeModal()">
    <div class="modal" @click.stop>
      <div class="modal-head">
        <div class="modal-title" x-text="modal.title"></div>
        <button class="btn btn-ghost" @click="closeModal()" type="button">Close</button>
      </div>
      <div class="modal-body">
        <template x-if="modal.kind === 'login'">
          <form class="form" @submit.prevent="login()">
            <label class="field">
              <span>Username</span>
              <input class="input" x-model.trim="auth.username" type="text" autofocus>
            </label>
            <label class="field">
              <span>Password</span>
              <input class="input" x-model="auth.password" type="password">
            </label>
            <div class="modal-actions">
              <button class="btn btn-primary" type="submit">Sign In</button>
              <button class="btn btn-ghost" @click="closeModal()" type="button">Cancel</button>
            </div>
          </form>
        </template>

        <template x-if="modal.kind === 'share'">
          <div class="form">
            <label class="field">
              <span>Share URL</span>
              <input class="input mono" :value="share.url" type="text" readonly>
            </label>
            <div class="share-hint" x-text="share.download ? 'This share can also be downloaded directly.' : 'This share opens in the browser UI only.'"></div>
            <div class="modal-actions">
              <button class="btn btn-primary" @click="copyShare()" type="button">Copy Link</button>
              <button class="btn btn-secondary" @click="openShare()" type="button">Open Link</button>
              <button class="btn btn-ghost" @click="closeModal()" type="button">Done</button>
            </div>
          </div>
        </template>

        <template x-if="modal.kind !== 'login' && modal.kind !== 'share'">
          <form class="form" @submit.prevent="submitModal()">
            <template x-for="field in modal.fields" :key="field.key">
              <label class="field">
                <span x-text="field.label"></span>
                <input class="input" x-model.trim="modal.form[field.key]" :type="field.type || 'text'" :placeholder="field.placeholder || ''">
              </label>
            </template>
            <div class="modal-actions">
              <button class="btn btn-primary" type="submit">Apply</button>
              <button class="btn btn-ghost" @click="closeModal()" type="button">Cancel</button>
            </div>
          </form>
        </template>
      </div>
    </div>
  </div>

  <div class="toast" x-show="toast.show" x-transition.opacity x-text="toast.message"></div>
</body>
</html>
`

const uiStyles = `:root{
  --bg:#f4f0e8;
  --bg-accent:#e8dfcf;
  --panel:#fbf8f2;
  --panel-strong:#ffffff;
  --line:#d5cab8;
  --line-strong:#b9ab95;
  --text:#2f261d;
  --muted:#756754;
  --accent:#1f6c5c;
  --accent-strong:#154d42;
  --danger:#a24334;
  --shadow:0 18px 50px rgba(63,44,22,0.12);
  --radius:18px;
}
*{box-sizing:border-box}
html,body{min-height:100%;margin:0}
body{
  background:radial-gradient(circle at top left,#fff8ea 0%,var(--bg) 42%,#efe4d2 100%);
  color:var(--text);
  font:14px/1.5 "IBM Plex Sans","Avenir Next","Segoe UI",sans-serif;
}
button,input,textarea{font:inherit}
button{cursor:pointer}
a{color:inherit}
.mono{font-family:"IBM Plex Mono","SFMono-Regular",monospace}
.app{min-height:100vh;display:flex;flex-direction:column}
.header{
  display:grid;
  grid-template-columns:auto 1fr auto;
  gap:18px;
  align-items:center;
  padding:18px 24px;
  border-bottom:1px solid rgba(47,38,29,0.08);
  backdrop-filter:blur(18px);
  background:rgba(251,248,242,0.88);
  position:sticky;
  top:0;
  z-index:10;
}
.header-brand{display:flex;align-items:center;gap:12px}
.logo-mark{
  width:44px;height:44px;border-radius:14px;
  display:grid;place-items:center;
  background:linear-gradient(135deg,var(--accent),#5f9470);
  color:#fff;font-weight:700;letter-spacing:0.08em;
  box-shadow:0 12px 24px rgba(31,108,92,0.2);
}
.eyebrow{font-size:11px;text-transform:uppercase;letter-spacing:0.14em;color:var(--muted)}
.logo-title{font-size:20px;font-weight:650}
.header-main{min-width:0}
.breadcrumb{display:flex;align-items:center;flex-wrap:wrap;gap:6px;font-size:13px}
.crumb-wrap{display:flex;align-items:center;gap:6px}
.crumb,.crumb.home{
  border:0;background:none;color:var(--muted);padding:4px 8px;border-radius:999px;
}
.crumb:hover,.crumb.home:hover{background:rgba(31,108,92,0.08);color:var(--accent-strong)}
.crumb-sep{color:#a3947d}
.header-status{margin-top:6px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.header-actions{display:flex;align-items:center;gap:10px}
.shared-badge{
  padding:8px 12px;border-radius:999px;background:#e0efe8;color:var(--accent-strong);
  font-weight:600;letter-spacing:0.04em;text-transform:uppercase;font-size:11px;
}
.workspace{display:grid;grid-template-columns:300px minmax(0,1fr);gap:20px;padding:20px;min-height:0;flex:1}
.sidebar{display:flex;flex-direction:column;gap:16px}
.panel,.browser-panel,.preview-panel,.modal{
  background:rgba(255,255,255,0.78);
  border:1px solid rgba(47,38,29,0.1);
  border-radius:var(--radius);
  box-shadow:var(--shadow);
}
.panel{padding:18px}
.panel-title{font-size:12px;text-transform:uppercase;letter-spacing:0.14em;color:var(--muted);margin-bottom:12px}
.panel-subtitle{color:var(--muted);font-size:13px;word-break:break-word}
.field{display:flex;flex-direction:column;gap:6px;margin-bottom:12px}
.field span{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em}
.input,.editor{
  width:100%;border:1px solid var(--line);border-radius:14px;background:var(--panel-strong);
  color:var(--text);padding:12px 14px;outline:none;
}
.input:focus,.editor:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(31,108,92,0.12)}
.btn{
  border:1px solid transparent;border-radius:999px;padding:10px 14px;background:transparent;color:var(--text);
}
.btn:hover:not(:disabled){transform:translateY(-1px)}
.btn:disabled{opacity:0.45;cursor:not-allowed}
.btn-primary{background:var(--accent);color:#fff}
.btn-primary:hover:not(:disabled){background:var(--accent-strong)}
.btn-secondary{background:var(--panel-strong);border-color:var(--line)}
.btn-secondary:hover:not(:disabled),.btn-ghost:hover:not(:disabled),.btn-icon:hover:not(:disabled){background:var(--bg-accent)}
.btn-danger{background:#f7e2de;color:var(--danger)}
.btn-danger:hover:not(:disabled){background:#f1d4cd}
.btn-ghost,.btn-icon{background:transparent;border-color:var(--line)}
.btn-block{width:100%}
.stats-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.stat-card{padding:14px;border:1px solid var(--line);border-radius:16px;background:var(--panel)}
.stat-value{font-size:18px;font-weight:650}
.stat-label{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em}
.selection-card{padding:14px;border-radius:16px;background:var(--panel);border:1px solid var(--line)}
.selection-name{font-size:16px;font-weight:650;word-break:break-word}
.selection-path{margin-top:6px;color:var(--muted);word-break:break-word}
.selection-meta{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px;color:var(--muted);font-size:13px}
.action-stack{display:grid;gap:10px;margin-top:14px}
.main{display:flex;flex-direction:column;gap:16px;min-width:0}
.toolbar{
  display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;
}
.toolbar-group{display:flex;gap:10px;flex-wrap:wrap}
.view-toggle{display:flex;padding:4px;border:1px solid var(--line);border-radius:999px;background:rgba(255,255,255,0.6)}
.toggle{border:0;background:transparent;border-radius:999px;padding:8px 12px;color:var(--muted)}
.toggle.active{background:var(--accent);color:#fff}
.content-shell{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(300px,0.9fr);gap:18px;min-height:0;flex:1}
.browser-panel,.preview-panel{padding:18px;display:flex;flex-direction:column;min-height:0}
.browser-head,.preview-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:16px}
.count-chip{padding:6px 10px;border-radius:999px;background:var(--bg-accent);color:var(--muted);font-size:12px}
.file-list{display:grid;gap:10px;min-height:0;overflow:auto;padding-right:2px}
.file-list.view-grid{grid-template-columns:repeat(auto-fill,minmax(220px,1fr))}
.file-item{
  display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:14px;
  padding:14px;border:1px solid var(--line);border-radius:18px;background:var(--panel);text-align:left;
}
.file-item:hover{border-color:var(--line-strong);background:#fffdf8}
.file-item.selected{border-color:var(--accent);background:#eef8f5;box-shadow:0 0 0 3px rgba(31,108,92,0.1)}
.file-icon{width:42px;height:42px;border-radius:14px;display:grid;place-items:center;background:#fff;font-size:20px;border:1px solid rgba(47,38,29,0.08)}
.file-body{min-width:0}
.file-name{font-size:15px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.file-path{font-size:12px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.file-meta{display:flex;flex-direction:column;align-items:flex-end;color:var(--muted);font-size:12px;gap:4px}
.preview-panel{gap:14px}
.preview-empty{
  display:grid;place-items:center;min-height:220px;border:1px dashed var(--line-strong);border-radius:18px;
  color:var(--muted);background:rgba(248,244,236,0.72);padding:20px;text-align:center;
}
.preview-image,.preview-media{max-width:100%;max-height:440px;border-radius:16px;border:1px solid var(--line);background:#fff}
.editor{min-height:320px;resize:vertical;font:13px/1.6 "IBM Plex Mono","SFMono-Regular",monospace;background:#fdfcf9}
.preview-actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:auto}
.empty-state{
  min-height:220px;display:grid;place-items:center;text-align:center;padding:24px;border:1px dashed var(--line-strong);
  border-radius:20px;background:rgba(248,244,236,0.72);
}
.empty-title{font-size:22px;font-weight:650}
.empty-copy{margin-top:8px;color:var(--muted);max-width:34ch}
.hidden-input{display:none}
.modal-backdrop{
  position:fixed;inset:0;background:rgba(47,38,29,0.38);display:grid;place-items:center;padding:20px;z-index:20;
}
.modal{width:min(520px,100%);padding:18px}
.modal-head,.modal-actions{display:flex;justify-content:space-between;align-items:center;gap:10px}
.modal-title{font-size:18px;font-weight:650}
.modal-body{margin-top:16px}
.form{display:grid;gap:12px}
.share-hint{color:var(--muted);font-size:13px}
.toast{
  position:fixed;right:18px;bottom:18px;padding:12px 16px;border-radius:14px;background:#2f261d;color:#fff;z-index:30;
  box-shadow:0 16px 32px rgba(47,38,29,0.26);
}
@media (max-width: 1080px){
  .workspace,.content-shell{grid-template-columns:1fr}
  .preview-panel{order:-1}
}
@media (max-width: 760px){
  .header{grid-template-columns:1fr;align-items:flex-start}
  .workspace{padding:14px}
  .sidebar{order:2}
  .file-item{grid-template-columns:auto minmax(0,1fr)}
  .file-meta{grid-column:2;align-items:flex-start}
}
`

const uiScript = `window.storhubApp = function () {
  return {
    config: window.STORHUB_UI_CONFIG || { basePath: '/api/v1', authEnabled: false, sharePath: '/shares/' },
    viewMode: 'list',
    token: localStorage.getItem('storhub.token') || '',
    shareToken: '',
    shareRootPath: '',
    shareRootIsDir: false,
    project: '',
    currentPath: '',
    selectedPath: '',
    selectedEntry: null,
    entries: [],
    projectStats: { files: 0, directories: 0, bytes: 0, assets: 0 },
    status: 'Ready',
    editor: '',
    preview: { kind: 'none', url: '', revoke: null },
    share: { url: '', download: true },
    auth: { username: '', password: '' },
    modal: { open: false, title: '', kind: '', fields: [], form: {} },
    toast: { show: false, message: '', timer: null },

    get isSharedMode() {
      return !!this.shareToken;
    },

    async init() {
      const params = new URLSearchParams(window.location.search);
      const shareToken = params.get('share');
      if (shareToken) {
        this.shareToken = shareToken;
        this.token = shareToken;
        await this.bootstrapSharedMode();
      }
    },

    headers(extra) {
      const headers = Object.assign({}, extra || {});
      if (this.token) {
        headers.Authorization = 'Bearer ' + this.token;
      }
      return headers;
    },

    normalizePath(value) {
      return String(value || '').trim().replace(/^\/+|\/+$/g, '');
    },

    joinPath(basePath, nextPart) {
      const base = this.normalizePath(basePath);
      const next = this.normalizePath(nextPart);
      if (!base) {
        return next;
      }
      if (!next) {
        return base;
      }
      return base + '/' + next;
    },

    parentPath(value) {
      const current = this.normalizePath(value);
      if (!current) {
        return '';
      }
      const parts = current.split('/');
      parts.pop();
      return parts.join('/');
    },

    pathParts(value) {
      const current = this.normalizePath(value);
      return current ? current.split('/') : [];
    },

    withinSharedRoot(targetPath) {
      if (!this.isSharedMode) {
        return true;
      }
      const target = this.normalizePath(targetPath);
      const root = this.normalizePath(this.shareRootPath);
      if (!root) {
        return true;
      }
      if (this.shareRootIsDir) {
        return target === root || target.indexOf(root + '/') === 0;
      }
      return target === root;
    },

    decodeShareToken(token) {
      try {
        const parts = String(token || '').split('.');
        if (parts.length !== 2) {
          return null;
        }
        let payload = parts[0].replace(/-/g, '+').replace(/_/g, '/');
        while (payload.length % 4) {
          payload += '=';
        }
        const decoded = atob(payload);
        const bytes = new Uint8Array(decoded.length);
        for (let i = 0; i < decoded.length; i += 1) {
          bytes[i] = decoded.charCodeAt(i);
        }
        return JSON.parse(new TextDecoder().decode(bytes));
      } catch (error) {
        return null;
      }
    },

    normalizeEntry(entry) {
      return Object.assign({}, entry, {
        path: this.normalizePath(entry.path),
        isDir: !!(entry.isDir || entry.is_dir),
        isSymlink: !!(entry.isSymlink || entry.is_symlink)
      });
    },

    locationLabel() {
      if (!this.project) {
        return 'No project selected';
      }
      if (!this.currentPath) {
        return '/';
      }
      return this.currentPath;
    },

    breadcrumbParts() {
      const current = this.normalizePath(this.currentPath || this.shareRootPath);
      const parts = this.pathParts(current);
      if (!this.isSharedMode) {
        return parts.map((part, idx) => ({
          label: part,
          path: parts.slice(0, idx + 1).join('/')
        }));
      }
      const rootParts = this.pathParts(this.shareRootPath);
      const visible = [];
      if (rootParts.length) {
        visible.push({ label: rootParts.join('/'), path: this.shareRootPath });
      }
      for (let i = rootParts.length; i < parts.length; i += 1) {
        visible.push({
          label: parts[i],
          path: parts.slice(0, i + 1).join('/')
        });
      }
      return visible;
    },

    formatNumber(value) {
      if (value === undefined || value === null || value === '') {
        return '-';
      }
      return Number(value).toLocaleString();
    },

    formatBytes(bytes) {
      const value = Number(bytes || 0);
      if (!value) {
        return '0 B';
      }
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
      const amount = value / Math.pow(1024, index);
      return (Math.round(amount * 10) / 10) + ' ' + units[index];
    },

    getFileIcon(entry) {
      if (!entry) {
        return '[ ]';
      }
      if (entry.isDir) {
        return '[D]';
      }
      const name = String(entry.name || '').toLowerCase();
      if (/\.(png|jpg|jpeg|gif|bmp|svg|webp)$/.test(name)) return '[I]';
      if (/\.(mp4|mov|avi|mkv|webm)$/.test(name)) return '[V]';
      if (/\.(mp3|wav|ogg|flac|m4a)$/.test(name)) return '[A]';
      if (/\.(txt|md|json|yaml|yml|xml|js|ts|go|py|java|c|cpp|h|css|html)$/.test(name)) return '[T]';
      if (/\.(zip|gz|tar|rar|7z)$/.test(name)) return '[Z]';
      return '[F]';
    },

    getFileType(entry) {
      if (!entry || entry.isDir) {
        return 'Folder';
      }
      const name = String(entry.name || '').toLowerCase();
      const bits = name.split('.');
      if (bits.length <= 1) {
        return entry.isSymlink ? 'Symlink' : 'File';
      }
      return bits.pop().toUpperCase();
    },

    async api(url, options) {
      const opts = options || {};
      const res = await fetch(url, Object.assign({}, opts, { headers: this.headers(opts.headers || {}) }));
      const contentType = res.headers.get('content-type') || '';
      let payload = null;
      if (contentType.indexOf('application/json') !== -1) {
        payload = await res.json().catch(function () { return null; });
      } else {
        payload = await res.text();
      }
      if (!res.ok) {
        const message = payload && payload.error ? payload.error.message : (typeof payload === 'string' && payload.trim() ? payload.trim() : res.statusText);
        throw new Error(message || 'Request failed');
      }
      return { res: res, payload: payload };
    },

    async fetchBlobURL(url) {
      const res = await fetch(url, { headers: this.headers() });
      if (!res.ok) {
        const text = await res.text().catch(function () { return ''; });
        throw new Error(text || res.statusText || 'Failed to fetch asset');
      }
      const blob = await res.blob();
      const objectURL = URL.createObjectURL(blob);
      return {
        url: objectURL,
        revoke: function () {
          URL.revokeObjectURL(objectURL);
        }
      };
    },

    showToast(message) {
      if (this.toast.timer) {
        clearTimeout(this.toast.timer);
      }
      this.toast.message = message;
      this.toast.show = true;
      this.toast.timer = setTimeout(() => {
        this.toast.show = false;
      }, 3000);
    },

    clearPreview() {
      if (this.preview.revoke) {
        this.preview.revoke();
      }
      this.preview = { kind: 'none', url: '', revoke: null };
      this.editor = '';
    },

    clearSelection() {
      this.selectedPath = '';
      this.selectedEntry = null;
      this.clearPreview();
    },

    contentURL(targetPath) {
      return this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/content?path=' + encodeURIComponent(this.normalizePath(targetPath));
    },

    async loadNode(targetPath) {
      const response = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/nodes?path=' + encodeURIComponent(this.normalizePath(targetPath)));
      return this.normalizeEntry(response.payload.entry || {});
    },

    async bootstrapSharedMode() {
      const claims = this.decodeShareToken(this.shareToken);
      if (!claims || !claims.project) {
        this.status = 'Invalid share link';
        this.showToast('Failed to open share');
        return;
      }
      this.project = claims.project;
      this.shareRootPath = this.normalizePath(claims.path);
      this.share.download = claims.download !== false;
      this.status = 'Loading shared resource...';
      try {
        const entry = await this.loadNode(this.shareRootPath);
        this.shareRootIsDir = !!entry.isDir;
        if (entry.isDir) {
          this.currentPath = entry.path;
          this.clearSelection();
          await this.loadDirectory(false);
          this.status = 'Shared folder loaded';
          return;
        }
        this.currentPath = entry.path;
        this.entries = [entry];
        await this.select(entry);
        this.status = 'Shared file loaded';
      } catch (error) {
        this.entries = [];
        this.clearSelection();
        this.status = 'Failed to load share: ' + error.message;
        this.showToast('Failed to load shared resource');
      }
    },

    async openProject() {
      this.currentPath = '';
      this.clearSelection();
      await this.loadProject();
    },

    async loadProject() {
      if (!this.project) {
        this.status = 'Enter a project name';
        return;
      }
      if (this.isSharedMode) {
        await this.bootstrapSharedMode();
        return;
      }
      this.status = 'Loading project...';
      try {
        const response = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project));
        this.projectStats = response.payload.stats || this.projectStats;
        await this.loadDirectory(true);
        this.status = 'Project loaded';
      } catch (error) {
        this.entries = [];
        this.clearSelection();
        this.status = 'Failed to load project: ' + error.message;
      }
    },

    async loadDirectory(preserveSelection) {
      if (!this.project) {
        return;
      }
      if (this.isSharedMode && !this.shareRootIsDir && this.currentPath === this.shareRootPath) {
        const entry = await this.loadNode(this.shareRootPath);
        this.entries = [entry];
        if (preserveSelection && this.selectedPath === entry.path) {
          await this.select(entry);
        }
        return;
      }
      this.status = 'Loading directory...';
      try {
        const path = this.normalizePath(this.currentPath);
        const response = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/children?path=' + encodeURIComponent(path));
        this.entries = (response.payload.entries || []).map((entry) => this.normalizeEntry(entry));
        if (preserveSelection && this.selectedPath) {
          const match = this.entries.find((entry) => entry.path === this.selectedPath);
          if (match) {
            await this.select(match);
          } else {
            this.clearSelection();
          }
        } else {
          this.clearSelection();
        }
        this.status = this.entries.length + ' items';
      } catch (error) {
        this.entries = [];
        this.clearSelection();
        this.status = 'Failed to load directory: ' + error.message;
      }
    },

    async refreshAll() {
      if (this.isSharedMode) {
        await this.bootstrapSharedMode();
        return;
      }
      await this.loadProject();
    },

    async navigateHome() {
      if (this.isSharedMode) {
        await this.navigateTo(this.shareRootPath);
        return;
      }
      await this.navigateTo('');
    },

    async navigateTo(targetPath) {
      const next = this.normalizePath(targetPath);
      if (this.isSharedMode && !this.withinSharedRoot(next)) {
        this.showToast('That location is outside the shared area');
        return;
      }
      this.currentPath = next;
      if (this.isSharedMode && !this.shareRootIsDir && next === this.shareRootPath) {
        await this.loadDirectory(false);
        return;
      }
      await this.loadDirectory(false);
    },

    canGoUp() {
      if (!this.currentPath) {
        return false;
      }
      if (!this.isSharedMode) {
        return true;
      }
      if (!this.shareRootIsDir) {
        return false;
      }
      return this.normalizePath(this.currentPath) !== this.normalizePath(this.shareRootPath);
    },

    async goUp() {
      if (!this.canGoUp()) {
        return;
      }
      await this.navigateTo(this.parentPath(this.currentPath));
    },

    async select(entry) {
      const normalized = this.normalizeEntry(entry);
      this.selectedPath = normalized.path;
      this.selectedEntry = normalized;
      if (normalized.isDir) {
        this.clearPreview();
        return;
      }
      await this.loadPreview(normalized);
    },

    async openEntry(entry) {
      const normalized = this.normalizeEntry(entry);
      if (normalized.isDir) {
        await this.navigateTo(normalized.path);
        return;
      }
      await this.select(normalized);
    },

    closePreview() {
      this.clearSelection();
    },

    async loadPreview(entry) {
      this.clearPreview();
      const name = String(entry.name || '').toLowerCase();
      const url = this.contentURL(entry.path);
      try {
        if (/\.(txt|md|json|yaml|yml|xml|js|ts|go|py|java|c|cpp|h|css|html|svg)$/.test(name)) {
          const response = await this.api(url);
          this.editor = response.payload;
          this.preview.kind = 'text';
          return;
        }
        if (/\.(png|jpg|jpeg|gif|bmp|webp|svg)$/.test(name)) {
          const asset = await this.fetchBlobURL(url);
          this.preview = { kind: 'image', url: asset.url, revoke: asset.revoke };
          return;
        }
        if (/\.(mp3|wav|ogg|flac|m4a)$/.test(name)) {
          const asset = await this.fetchBlobURL(url);
          this.preview = { kind: 'audio', url: asset.url, revoke: asset.revoke };
          return;
        }
        if (/\.(mp4|mov|avi|mkv|webm)$/.test(name)) {
          const asset = await this.fetchBlobURL(url);
          this.preview = { kind: 'video', url: asset.url, revoke: asset.revoke };
          return;
        }
        this.preview.kind = 'none';
      } catch (error) {
        this.preview.kind = 'none';
        this.showToast('Preview failed: ' + error.message);
      }
    },

    canDownload() {
      return !!(this.selectedEntry && !this.selectedEntry.isDir);
    },

    async downloadSelected() {
      if (!this.canDownload()) {
        return;
      }
      try {
        const res = await fetch(this.contentURL(this.selectedPath), { headers: this.headers() });
        if (!res.ok) {
          throw new Error('Download failed');
        }
        const blob = await res.blob();
        const objectURL = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = objectURL;
        anchor.download = this.selectedEntry ? this.selectedEntry.name : 'download';
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(objectURL);
      } catch (error) {
        this.showToast(error.message || 'Download failed');
      }
    },

    canShare() {
      return !!(this.selectedEntry && !this.isSharedMode);
    },

    async shareSelected() {
      if (!this.canShare()) {
        return;
      }
      try {
        const response = await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/ops/share', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: this.selectedPath })
        });
        this.share.url = window.location.origin + response.payload.url;
        this.share.download = response.payload.download !== false;
        this.modal = { open: true, title: 'Share Link', kind: 'share', fields: [], form: {} };
      } catch (error) {
        this.showToast('Failed to create share link');
      }
    },

    copyShare() {
      if (!this.share.url) {
        return;
      }
      navigator.clipboard.writeText(this.share.url).then(() => {
        this.showToast('Link copied to clipboard');
      }).catch(() => {
        this.showToast('Clipboard copy failed');
      });
    },

    openShare() {
      if (!this.share.url) {
        return;
      }
      window.open(this.share.url, '_blank', 'noopener');
    },

    canSave() {
      return !!(this.selectedEntry && !this.selectedEntry.isDir && this.preview.kind === 'text' && !this.isSharedMode);
    },

    async saveSelected() {
      if (!this.canSave()) {
        return;
      }
      try {
        await this.api(this.contentURL(this.selectedPath), {
          method: 'PUT',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: this.editor
        });
        this.showToast('File saved');
        await this.loadDirectory(true);
      } catch (error) {
        this.showToast('Failed to save file');
      }
    },

    async removeSelected() {
      if (!this.selectedEntry || this.isSharedMode) {
        return;
      }
      if (!window.confirm('Delete ' + this.selectedEntry.name + '?')) {
        return;
      }
      const path = this.selectedPath;
      const endpoint = this.selectedEntry.isDir ? 'rmdir' : 'unlink';
      try {
        await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/ops/' + endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: path })
        });
        this.showToast('Deleted');
        await this.loadDirectory(false);
      } catch (error) {
        this.showToast('Failed to delete');
      }
    },

    pickUpload() {
      if (this.isSharedMode || !this.project) {
        return;
      }
      this.$refs.uploader.click();
    },

    async uploadPicked(event) {
      const file = event.target.files && event.target.files[0];
      if (!file) {
        return;
      }
      const targetPath = this.joinPath(this.currentPath, file.name);
      try {
        await this.api(this.contentURL(targetPath), {
          method: 'PUT',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: await file.arrayBuffer()
        });
        this.showToast('File uploaded');
        await this.loadDirectory(false);
      } catch (error) {
        this.showToast('Failed to upload file');
      } finally {
        event.target.value = '';
      }
    },

    openModal(kind) {
      if (kind === 'login') {
        this.modal = { open: true, title: 'Sign In', kind: 'login', fields: [], form: {} };
        return;
      }
      if (kind === 'mkdir') {
        this.modal = {
          open: true,
          title: 'New Folder',
          kind: kind,
          fields: [{ key: 'path', label: 'Folder name', placeholder: 'documents' }],
          form: { path: '' }
        };
        return;
      }
      if (kind === 'create-file') {
        this.modal = {
          open: true,
          title: 'New File',
          kind: kind,
          fields: [{ key: 'path', label: 'File name', placeholder: 'notes.txt' }],
          form: { path: '' }
        };
        return;
      }
      if (kind === 'rename' && this.selectedEntry) {
        this.modal = {
          open: true,
          title: 'Rename',
          kind: kind,
          fields: [{ key: 'new_path', label: 'New name or path', placeholder: this.selectedEntry.name }],
          form: { new_path: this.selectedEntry.name }
        };
      }
    },

    closeModal() {
      this.modal.open = false;
    },

    resolveCurrentPath(input) {
      return this.joinPath(this.currentPath, input);
    },

    resolveRenamePath(input) {
      const value = String(input || '').trim();
      if (!value) {
        return '';
      }
      if (value.indexOf('/') !== -1) {
        return this.normalizePath(value);
      }
      return this.joinPath(this.parentPath(this.selectedPath), value);
    },

    async submitModal() {
      try {
        if (this.modal.kind === 'mkdir') {
          await this.mkdir();
        } else if (this.modal.kind === 'create-file') {
          await this.createFile();
        } else if (this.modal.kind === 'rename') {
          await this.rename();
        }
        this.closeModal();
      } catch (error) {
        this.showToast(error.message || 'Action failed');
      }
    },

    async mkdir() {
      const targetPath = this.resolveCurrentPath(this.modal.form.path);
      if (!targetPath) {
        throw new Error('Folder name is required');
      }
      await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/ops/mkdir', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: targetPath })
      });
      this.showToast('Folder created');
      await this.loadDirectory(false);
    },

    async createFile() {
      const targetPath = this.resolveCurrentPath(this.modal.form.path);
      if (!targetPath) {
        throw new Error('File name is required');
      }
      await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/ops/create-file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: targetPath })
      });
      this.showToast('File created');
      await this.loadDirectory(false);
    },

    async rename() {
      const newPath = this.resolveRenamePath(this.modal.form.new_path);
      if (!newPath || !this.selectedPath) {
        throw new Error('New path is required');
      }
      await this.api(this.config.basePath + '/projects/' + encodeURIComponent(this.project) + '/ops/rename', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ old_path: this.selectedPath, new_path: newPath })
      });
      this.showToast('Renamed');
      this.selectedPath = newPath;
      await this.loadDirectory(true);
    },

    async login() {
      try {
        const response = await this.api(this.config.basePath + '/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this.auth)
        });
        this.token = response.payload.token;
        localStorage.setItem('storhub.token', this.token);
        this.auth.password = '';
        this.showToast('Signed in');
        this.closeModal();
        if (this.project) {
          await this.loadProject();
        }
      } catch (error) {
        this.showToast('Sign in failed');
      }
    },

    logout() {
      this.token = '';
      localStorage.removeItem('storhub.token');
      this.project = '';
      this.currentPath = '';
      this.entries = [];
      this.projectStats = { files: 0, directories: 0, bytes: 0, assets: 0 };
      this.clearSelection();
      this.showToast('Signed out');
    }
  };
};
`

func (h *restHandler) writeHTML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
