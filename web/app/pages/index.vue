<script setup lang="ts">
import { collectDroppedFiles } from '~/composables/use-upload-traversal'

const consoleStore = useConsole()
const {
  project,
  currentPath,
  entries,
  busy,
  authEnabled,
  isAdmin,
  isSharedView,
  canWrite,
  lockedProject,
} = consoleStore

const drawerOpen = ref(false)
const projectInput = ref('')
const { ask } = useConfirm()
const toasts = useToasts()

onMounted(() => {
  void consoleStore.init()
})

async function loadProject() {
  if (await consoleStore.loadProject(projectInput.value)) drawerOpen.value = false
}

async function navigate(path: string) {
  await consoleStore.loadDirectory(path)
  drawerOpen.value = false
}

async function selectEntry(entry: Parameters<typeof consoleStore.selectEntry>[0]) {
  // Selection only; keep drawer open on desktop for multi-select
  await consoleStore.focusEntry(entry)
}

async function openEntry(entry: Parameters<typeof consoleStore.selectEntry>[0]) {
  await consoleStore.selectEntry(entry)
  drawerOpen.value = false
}

function onSelect(entry: Parameters<typeof consoleStore.selectEntry>[0]) {
  void selectEntry(entry)
}

function onOpen(entry: Parameters<typeof consoleStore.selectEntry>[0]) {
  void openEntry(entry)
}

function onGlobalKey(event: KeyboardEvent) {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
    const target = event.target as HTMLElement | null
    if (target?.tagName === 'TEXTAREA') {
      event.preventDefault()
      void consoleStore.saveFile()
    }
  }
}

onMounted(() => window.addEventListener('keydown', onGlobalKey))
onUnmounted(() => window.removeEventListener('keydown', onGlobalKey))

function closeDrawer() {
  drawerOpen.value = false
}

const pruneScope = ref('all')
const pruneDryRun = ref(true)
const projectHealth = ref<null | { degraded: boolean, failure_streak: number, pending_depth: number }>(null)

async function refreshHealth() {
  const st = await consoleStore.projectStatus()
  projectHealth.value = st ? { degraded: st.degraded, failure_streak: st.failure_streak, pending_depth: st.pending_depth } : null
}

async function runEnable() {
  const ok = await ask({
    title: 'Enable project',
    body: 'Clear the degraded latch and admit mutations again? Only do this after the backend recovered.',
    confirmLabel: 'Enable',
    danger: true,
  })
  if (!ok) return
  if (await consoleStore.enableProject()) await refreshHealth()
}

async function runPrune() {
  const scope = pruneScope.value
  const dry = pruneDryRun.value
  if (!dry) {
    const ok = await ask({
      title: `Prune ${scope}`,
      body: 'Reclaim storage now? Orphaned index objects, untracked assets, and chunk orphans are deleted. History compaction is git-backend only and collapses every older manifest into a single checkpoint revision (keep is fixed at 1; the server rejects larger values). This cannot be undone.',
      confirmLabel: 'Prune',
      danger: true,
    })
    if (!ok) return
  }
  // keep=1 is the only value the backend accepts: history compaction keeps
  // exactly one checkpoint, and keep is ignored by the objects/assets/chunks scopes.
  const result = await consoleStore.prune(scope, 1, dry)
  if (!result) return
  const parts = [
    `${result.deleted_objects} objects`,
    `${result.deleted_releases} releases`,
    `${result.deleted_assets} assets`,
  ]
  if (result.history_compacted) parts.push('history compacted')
  if (scope === 'chunks') parts.push(`${result.orphan_chunks ?? 0} orphan chunks (${result.collected_bytes ?? result.orphan_bytes ?? 0} bytes) of ${result.scanned_chunks ?? 0} scanned`)
  toasts.info(`${dry ? 'Would prune' : 'Pruned'} ${result.scope}: ${parts.join(', ')}`)
  for (const note of result.notes ?? []) toasts.info(note)
}

const { panels } = usePanelWidths()
const { uploadProgress } = consoleStore

// Main-page directory actions.
function newFolderHere() {
  consoleStore.openModal('mkdir', currentPath.value || '')
}

// ---- Uploads ----------------------------------------------------------------

const fileInput = ref<HTMLInputElement | null>(null)
const dirInput = ref<HTMLInputElement | null>(null)
const dragDepth = ref(0)

function pickFiles() {
  fileInput.value?.click()
}

function pickFolder() {
  dirInput.value?.click()
}

async function onPicked(event: Event) {
  const el = event.target as HTMLInputElement
  const files = Array.from(el.files ?? [])
  el.value = ''
  if (!files.length) return
  const items = files.map((file) => ({
    file,
    relPath: (file as File & { webkitRelativePath?: string }).webkitRelativePath || file.name,
  }))
  await consoleStore.uploadFiles(items, currentPath.value)
}

async function onDrop(event: DragEvent) {
  dragDepth.value = 0
  if (!canWrite.value) return
  const out = await collectDroppedFiles(event.dataTransfer ?? null)
  if (out.length) await consoleStore.uploadFiles(out, currentPath.value)
}
</script>

<template>
  <div class="flex min-h-dvh flex-col lg:h-dvh lg:overflow-hidden">
    <!-- Top bar -->
    <header class="sticky top-0 z-30 flex h-14 shrink-0 items-center gap-2 border-b border-hair bg-shell/95 px-3 backdrop-blur sm:px-4">
      <button
        type="button"
        class="btn btn-sm px-2.5 lg:hidden"
        aria-label="Open menu"
        :aria-expanded="drawerOpen"
        @click="drawerOpen = true"
      >
        ☰
      </button>
      <span class="hidden font-mono text-sm font-semibold lg:inline">StorHub</span>

      <PathBar :project="project" :path="currentPath" @navigate="navigate" />

      <div class="ml-auto flex shrink-0 items-center gap-2">
        <span v-if="busy" class="chip animate-pulse motion-reduce:animate-none" role="status">working…</span>
      </div>
    </header>

    <div class="flex min-h-0 flex-1">
      <!-- First column: controls sidebar (resizable at lg+) -->
      <SideDrawer :open="drawerOpen" :width="panels.sidebar" @close="closeDrawer">
        <div class="mb-5 flex items-center justify-between lg:hidden">
          <span class="font-mono text-sm font-semibold">StorHub</span>
          <button type="button" class="btn btn-sm" aria-label="Close menu" @click="closeDrawer">✕</button>
        </div>

        <!-- Project: hidden entirely when pinned by the server -->
        <section v-if="!lockedProject" class="space-y-3 border-b border-hair pb-4">
          <label class="block">
            <span class="field-label">Project</span>
            <input
              v-model.trim="projectInput"
              type="text"
              placeholder="demo-project"
              autocomplete="off"
              autocapitalize="off"
              spellcheck="false"
              class="input font-mono"
              :disabled="isSharedView"
              @keydown.enter.prevent="loadProject"
            >
          </label>
          <div class="grid grid-cols-2 gap-2">
            <button class="btn btn-solid" :disabled="isSharedView || !projectInput" @click="loadProject">
              Load
            </button>
            <button class="btn" :disabled="!project || busy" @click="consoleStore.refreshAll()">
              Refresh
            </button>
          </div>
        </section>

        <!-- Auth -->
        <section v-if="authEnabled && !consoleStore.token.value && !isSharedView" class="border-b border-hair py-4">
          <LoginCard />
        </section>
        <section v-else-if="authEnabled && consoleStore.token.value && !isSharedView" class="space-y-3 border-b border-hair py-4 ">
          <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Account</h2>
          <dl class="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 text-sm">
            <dt class="text-xs leading-6 text-mist">user</dt>
            <dd class="font-mono text-xs leading-6">{{ consoleStore.principal.value?.username }}</dd>
            <dt class="text-xs leading-6 text-mist">uid</dt>
            <dd class="font-mono text-xs leading-6">{{ consoleStore.principal.value?.uid }}</dd>
            <dt class="text-xs leading-6 text-mist">gid</dt>
            <dd class="font-mono text-xs leading-6">{{ consoleStore.principal.value?.primary_gid }}</dd>
            <dt v-if="consoleStore.principal.value?.groups?.length" class="text-xs leading-6 text-mist">groups</dt>
            <dd
              v-if="consoleStore.principal.value?.groups?.length"
              class="truncate font-mono text-xs leading-6"
              :title="(consoleStore.principal.value.groups ?? []).join(', ')"
            >
              {{ (consoleStore.principal.value.groups ?? []).join(',') }}
            </dd>
            <dt class="text-xs leading-6 text-mist">role</dt>
            <dd class="font-mono text-xs leading-6">
              {{ consoleStore.principal.value?.admin ? 'admin' : 'user' }}
            </dd>
          </dl>
          <button class="btn btn-sm w-full" @click="consoleStore.logout()">Sign out</button>
        </section>

        <!-- Stats: hidden in shared view (no dashes) -->
        <section v-if="!isSharedView" class="space-y-2.5 border-b border-hair py-4">
          <StatsGrid :stats="consoleStore.stats.value" />
          <div v-if="isAdmin" class="space-y-1.5">
            <div class="flex items-center gap-2 text-xs">
              <span class="font-mono" :title="`streak ${projectHealth?.failure_streak ?? 0}, pending ${projectHealth?.pending_depth ?? 0}`">
                health: {{ projectHealth === null ? 'unknown' : projectHealth.degraded ? 'degraded' : 'healthy' }}
              </span>
              <button
                class="btn btn-xs ml-auto"
                :disabled="busy || !project"
                title="Refresh project health"
                @click="refreshHealth"
              >
                Refresh
              </button>
              <button
                v-if="projectHealth?.degraded"
                class="btn btn-xs"
                :disabled="busy || !project"
                title="Clear the degraded latch"
                @click="runEnable"
              >
                Enable…
              </button>
            </div>
            <div class="flex items-center gap-2">
              <select v-model="pruneScope" class="input input-sm flex-1 font-mono" :disabled="busy || !project" aria-label="Prune scope">
                <option value="all">all</option>
                <option value="objects">objects</option>
                <option value="assets">assets</option>
                <option value="history">history</option>
                <option value="chunks">chunks</option>
              </select>
              <label class="flex items-center gap-1 text-xs text-mist" title="Report what would be reclaimed without deleting">
                <input v-model="pruneDryRun" type="checkbox" >
                dry run
              </label>
            </div>
            <button
              class="btn btn-sm w-full"
              :disabled="busy || !project"
              title="Admin only: reclaim orphaned index objects, untracked assets, chunk orphans, or collapsed history"
              @click="runPrune"
            >
              {{ pruneDryRun ? 'Preview prune' : 'Prune now…' }}
            </button>
          </div>
          <ConfirmDeleteProject v-if="project" @deleted="projectInput = ''" />
        </section>

        <!-- Shares -->
        <section class="border-b border-hair py-4">
          <SharePanel />
        </section>

        <!-- Revisions -->
        <section v-if="!isSharedView" class="py-4">
          <RevisionPanel />
        </section>
      </SideDrawer>

      <!-- Gutter: sidebar | workspace -->
      <PanelGutter panel="sidebar" />

      <!-- Workspace: directory center, preview right (desktop); stacked on mobile -->
      <main class="min-w-0 flex-1 lg:h-full lg:overflow-hidden">
        <div
          class="grid h-full grid-cols-1 md:grid-cols-2 lg:[grid-template-columns:var(--dir-w,35rem)_auto_1fr]"
          :style="{ '--dir-w': `${panels.directory}px` }"
        >
          <!-- Directory pane with inline actions -->
          <section
            class="relative flex min-h-0 min-w-0 flex-col gap-3 p-4 md:border-r max-md:border-t max-md:border-hair lg:border-hair"
            :class="dragDepth > 0 ? 'ring-2 ring-inset ring-ember' : ''"
            @dragenter.prevent="dragDepth++"
            @dragover.prevent
            @dragleave.prevent="dragDepth = Math.max(0, dragDepth - 1)"
            @drop.prevent="onDrop"
          >
            <input ref="fileInput" type="file" multiple class="hidden" @change="onPicked">
            <input
              ref="dirInput"
              type="file"
              multiple
              class="hidden"
              :webkitdirectory="true"
              @change="onPicked"
            >

            <header class="flex items-center justify-between gap-2">
              <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Directory</h2>
              <div class="flex items-center gap-1.5">
                <button class="btn btn-sm px-2.5" :disabled="!currentPath || busy" title="Up one level" aria-label="Up one level" @click="consoleStore.goUp()">
                  ↑
                </button>
                <button
                  class="btn btn-sm px-2.5"
                  :disabled="!project || busy"
                  title="Refresh everything"
                  aria-label="Refresh"
                  @click="consoleStore.refreshAll()"
                >
                  ⟳
                </button>
                <button class="btn btn-sm" :disabled="!canWrite || busy" title="Upload files" @click="pickFiles">+ Upload</button>
                <button class="btn btn-sm px-2.5" :disabled="!canWrite || busy" title="Upload a folder (keeps structure)" aria-label="Upload folder" @click="pickFolder">⇪</button>
                <button class="btn btn-sm" :disabled="!canWrite || busy" @click="newFolderHere">+ Folder</button>
                <span class="ml-1 hidden text-xs text-mist sm:inline">{{ entries.length }}</span>
              </div>
            </header>

            <!-- Upload progress (byte-level across the whole batch) -->
            <div v-if="uploadProgress.active" class="space-y-1">
              <div class="h-1 overflow-hidden rounded bg-hair">
                <div
                  class="h-full bg-ember transition-[width] motion-reduce:transition-none"
                  :style="{ width: `${Math.round((uploadProgress.bytesDone / Math.max(1, uploadProgress.bytesTotal)) * 100)}%` }"
                />
              </div>
              <p class="truncate font-mono text-xs text-mist">
                Uploading {{ uploadProgress.done }}/{{ uploadProgress.total }} · {{ formatBytes(uploadProgress.bytesDone) }} / {{ formatBytes(uploadProgress.bytesTotal) }} · {{ uploadProgress.current }}
              </p>
            </div>

            <!-- Drop overlay -->
            <div
              v-if="dragDepth > 0 && canWrite"
              class="pointer-events-none absolute inset-2 z-10 grid place-items-center rounded-lg border-2 border-dashed border-ember bg-shell/80"
            >
              <p class="text-sm font-medium text-parchment">Drop files or folders to upload</p>
            </div>

            <EmptyState
              v-if="!entries.length"
              icon="🗂"
              title="Nothing here"
              :hint="project ? 'This directory is empty. Drop files to upload.' : 'Load a project to start browsing.'"
            />
            <div v-else class="min-h-0 flex-1 overflow-y-auto pr-1">
              <EntryList :entries="entries" @select="onSelect" @open="onOpen" />
            </div>
          </section>

          <!-- Gutter: directory | preview -->
          <PanelGutter panel="directory" />

          <!-- Preview pane -->
          <section class="flex min-h-0 min-w-0 flex-col p-4 max-md:border-t max-md:border-hair">
            <EditorPane />
          </section>
        </div>
      </main>
    </div>

    <!-- Modals & overlays -->
    <ActionModals />
    <ConfirmDialog />
    <ToastStack />
  </div>
</template>
