<script setup lang="ts">
const console_ = useConsole()
const {
  project,
  currentPath,
  selectedPath,
  entries,
  busy,
  authEnabled,
  isAdmin,
  isSharedView,
  sharedMode,
  canWrite,
  lockedProject,
} = console_

const drawerOpen = ref(false)
const projectInput = ref('')
const { ask } = useConfirm()

onMounted(() => {
  void console_.init()
})

async function loadProject() {
  if (await console_.loadProject(projectInput.value)) drawerOpen.value = false
}

async function navigate(path: string) {
  await console_.loadDirectory(path)
  drawerOpen.value = false
}

async function selectEntry(entry: Parameters<typeof console_.selectEntry>[0]) {
  await console_.selectEntry(entry)
  drawerOpen.value = false
}

function onGlobalKey(event: KeyboardEvent) {
  if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
    const target = event.target as HTMLElement | null
    if (target?.tagName === 'TEXTAREA') {
      event.preventDefault()
      void console_.saveFile()
    }
  }
}

onMounted(() => window.addEventListener('keydown', onGlobalKey))
onUnmounted(() => window.removeEventListener('keydown', onGlobalKey))

function closeDrawer() {
  drawerOpen.value = false
}

async function purge() {
  const ok = await ask({
    title: 'Purge untracked chunks',
    body: 'Delete release assets that are no longer referenced by any metadata revision? This frees storage but old revisions may lose their blobs.',
    confirmLabel: 'Purge',
    danger: true,
  })
  if (ok) await console_.purgeUntracked()
}

const { panels } = usePanelWidths()
const { uploadProgress } = console_

// Main-page directory actions.
function newFolderHere() {
  console_.openModal('mkdir', currentPath.value || '')
}

// ---- Uploads ----------------------------------------------------------------

const fileInput = ref<HTMLInputElement | null>(null)
const dirInput = ref<HTMLInputElement | null>(null)
const dragDepth = ref(0)

interface FsReader {
  readEntries(cb: (entries: FsEntry[]) => void, err?: (e: unknown) => void): void
}
interface FsEntry {
  name: string
  isFile: boolean
  isDirectory: boolean
  file(cb: (file: File) => void, err?: (e: unknown) => void): void
  createReader(): FsReader
}

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
  await console_.uploadFiles(items, currentPath.value)
}

async function walkEntry(entry: FsEntry, prefix: string, out: Array<{ file: File; relPath: string }>): Promise<void> {
  if (entry.isFile) {
    const file = await new Promise<File>((resolve, reject) => entry.file(resolve, reject))
    out.push({ file, relPath: prefix ? `${prefix}/${entry.name}` : entry.name })
    return
  }
  const reader = entry.createReader()
  for (;;) {
    const batch: FsEntry[] = await new Promise((resolve, reject) => reader.readEntries(resolve, reject))
    if (!batch.length) break
    const nextPrefix = prefix ? `${prefix}/${entry.name}` : entry.name
    for (const child of batch) await walkEntry(child, nextPrefix, out)
  }
}

async function onDrop(event: DragEvent) {
  dragDepth.value = 0
  if (!canWrite.value) return
  const items = Array.from(event.dataTransfer?.items ?? [])
  const out: Array<{ file: File; relPath: string }> = []
  let usedEntries = false
  for (const item of items) {
    const getter = (item as DataTransferItem & { webkitGetAsEntry?: () => FsEntry | null }).webkitGetAsEntry
    const entry = typeof getter === 'function' ? getter.call(item) : null
    if (!entry) continue
    usedEntries = true
    await walkEntry(entry, '', out)
  }
  if (!usedEntries) {
    for (const file of Array.from(event.dataTransfer?.files ?? [])) out.push({ file, relPath: file.name })
  }
  if (out.length) await console_.uploadFiles(out, currentPath.value)
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
      <span class="font-mono text-sm font-semibold sm:text-base">StorHub</span>

      <PathBar :project="project" :path="currentPath" @navigate="navigate" />

      <div class="ml-auto flex shrink-0 items-center gap-2">
        <span v-if="busy" class="chip animate-pulse motion-reduce:animate-none" role="status">working…</span>
        <span v-if="isSharedView && sharedMode" class="chip text-sage">shared · read-only</span>
        <template v-if="authEnabled && console_.principal.value">
          <span class="chip">
            {{ console_.principal.value.username }}
            <template v-if="console_.principal.value.admin"> · admin</template>
          </span>
          <button class="btn btn-sm hidden sm:inline-flex" @click="console_.logout()">Sign out</button>
        </template>
      </div>
    </header>

    <div class="flex min-h-0 flex-1">
      <!-- First column: controls sidebar (resizable at lg+) -->
      <SideDrawer :open="drawerOpen" :width="panels.sidebar" @close="closeDrawer">
        <nav class="mb-5 flex items-center justify-between lg:hidden">
          <span class="font-mono text-base font-semibold">StorHub</span>
          <button type="button" class="btn btn-sm" aria-label="Close menu" @click="closeDrawer">✕</button>
        </nav>

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
            <button class="btn" :disabled="!project || busy" @click="console_.refreshAll()">
              Refresh
            </button>
          </div>
        </section>

        <!-- Auth -->
        <section v-if="authEnabled && !console_.token.value && !isSharedView" class="border-b border-hair py-4">
          <LoginCard />
        </section>
        <section v-else-if="authEnabled && console_.token.value && !isSharedView" class="space-y-3 border-b border-hair py-4 ">
          <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Account</h2>
          <dl class="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 text-sm">
            <dt class="text-xs leading-6 text-mist">user</dt>
            <dd class="font-mono text-xs leading-6">{{ console_.principal.value?.username }}</dd>
            <dt class="text-xs leading-6 text-mist">uid</dt>
            <dd class="font-mono text-xs leading-6">{{ console_.principal.value?.uid }}</dd>
            <dt class="text-xs leading-6 text-mist">gid</dt>
            <dd class="font-mono text-xs leading-6">{{ console_.principal.value?.primary_gid }}</dd>
            <dt v-if="console_.principal.value?.groups?.length" class="text-xs leading-6 text-mist">groups</dt>
            <dd
              v-if="console_.principal.value?.groups?.length"
              class="truncate font-mono text-xs leading-6"
              :title="(console_.principal.value.groups ?? []).join(', ')"
            >
              {{ (console_.principal.value.groups ?? []).join(',') }}
            </dd>
            <dt class="text-xs leading-6 text-mist">role</dt>
            <dd class="font-mono text-xs leading-6">
              {{ console_.principal.value?.admin ? 'admin' : 'user' }}
            </dd>
          </dl>
          <button class="btn btn-sm w-full" @click="console_.logout()">Sign out</button>
        </section>

        <!-- Stats: always visible; dashes until a project reports numbers -->
        <section class="space-y-2.5 border-b border-hair py-4">
          <StatsGrid :stats="console_.stats.value" />
          <button
            v-if="project && !isSharedView && isAdmin"
            class="btn btn-sm w-full"
            :disabled="busy || !project"
            title="Admin only: delete release assets no longer referenced by metadata"
            @click="purge"
          >
            Purge untracked assets…
          </button>
          <ConfirmDeleteProject v-if="project && !isSharedView" @deleted="projectInput = ''" />
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
                <button class="btn btn-sm px-2.5" :disabled="!currentPath || busy" title="Up one level" aria-label="Up one level" @click="console_.goUp()">
                  ↑
                </button>
                <button
                  class="btn btn-sm px-2.5"
                  :disabled="!project || busy"
                  title="Refresh everything"
                  aria-label="Refresh"
                  @click="console_.refreshAll()"
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
                Uploading {{ uploadProgress.done }}/{{ uploadProgress.total }} · {{ uploadProgress.current }}
                · {{ formatBytes(uploadProgress.bytesDone) }} / {{ formatBytes(uploadProgress.bytesTotal) }}
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
              :hint="project ? 'This directory is empty — drop files to upload.' : 'Load a project to start browsing.'"
            />
            <div v-else class="min-h-0 flex-1 overflow-y-auto pr-1">
              <EntryList :entries="entries" :selected-path="selectedPath" @select="selectEntry" />
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
