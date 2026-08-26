<script setup lang="ts">
const console_ = useConsole()
const {
  project,
  currentPath,
  selectedPath,
  selectedEntry,
  entries,
  busy,
  authEnabled,
  isAdmin,
  isSharedView,
  sharedMode,
  canWrite,
} = console_

const drawerOpen = ref(false)
const projectInput = ref('')
const { ask } = useConfirm()

onMounted(async () => {
  console_.restoreSession()
  const shareParam = new URLSearchParams(window.location.search).get('share')
  if (shareParam) {
    await console_.bootstrapShare(shareParam)
    return
  }
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

async function purge() {
  const ok = await ask({
    title: 'Purge untracked chunks',
    body: 'Delete release assets that are no longer referenced by any metadata revision? This frees storage but old revisions may lose their blobs.',
    confirmLabel: 'Purge',
    danger: true,
  })
  if (ok) await console_.purgeUntracked()
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
      <span class="hidden font-mono text-base font-semibold sm:block">StorHub</span>

      <AppBreadcrumbs :project="project" :path="currentPath" @navigate="navigate" />

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
      <!-- Sidebar: drawer below lg, pinned at lg+ -->
      <SideDrawer :open="drawerOpen" @close="closeDrawer">
        <nav class="mb-5 flex items-center justify-between lg:hidden">
          <span class="font-mono text-base font-semibold">StorHub</span>
          <button type="button" class="btn btn-sm" aria-label="Close menu" @click="closeDrawer">✕</button>
        </nav>

        <!-- Project -->
        <section class="space-y-3 border-b border-hair pb-5">
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
            <button class="btn" :disabled="!project || busy" @click="console_.refreshAll()">Refresh</button>
          </div>
        </section>

        <!-- Auth -->
        <section v-if="authEnabled && !console_.token.value && !isSharedView" class="border-b border-hair py-5">
          <LoginCard />
        </section>
        <section v-else-if="authEnabled && console_.token.value && !isSharedView" class="space-y-2 border-b border-hair py-5 max-lg:hidden">
          <p class="text-sm text-sage">✓ Signed in as {{ console_.principal.value?.username }}</p>
          <button class="btn btn-sm w-full" @click="console_.logout()">Sign out</button>
        </section>

        <!-- Stats -->
        <section v-if="project && !isSharedView" class="space-y-2.5 border-b border-hair py-5">
          <StatsGrid :stats="console_.stats.value" />
          <button
            v-if="isAdmin"
            class="btn btn-sm w-full"
            :disabled="busy || !project"
            title="Admin only: delete release assets no longer referenced by metadata"
            @click="purge"
          >
            Purge untracked assets…
          </button>
          <ConfirmDeleteProject @deleted="projectInput = ''" />
        </section>

        <!-- Directory actions -->
        <section v-if="project" class="space-y-2 border-b border-hair py-5">
          <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Directory actions</h2>
          <div class="grid gap-2">
            <button class="btn btn-sm" :disabled="!canWrite || busy" @click="console_.openModal('mkdir')">
              New directory…
            </button>
            <button class="btn btn-sm" :disabled="!currentPath || (!canWrite && !!selectedEntry)" @click="console_.goUp()">
              Up one level
            </button>
            <button class="btn btn-sm" :disabled="!canWrite || busy" @click="console_.openModal('create-file')">
              New file…
            </button>
          </div>
        </section>

        <section class="py-5">
          <SharePanel />
        </section>

        <p v-if="!isSharedView" class="pb-4 text-xs leading-relaxed text-mist/70">
          Everything here goes through the same REST API the CLI uses — nothing is special-cased.
        </p>
        <ServerInfoCard />
      </SideDrawer>

      <!-- Main workspace -->
      <main class="min-w-0 flex-1 lg:h-full lg:overflow-hidden">
        <div
          class="grid grid-cols-1 md:grid-cols-2 lg:h-full lg:grid-cols-[minmax(260px,340px)_minmax(360px,1fr)_minmax(300px,380px)]"
        >
          <!-- Directory pane -->
          <section class="flex min-h-0 flex-col gap-3 border-b border-hair p-4 max-md:border-r-0 md:border-b lg:border-b-0 lg:border-r">
            <header class="flex items-baseline justify-between gap-3">
              <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Directory</h2>
              <span class="text-xs text-mist">{{ entries.length }} entries</span>
            </header>
            <EmptyState
              v-if="!entries.length"
              icon="🗂"
              title="Nothing here"
              :hint="project ? 'This directory is empty.' : 'Load a project from the menu to start browsing.'"
            />
            <div v-else class="min-h-0 overflow-y-auto pr-1 lg:min-h-0">
              <EntryList :entries="entries" :selected-path="selectedPath" @select="selectEntry" />
            </div>
          </section>

          <!-- Editor pane -->
          <section class="min-h-0 border-b border-hair p-4 lg:border-b-0 lg:border-r">
            <EditorPane />
          </section>

          <!-- Details column -->
          <aside class="flex min-h-0 flex-col gap-5 overflow-y-auto p-4 md:col-span-2 lg:col-span-1 lg:overflow-y-auto">
            <EntryDetails />
            <XattrPanel />
            <RevisionPanel v-if="!isSharedView" />
          </aside>
        </div>
      </main>
    </div>

    <!-- Modals -->
    <ActionModals />
    <ConfirmDialog />
    <ToastStack />
  </div>
</template>
