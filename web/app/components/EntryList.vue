<script setup lang="ts">
import type { DirEntry } from '~/utils/api-types'
import { copyText } from '~/utils/clipboard'
import { shareLink, SHARE_TTL_5M, SHARE_TTL_5M_LABEL } from '~/utils/share-links'

const consoleStore = useConsole()
const toasts = useToasts()
const { ask } = useConfirm()

const props = defineProps<{
  entries: DirEntry[]
  selectedPath: string
}>()

const emit = defineEmits<{
  select: [entry: DirEntry]
  open: [entry: DirEntry]
}>()

const menu = useEntryMenu()
const { openMenu, setKebabRef, toggleMenu, closeMenu } = menu
const selection = useEntrySelection({
  getEntries: () => props.entries,
  openEntry: entry => emit('open', entry),
})
const {
  listRef,
  selectedSet,
  isSelected,
  handleRowClick,
  handleRowDblClick,
  handleKeydown,
  onTouchStart,
  onTouchEnd,
  onTouchMove,
} = selection

function glyph(entry: DirEntry): string {
  if (entry.is_dir) return '▸'
  if (entry.is_symlink) return '↪'
  return '▪'
}

function glyphClass(entry: DirEntry): string {
  if (entry.is_dir) return 'text-ember'
  if (entry.is_symlink) return 'text-sage'
  return 'text-mist/60'
}

async function runFor(_entry: DirEntry, fn: () => Promise<void> | void) {
  closeMenu()
  await fn()
}

/** Stat the row first so modals + detail panes operate on the same entry. */
async function withFocus(entry: DirEntry, kind: Parameters<typeof consoleStore.openModal>[0], contextDir?: string) {
  await consoleStore.focusEntry(entry)
  consoleStore.openModal(kind, contextDir)
}

async function shareEntry(entry: DirEntry) {
  closeMenu()
  const share = await consoleStore.createShare(entry.path, SHARE_TTL_5M)
  if (!share?.token) {
    toasts.error('Failed to create share link')
    return
  }
  await copyText(shareLink(share))
  toasts.success(`Share link copied (valid ${SHARE_TTL_5M_LABEL})`)
}

async function removeEntry(entry: DirEntry) {
  const ok = await ask({
    title: 'Remove entry',
    body: `Permanently remove "${entry.path}"${entry.is_dir ? ' and everything inside it' : ''}?`,
    confirmLabel: 'Remove',
    danger: true,
  })
  if (ok) await consoleStore.removeSelected(entry)
}

function isFile(entry: DirEntry): boolean {
  return !entry.is_dir && !entry.is_symlink
}

// Bulk helpers
const menuTargets = computed(() => {
  const cur = openMenu.value?.entry
  if (!cur) return [] as DirEntry[]
  if (selectedSet.value.has(cur.path) && selectedSet.value.size > 1) {
    return props.entries.filter(e => selectedSet.value.has(e.path))
  }
  return [cur]
})

const canBulkDownload = computed(() => menuTargets.value.length > 0 && menuTargets.value.every(isFile))
const canBulkRemove = computed(() => menuTargets.value.length > 0)
const canBulkMove = computed(() => menuTargets.value.length > 0)
const canBulkCopy = computed(() => menuTargets.value.length > 0)

async function downloadBulk() {
  closeMenu()
  for (const e of menuTargets.value) await consoleStore.downloadEntry(e)
}

async function removeBulk() {
  const targets = menuTargets.value
  closeMenu()
  const only = targets[0]
  const ok = await ask({
    title: targets.length === 1 ? 'Remove entry' : `Remove ${targets.length} items`,
    body: targets.length === 1 && only
      ? `Permanently remove "${only.path}"${only.is_dir ? ' and everything inside it' : ''}?`
      : `Permanently remove ${targets.length} items?`,
    confirmLabel: targets.length === 1 ? 'Remove' : `Remove (${targets.length})`,
    danger: true,
  })
  if (!ok) return
  await consoleStore.removeMany(targets.map(e => e.path))
}

function moveBulk() {
  closeMenu()
  // keep current multi-selection as is
  consoleStore.openModal('move')
}

function copyBulk() {
  closeMenu()
  consoleStore.openModal('copy')
}

async function openMove(entry: DirEntry) {
  await consoleStore.focusEntry(entry)
  // Preserve bulk selection if this entry is part of it
  if (!consoleStore.selectedPaths.value.has(entry.path)) {
    consoleStore.selectSingle(entry.path)
  } else if (consoleStore.selectedPaths.value.size === 0) {
    consoleStore.selectSingle(entry.path)
  }
  consoleStore.openModal('move')
}

async function openCopy(entry: DirEntry) {
  await consoleStore.focusEntry(entry)
  if (!consoleStore.selectedPaths.value.has(entry.path)) {
    consoleStore.selectSingle(entry.path)
  } else if (consoleStore.selectedPaths.value.size === 0) {
    consoleStore.selectSingle(entry.path)
  }
  consoleStore.openModal('copy')
}
</script>

<template>
  <ul
    ref="listRef"
    data-entry-list
    class="flex flex-col gap-0.5"
    tabindex="0"
    role="listbox"
    aria-multiselectable="true"
    @keydown="handleKeydown"
  >
    <li v-for="entry in entries" :key="entry.path" :data-path="entry.path" class="group relative">
      <div class="flex items-center gap-1">
        <button
          type="button"
          role="option"
          :aria-selected="isSelected(entry)"
          class="flex min-w-0 flex-1 items-center gap-3 rounded-lg border px-3 py-2.5 text-left transition-colors motion-reduce:transition-none"
          :class="
            isSelected(entry)
              ? 'border-hair bg-surface shadow-[inset_2px_0_0_0_var(--color-ember)]'
              : 'border-transparent hover:border-hair hover:bg-surface'
          "
          @click="handleRowClick(entry, $event)"
          @dblclick="handleRowDblClick(entry)"
          @touchstart.passive="onTouchStart(entry)"
          @touchend="onTouchEnd"
          @touchmove="onTouchMove"
        >
          <span aria-hidden="true" class="w-4 shrink-0 text-center" :class="glyphClass(entry)">
            {{ glyph(entry) }}
          </span>
          <span class="min-w-0 flex-1">
            <div class="font-medium">
              <MidTruncate :text="entry.path.split('/').pop() ?? entry.path" />
            </div>
            <span class="block truncate text-xs text-mist">
              {{ entry.is_dir ? 'directory' : entry.is_symlink ? 'symlink' : 'file' }}
            </span>
          </span>
          <span v-if="isFile(entry)" class="shrink-0 font-mono text-xs tabular-nums text-mist">
            {{ formatBytes(entry.size) }}
          </span>
        </button>

        <button
          :ref="(el) => setKebabRef(entry.path, el)"
          type="button"
          class="btn btn-sm shrink-0 border-transparent px-2 opacity-60 group-hover:opacity-100 hover:!border-hair max-lg:opacity-100"
          :aria-label="`Actions for ${entry.path}`"
          :aria-expanded="openMenu?.entry.path === entry.path"
          @click.stop="toggleMenu(entry)"
        >
          ⋮
        </button>
      </div>

      <!-- Row action menu (body portal) -->
      <Teleport to="body">
        <div
          v-if="openMenu?.entry.path === entry.path"
          data-entry-menu
          role="menu"
          :aria-label="`Actions for ${entry.path}`"
          class="card fixed z-50 min-w-56 max-w-[calc(100vw-16px)] py-1 shadow-2xl overscroll-contain"
          :style="openMenu.style"
        >
          <div v-if="menuTargets.length > 1" class="px-3 py-1.5 text-xs font-medium text-mist">
            {{ menuTargets.length }} selected
          </div>
          <div v-if="menuTargets.length > 1" class="menu-sep" />

          <template v-if="menuTargets.length > 1">
            <button v-if="canBulkDownload" role="menuitem" class="menu-item" @click="downloadBulk">Download ({{ menuTargets.length }})</button>
            <template v-if="consoleStore.canWrite.value">
              <div class="menu-sep" />
              <button v-if="canBulkMove" role="menuitem" class="menu-item" @click="moveBulk">Move ({{ menuTargets.length }})</button>
              <button v-if="canBulkCopy" role="menuitem" class="menu-item" @click="copyBulk">Copy ({{ menuTargets.length }})</button>
            </template>
            <template v-if="consoleStore.canWrite.value && canBulkRemove">
              <div class="menu-sep" />
              <button role="menuitem" class="menu-item text-clay-soft hover:bg-clay/20" @click="removeBulk">Remove ({{ menuTargets.length }})</button>
            </template>
          </template>

          <template v-else>
            <button role="menuitem" class="menu-item" @click="runFor(entry, () => emit('open', entry))">
              {{ entry.is_dir ? 'Browse' : 'Preview' }}
            </button>

            <template v-if="consoleStore.canWrite.value">
              <div v-if="entry.is_dir" class="menu-sep" />
              <button v-if="entry.is_dir" role="menuitem" class="menu-item" @click="runFor(entry, () => consoleStore.openModal('create-file', entry.path))">New file here</button>
              <button v-if="entry.is_dir" role="menuitem" class="menu-item" @click="runFor(entry, () => consoleStore.openModal('mkdir', entry.path))">New folder here</button>

              <div class="menu-sep" />
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'rename'))">Rename</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => openMove(entry))">Move</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => openCopy(entry))">Copy</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'link'))">Hard link</button>
              <button role="menuitem" class="menu-item" title="Create a symlink pointing to this entry, inside the current directory" @click="runFor(entry, () => withFocus(entry, 'symlink', consoleStore.currentPath.value))">Symlink</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'chmod'))">Chmod</button>
              <button v-if="consoleStore.isAdmin.value" role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'chown'))">Chown</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'utimes'))">Timestamps</button>
              <button role="menuitem" class="menu-item" title="Inspect and edit extended attributes in the preview pane" @click="runFor(entry, () => withFocus(entry, 'xattr-set'))">Set xattr</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'xattr-remove'))">Remove xattr</button>

              <template v-if="isFile(entry)">
                <div class="menu-sep" />
                <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'append'))">Append</button>
                <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'patch'))">Patch</button>
                <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'truncate'))">Truncate</button>
              </template>

              <div class="menu-sep" />
              <button role="menuitem" class="menu-item" title="Short-lived share, valid 5 minutes" @click="runFor(entry, () => shareEntry(entry))">Share (5 min)</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => consoleStore.downloadEntry(entry))">Download</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" title="Signed URL, valid 5 minutes. Works with curl/wget too." @click="runFor(entry, () => consoleStore.copyDirectLink(entry))">Copy direct link</button>
            </template>
            <template v-else>
              <div class="menu-sep" />
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => consoleStore.downloadEntry(entry))">Download</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => consoleStore.copyDirectLink(entry))">Copy direct link</button>
            </template>

            <template v-if="consoleStore.canWrite.value">
              <div class="menu-sep" />
              <button role="menuitem" class="menu-item text-clay-soft hover:bg-clay/20" @click="runFor(entry, () => removeEntry(entry))">Remove</button>
            </template>
          </template>
        </div>
      </Teleport>
    </li>
  </ul>
</template>
