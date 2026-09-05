<script setup lang="ts">
import type { EntryInfo } from '~/utils/api-types'
import { copyText } from '~/utils/clipboard'
import { shareLink, SHARE_TTL_5M } from '~/utils/share-links'

const console_ = useConsole()
const toasts = useToasts()
const { ask } = useConfirm()

const props = defineProps<{
  entries: EntryInfo[]
  selectedPath: string
}>()

const emit = defineEmits<{
  select: [entry: EntryInfo]
  open: [entry: EntryInfo]
}>()

function glyph(entry: EntryInfo): string {
  if (entry.is_dir) return '▸'
  if (entry.is_symlink) return '↪'
  return '▪'
}

function glyphClass(entry: EntryInfo): string {
  if (entry.is_dir) return 'text-ember'
  if (entry.is_symlink) return 'text-sage'
  return 'text-mist/60'
}

// ---- Kebab menu ------------------------------------------------------------
// One open menu at a time; rendered in a body portal so pane scrolling can
// never clip it. Flips above the anchor when space below runs out.

interface MenuState {
  entry: EntryInfo
  style: Record<string, string>
}

const MENU_MAX_HEIGHT = 380

const openMenu = ref<MenuState | null>(null)
const kebabButtons = new Map<string, HTMLElement>()

function setKebabRef(path: string, el: Element | { $el?: unknown } | null) {
  if (el instanceof HTMLElement) kebabButtons.set(path, el)
}

function toggleMenu(entry: EntryInfo) {
  if (openMenu.value?.entry.path === entry.path) {
    closeMenu()
    return
  }
  const button = kebabButtons.get(entry.path)
  if (!button) return
  const rect = button.getBoundingClientRect()
  const vw = window.innerWidth
  const vh = window.innerHeight
  const margin = 8
  const estWidth = 256 // min-w-56 + padding, covers longest labels like "Copy direct link"
  let left = rect.right
  // Clamp horizontally so the menu (right-aligned to the button) never leaves the viewport
  if (left > vw - margin) left = vw - margin
  if (left - estWidth < margin) left = estWidth + margin
  const spaceBelow = vh - rect.bottom - margin
  const spaceAbove = rect.top - margin
  // Flip only if the menu wouldn't fit below and there's more room above
  const flipUp = spaceBelow < MENU_MAX_HEIGHT && spaceAbove > spaceBelow
  const available = flipUp ? spaceAbove : spaceBelow
  // Only limit height to what the viewport actually offers - the menu
  // will scroll internally (overflowY) only when its natural height
  // exceeds this available space.
  const maxHeight = Math.max(160, available - 4)
  const y = flipUp ? rect.top : rect.bottom + 4
  openMenu.value = {
    entry,
    style: {
      left: `${left}px`,
      ...(flipUp ? { bottom: `${vh - y + 4}px` } : { top: `${y}px` }),
      transform: 'translateX(-100%)',
      maxHeight: `${maxHeight}px`,
      overflowY: 'auto',
    },
  }
}

function closeMenu() {
  openMenu.value = null
}

function onGlobalPointer(event: MouseEvent) {
  const target = event.target as HTMLElement
  if (!target.closest('[data-entry-menu]')) closeMenu()
}

function onKey(event: KeyboardEvent) {
  if (event.key === 'Escape') closeMenu()
}

watch(openMenu, (open) => {
  if (import.meta.client) {
    if (open) {
      window.addEventListener('click', onGlobalPointer, true)
      window.addEventListener('keydown', onKey)
      window.addEventListener('resize', closeMenu)
    } else {
      window.removeEventListener('click', onGlobalPointer, true)
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('resize', closeMenu)
    }
  }
})

onMounted(() => {
  window.addEventListener('keydown', handleKeydown)
  // Focus list for desktop keyboard navigation
  if (!isMobile.value && listRef.value) {
    nextTick(() => listRef.value?.focus({ preventScroll: true }))
  }
})

onUnmounted(() => {
  window.removeEventListener('keydown', handleKeydown)
})

onUnmounted(closeMenu)

async function runFor(_entry: EntryInfo, fn: () => Promise<void> | void) {
  closeMenu()
  await fn()
}

/** Stat the row first so modals + detail panes operate on the same entry. */
async function withFocus(entry: EntryInfo, kind: Parameters<typeof console_.openModal>[0], contextDir?: string) {
  await console_.focusEntry(entry)
  console_.openModal(kind, contextDir)
}

async function shareEntry(entry: EntryInfo) {
  closeMenu()
  const share = await console_.createShare(entry.path, SHARE_TTL_5M)
  if (!share?.token) {
    toasts.error('Failed to create share link')
    return
  }
  await copyText(shareLink(share))
  toasts.success('Share link copied (valid 5 min)')
}

async function removeEntry(entry: EntryInfo) {
  const ok = await ask({
    title: 'Remove entry',
    body: `Permanently remove "${entry.path}"${entry.is_dir ? ' and everything inside it' : ''}?`,
    confirmLabel: 'Remove',
    danger: true,
  })
  if (ok) await console_.removeSelected(entry)
}

function isFile(entry: EntryInfo): boolean {
  return !entry.is_dir && !entry.is_symlink
}

// ---- Selection (desktop/mobile) -------------------------------------------
// Desktop = click selects, double-click opens, arrows work.
// Mobile = tap opens when nothing selected, otherwise toggles.
// Many desktop browsers now report maxTouchPoints>0 (touchpad) or
// pointer:coarse, so we require coarse+no-hover+small viewport together.
const isMobile = ref(false)

function updateIsMobile() {
  if (!import.meta.client) {
    isMobile.value = false
    return
  }
  const coarse = window.matchMedia('(pointer: coarse)').matches
  const noHover = window.matchMedia('(hover: none)').matches
  const touch = navigator.maxTouchPoints > 0
  const small = window.matchMedia('(max-width: 1023px)').matches
  // Only treat as mobile on small viewports with touch/coarse input.
  isMobile.value = small && (coarse || noHover || touch)
}

if (import.meta.client) {
  updateIsMobile()
  window.addEventListener('resize', updateIsMobile)
  // Also listen to media query changes for devices that toggle coarse/hover
  try {
    window.matchMedia('(pointer: coarse)').addEventListener('change', updateIsMobile)
    window.matchMedia('(hover: none)').addEventListener('change', updateIsMobile)
    window.matchMedia('(max-width: 1023px)').addEventListener('change', updateIsMobile)
  } catch (_e) {
    void _e
  }
}

onUnmounted(() => {
  if (import.meta.client) {
    window.removeEventListener('resize', updateIsMobile)
    try {
      window.matchMedia('(pointer: coarse)').removeEventListener('change', updateIsMobile)
      window.matchMedia('(hover: none)').removeEventListener('change', updateIsMobile)
      window.matchMedia('(max-width: 1023px)').removeEventListener('change', updateIsMobile)
    } catch (_e) {
      void _e
    }
  }
})

const selectedSet = computed(() => console_.selectedPaths.value)

const listRef = ref<HTMLElement | null>(null)

// Keep keyboard focus on the list after selection changes so arrows keep working
watch(() => props.entries.length, () => {
  if (!isMobile.value && listRef.value && document.activeElement?.closest('[data-entry-list]')) {
    listRef.value.focus({ preventScroll: true })
  }
})

function isSelected(entry: EntryInfo): boolean {
  return selectedSet.value.has(entry.path)
}

function handleRowClick(entry: EntryInfo, event: MouseEvent) {
  if (isMobile.value) {
    // Mobile: click is open when nothing selected, else toggle select
    if (selectedSet.value.size === 0) emit('open', entry)
    else handleSelect(entry, event)
    return
  }
  handleSelect(entry, event)
}

function handleRowDblClick(entry: EntryInfo) {
  if (!isMobile.value) emit('open', entry)
}

let shiftAnchor: string | null = null

function handleSelect(entry: EntryInfo, event: MouseEvent) {
  const e = event as MouseEvent & { metaKey: boolean; ctrlKey: boolean; shiftKey: boolean }
  if (e.shiftKey) {
    // Keep anchor at the point where Shift was first held; extend from there
    if (!shiftAnchor) shiftAnchor = console_.lastSelected.value ?? console_.selectedPath.value ?? entry.path
    const anchor = shiftAnchor ?? entry.path
    console_.selectRange(anchor, entry.path)
  } else {
    shiftAnchor = null
    if (e.metaKey || e.ctrlKey) {
      console_.toggleSelect(entry.path)
    } else {
      console_.selectSingle(entry.path)
    }
  }
}

function handleKeydown(event: KeyboardEvent) {
  if (event.defaultPrevented) return
  if (!props.entries.length) return
  const active = document.activeElement as HTMLElement | null
  if (active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA' || active.isContentEditable)) return
  // Only handle when focus is inside the entry list or no input is focused
  const inList = !!active?.closest('[data-entry-list]') || active === listRef.value || !active || active === document.body
  if (!inList && active && active !== document.body) {
    // If focus is outside the list (e.g., sidebar), don't hijack arrows
    const listEl = listRef.value
    if (listEl && !listEl.contains(active)) return
  }
  const idx = props.entries.findIndex((e) => e.path === console_.lastSelected.value)
  if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
    event.preventDefault()
    const dir = event.key === 'ArrowDown' ? 1 : -1
    // When nothing is selected, ArrowDown selects first, ArrowUp selects last
    let nextIdx: number
    if (idx === -1) {
      nextIdx = dir === 1 ? 0 : props.entries.length - 1
    } else {
      nextIdx = Math.max(0, Math.min(props.entries.length - 1, idx + dir))
    }
    const next = props.entries[nextIdx]!
    if (event.shiftKey) {
      if (!shiftAnchor) shiftAnchor = console_.lastSelected.value ?? console_.selectedPath.value ?? props.entries[idx]?.path ?? next.path
      const anchor = shiftAnchor ?? next.path
      console_.selectRange(anchor, next.path)
    } else {
      shiftAnchor = null
      console_.selectSingle(next.path)
    }
    // Keep the newly selected row visible and keep keyboard focus on the list
    nextTick(() => {
      const row = document.querySelector<HTMLElement>(`[data-path="${CSS.escape(next.path)}"]`)
      row?.scrollIntoView({ block: 'nearest' })
    })
  } else if (event.key === 'Enter') {
    event.preventDefault()
    shiftAnchor = null
    const targetPath = console_.lastSelected.value || console_.selectedPath.value
    const target = props.entries.find((e) => e.path === targetPath) ?? (idx >= 0 ? props.entries[idx] : null)
    if (target) emit('open', target)
  } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'a') {
    event.preventDefault()
    shiftAnchor = null
    console_.selectAll()
  } else if (event.key === 'Escape') {
    shiftAnchor = null
    console_.clearSelection()
  } else if (event.key === 'Home' || (event.key === 'ArrowUp' && (event.ctrlKey || event.metaKey))) {
    event.preventDefault()
    shiftAnchor = null
    const first = props.entries[0]!
    if (event.shiftKey) {
      if (!shiftAnchor) shiftAnchor = console_.lastSelected.value ?? props.entries[idx]?.path ?? first.path
      console_.selectRange(shiftAnchor, first.path)
    } else {
      console_.selectSingle(first.path)
    }
  } else if (event.key === 'End' || (event.key === 'ArrowDown' && (event.ctrlKey || event.metaKey))) {
    event.preventDefault()
    shiftAnchor = null
    const last = props.entries[props.entries.length - 1]!
    if (event.shiftKey) {
      if (!shiftAnchor) shiftAnchor = console_.lastSelected.value ?? props.entries[idx]?.path ?? last.path
      console_.selectRange(shiftAnchor, last.path)
    } else {
      console_.selectSingle(last.path)
    }
  }
}

// Long-press for mobile select
let pressTimer: ReturnType<typeof setTimeout> | null = null

function onTouchStart(entry: EntryInfo) {
  if (!isMobile.value) return
  pressTimer = setTimeout(() => {
    console_.toggleSelect(entry.path)
    if (navigator.vibrate) navigator.vibrate(20)
    pressTimer = null
  }, 500)
}

function onTouchEnd() {
  if (pressTimer) {
    clearTimeout(pressTimer)
    pressTimer = null
  }
}

function onTouchMove() {
  onTouchEnd()
}

// Bulk helpers
const menuTargets = computed(() => {
  const cur = openMenu.value?.entry
  if (!cur) return [] as EntryInfo[]
  if (selectedSet.value.has(cur.path) && selectedSet.value.size > 1) {
    return props.entries.filter((e) => selectedSet.value.has(e.path))
  }
  return [cur]
})

const canBulkDownload = computed(() => menuTargets.value.length > 0 && menuTargets.value.every(isFile))
const canBulkRemove = computed(() => menuTargets.value.length > 0)
const canBulkMove = computed(() => menuTargets.value.length > 0)
const canBulkCopy = computed(() => menuTargets.value.length > 0)

async function downloadBulk() {
  closeMenu()
  for (const e of menuTargets.value) await console_.downloadEntry(e)
}

async function removeBulk() {
  const targets = menuTargets.value
  closeMenu()
  const ok = await ask({
    title: targets.length === 1 ? 'Remove entry' : `Remove ${targets.length} items`,
    body: targets.length === 1
      ? `Permanently remove "${targets[0]!.path}"${targets[0]!.is_dir ? ' and everything inside it' : ''}?`
      : `Permanently remove ${targets.length} items?`,
    confirmLabel: targets.length === 1 ? 'Remove' : `Remove (${targets.length})`,
    danger: true,
  })
  if (!ok) return
  await console_.removeMany(targets.map((e) => e.path))
}

function moveBulk() {
  closeMenu()
  // keep current multi-selection as is
  console_.openModal('move')
}

function copyBulk() {
  closeMenu()
  console_.openModal('copy')
}

async function openMove(entry: EntryInfo) {
  await console_.focusEntry(entry)
  // Preserve bulk selection if this entry is part of it
  if (!console_.selectedPaths.value.has(entry.path)) {
    console_.selectSingle(entry.path)
  } else if (console_.selectedPaths.value.size === 0) {
    console_.selectSingle(entry.path)
  }
  console_.openModal('move')
}

async function openCopy(entry: EntryInfo) {
  await console_.focusEntry(entry)
  if (!console_.selectedPaths.value.has(entry.path)) {
    console_.selectSingle(entry.path)
  } else if (console_.selectedPaths.value.size === 0) {
    console_.selectSingle(entry.path)
  }
  console_.openModal('copy')
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
              {{ entry.is_dir ? 'directory' : entry.is_symlink ? `symlink → ${entry.symlink_target ?? '?'}` : 'file' }}
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
            <template v-if="console_.canWrite.value">
              <div class="menu-sep" />
              <button v-if="canBulkMove" role="menuitem" class="menu-item" @click="moveBulk">Move ({{ menuTargets.length }})</button>
              <button v-if="canBulkCopy" role="menuitem" class="menu-item" @click="copyBulk">Copy ({{ menuTargets.length }})</button>
            </template>
            <template v-if="console_.canWrite.value && canBulkRemove">
              <div class="menu-sep" />
              <button role="menuitem" class="menu-item text-clay-soft hover:bg-clay/20" @click="removeBulk">Remove ({{ menuTargets.length }})</button>
            </template>
          </template>

          <template v-else>
            <button role="menuitem" class="menu-item" @click="runFor(entry, () => emit('open', entry))">
              {{ entry.is_dir ? 'Browse' : 'Preview' }}
            </button>

            <template v-if="console_.canWrite.value">
              <div v-if="entry.is_dir" class="menu-sep" />
              <button v-if="entry.is_dir" role="menuitem" class="menu-item" @click="runFor(entry, () => console_.openModal('create-file', entry.path))">New file here</button>
              <button v-if="entry.is_dir" role="menuitem" class="menu-item" @click="runFor(entry, () => console_.openModal('mkdir', entry.path))">New folder here</button>

              <div class="menu-sep" />
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'rename'))">Rename</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => openMove(entry))">Move</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => openCopy(entry))">Copy</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'link'))">Hard link</button>
              <button role="menuitem" class="menu-item" title="Create a symlink pointing to this entry, inside the current directory" @click="runFor(entry, () => withFocus(entry, 'symlink', console_.currentPath.value))">Symlink</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'chmod'))">Chmod</button>
              <button v-if="console_.isAdmin.value" role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'chown'))">Chown</button>
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'utimes'))">Timestamps</button>

              <template v-if="isFile(entry)">
                <div class="menu-sep" />
                <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'append'))">Append</button>
                <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'patch'))">Patch</button>
                <button role="menuitem" class="menu-item" @click="runFor(entry, () => withFocus(entry, 'truncate'))">Truncate</button>
              </template>

              <div class="menu-sep" />
              <button role="menuitem" class="menu-item" @click="runFor(entry, () => shareEntry(entry))">Share</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => console_.downloadEntry(entry))">Download</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" title="Signed URL, valid 5 minutes - works with curl/wget too" @click="runFor(entry, () => console_.copyDirectLink(entry))">Copy direct link</button>
            </template>
            <template v-else>
              <div class="menu-sep" />
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => console_.downloadEntry(entry))">Download</button>
              <button v-if="isFile(entry)" role="menuitem" class="menu-item" @click="runFor(entry, () => console_.copyDirectLink(entry))">Copy direct link</button>
            </template>

            <template v-if="console_.canWrite.value">
              <div class="menu-sep" />
              <button role="menuitem" class="menu-item text-clay-soft hover:bg-clay/20" @click="runFor(entry, () => removeEntry(entry))">Remove</button>
            </template>
          </template>
        </div>
      </Teleport>
    </li>
  </ul>
</template>
