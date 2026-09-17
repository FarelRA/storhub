<script setup lang="ts">
import { copyText } from '~/utils/clipboard'
import { directLink, shareLink, SHARE_TTL_7D, SHARE_TTL_7D_LABEL } from '~/utils/share-links'
import type { AnyEntry } from '~/utils/api-types'

const consoleStore = useConsole()
const { shares } = consoleStore
const toasts = useToasts()
const { ask } = useConfirm()

// One selected entry, whether it arrives via the multi-select set or the
// single focus path: null while a multi-selection is active.
const singleSelection = computed<{ path: string; entry: AnyEntry | null } | null>(() => {
  if (consoleStore.selectedPaths.value.size === 1) {
    const [only] = [...consoleStore.selectedPaths.value]
    if (only === undefined) return null
    const entry = consoleStore.entries.value.find(e => e.path === only) ?? consoleStore.selectedEntry.value
    return { path: only, entry }
  }
  if (consoleStore.selectedPaths.value.size === 0 && consoleStore.selectedPath.value) {
    return { path: consoleStore.selectedPath.value, entry: consoleStore.selectedEntry.value }
  }
  return null
})

async function copy(label: string, value: string) {
  await copyText(value)
  toasts.success(`${label} copied`)
}

// Both buttons mint 7-day shares (see share-links.ts for WHY the panel TTL
// differs from the kebab's); one copies the ?share link, the other the
// direct download link.
async function createAndCopy(kind: 'share' | 'direct') {
  const selection = singleSelection.value
  if (!selection) return
  const share = await consoleStore.createShare(selection.path, SHARE_TTL_7D)
  if (!share) return
  if (kind === 'share') {
    await copy('Share link', shareLink(share))
    return
  }
  if (share.download_url) await copy('Direct link', directLink(share))
  else await copy('Share link', shareLink(share))
}

async function remove(share: { id: string; path: string }) {
  const ok = await ask({
    title: 'Delete share',
    body: `Delete the share for "${share.path || '/'}"? Existing links will stop working.`,
    confirmLabel: 'Delete',
    danger: true,
  })
  if (ok) await consoleStore.deleteShare(share as Parameters<typeof consoleStore.deleteShare>[0])
}
</script>

<template>
  <section v-if="!consoleStore.isSharedView.value" class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Shares</h2>

    <div class="flex flex-wrap gap-2">
      <button
        class="btn btn-sm"
        :disabled="!singleSelection || !consoleStore.project.value"
        :title="`Share the selected file or folder (${SHARE_TTL_7D_LABEL}, single only)`"
        @click="createAndCopy('share')"
      >
        Share selected ({{ SHARE_TTL_7D_LABEL }})…
      </button>
      <button
        class="btn btn-sm"
        :disabled="!singleSelection || !consoleStore.project.value || !!singleSelection?.entry?.is_dir"
        :title="`Direct download share for the selected file (${SHARE_TTL_7D_LABEL}, single file only)`"
        @click="createAndCopy('direct')"
      >
        Direct download ({{ SHARE_TTL_7D_LABEL }})…
      </button>
    </div>

    <p v-if="!shares.length" class="text-sm text-mist">No active shares.</p>

    <ul v-else class="flex max-h-64 flex-col gap-1.5 overflow-y-auto pr-1">
      <li v-for="share in shares" :key="share.id" class="card space-y-1.5 px-3 py-2">
        <p class="truncate font-mono text-xs font-medium">{{ share.path || '/' }}</p>
        <div class="flex flex-wrap gap-1">
          <span class="chip">{{ share.is_dir ? 'folder' : 'file' }}</span>
          <span class="chip" :title="share.expires_at">expires {{ relativeTime(share.expires_at) }}</span>
        </div>
        <div class="flex flex-wrap gap-1.5">
          <!-- No "copy share link" button here: GET /shares never carries the
               signed token (only create/derive responses do), so a link could
               not be rebuilt from list rows. -->
          <button v-if="share.download_url" class="btn btn-sm" @click="copy('Direct link', directLink(share))">
            Direct
          </button>
          <button class="btn btn-danger btn-sm ml-auto" @click="remove(share)">Delete</button>
        </div>
      </li>
    </ul>
  </section>
</template>
