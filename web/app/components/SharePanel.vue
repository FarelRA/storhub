<script setup lang="ts">
import { copyText } from '~/utils/clipboard'

const console_ = useConsole()
const { shares } = console_
const toasts = useToasts()
const { ask } = useConfirm()

const singlePath = computed(() => {
  if (console_.selectedPaths.value.size === 1) return [...console_.selectedPaths.value][0]!
  if (console_.selectedPaths.value.size === 0 && console_.selectedPath.value) return console_.selectedPath.value
  return null
})
const singleEntry = computed(() => {
  if (console_.selectedPaths.value.size === 1) {
    const p = [...console_.selectedPaths.value][0]!
    return console_.entries.value.find((e) => e.path === p) ?? console_.selectedEntry.value
  }
  return console_.selectedEntry.value
})

async function copy(label: string, value: string) {
  await copyText(value)
  toasts.success(`${label} copied`)
}

function shareLink(share: { id: string; token?: string }): string {
  return `${window.location.origin}${window.location.pathname}?share=${encodeURIComponent(share.token ?? share.id)}`
}

async function createShare(download: boolean) {
  const p = singlePath.value
  if (!p) return
  // 7 days = 604800 seconds
  const share = await console_.createShare(p, download, 604800)
  if (share) await copy('Share link', shareLink(share))
}

async function remove(share: { id: string; path: string }) {
  const ok = await ask({
    title: 'Delete share',
    body: `Delete the share for "${share.path || '/'}"? Existing links will stop working.`,
    confirmLabel: 'Delete',
    danger: true,
  })
  if (ok) await console_.deleteShare(share as Parameters<typeof console_.deleteShare>[0])
}
</script>

<template>
  <section v-if="console_.isSharedView.value" class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Share</h2>
    <p class="text-sm text-mist">
      Viewing <span class="font-mono text-xs">{{ console_.shareRootPath.value || '/' }}</span> — read-only.
    </p>
    <p class="text-xs text-mist">Links expire with the share.</p>
  </section>
  <section v-else class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Shares</h2>

    <div class="flex flex-wrap gap-2">
      <button
        class="btn btn-sm"
        :disabled="!singlePath || !console_.project.value"
        title="Share the selected file or folder in browser (7 days, single only)"
        @click="createShare(false)"
      >
        Share selected…
      </button>
      <button
        class="btn btn-sm"
        :disabled="!singlePath || !console_.project.value || !!singleEntry?.is_dir"
        title="Direct download share for the selected file (7 days, single file only)"
        @click="createShare(true)"
      >
        Direct download…
      </button>
    </div>

    <p v-if="!shares.length" class="text-sm text-mist">No active shares.</p>

    <ul v-else class="flex max-h-64 flex-col gap-1.5 overflow-y-auto pr-1">
      <li v-for="share in shares" :key="share.id" class="card space-y-1.5 px-3 py-2">
        <p class="truncate font-mono text-xs font-medium">{{ share.path || '/' }}</p>
        <div class="flex flex-wrap gap-1">
          <span class="chip">{{ share.is_dir ? 'folder' : 'file' }}</span>
          <span class="chip">{{ share.download ? 'download' : 'browser' }}</span>
          <span class="chip" :title="share.expires_at">expires {{ relativeTime(share.expires_at) }}</span>
        </div>
        <div class="flex flex-wrap gap-1.5">
          <button v-if="share.token" class="btn btn-sm" @click="copy('Share link', shareLink(share))">Link</button>
          <button
            v-if="share.download && share.download_url"
            class="btn btn-sm"
            @click="copy('Direct link', new URL(share.download_url, window.location.origin).toString())"
          >
            Direct
          </button>
          <button class="btn btn-danger btn-sm ml-auto" @click="remove(share)">Delete</button>
        </div>
      </li>
    </ul>
  </section>
</template>
