<script setup lang="ts">
import { copyText } from '~/utils/clipboard'

const console_ = useConsole()
const { shares } = console_
const toasts = useToasts()
const { ask } = useConfirm()

async function copy(label: string, value: string) {
  await copyText(value)
  toasts.success(`${label} copied`)
}

function shareLink(share: { id: string; token?: string }): string {
  return `${window.location.origin}${window.location.pathname}?share=${encodeURIComponent(share.token ?? share.id)}`
}

async function createShare(download: boolean) {
  const paths = console_.selectedPaths.value.size > 0 ? [...console_.selectedPaths.value] : [console_.selectedPath.value].filter(Boolean) as string[]
  if (!paths.length) return
  for (const p of paths) {
    const share = await console_.createShare(p, download)
    if (share) await copy('Share link', shareLink(share))
  }
  if (paths.length > 1) toasts.success(`Created ${paths.length} shares`)
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
  <section v-if="!console_.isSharedView.value" class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Shares</h2>

    <div class="flex flex-wrap gap-2">
      <button
        class="btn btn-sm"
        :disabled="!console_.selectedPath.value || !console_.project.value"
        title="Create a browser-only share for the selected entry (works for files and folders)"
        @click="createShare(false)"
      >
        Share selected…
      </button>
      <button
        class="btn btn-sm"
        :disabled="!console_.selectedPath.value || !console_.project.value"
        title="Create a share that allows direct download (works for files and folders)"
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
          <span class="chip">{{ share.download ? 'download' : 'browser-only' }}</span>
          <span class="chip" :title="share.expires_at">expires {{ relativeTime(share.expires_at) }}</span>
        </div>
        <div class="flex flex-wrap gap-1.5">
          <button class="btn btn-sm" @click="copy('Share link', shareLink(share))">Link</button>
          <button
            v-if="share.download && !share.is_dir && share.download_url"
            class="btn btn-sm"
            @click="copy('Direct link', share.download_url)"
          >
            Direct
          </button>
          <button class="btn btn-danger btn-sm ml-auto" @click="remove(share)">Delete</button>
        </div>
      </li>
    </ul>
  </section>
</template>
