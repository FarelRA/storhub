<script setup lang="ts">
const console_ = useConsole()
const { shares, selectedEntry } = console_
const toasts = useToasts()
const { ask } = useConfirm()

function shareLink(share: { id: string }): string {
  return `${window.location.origin}${window.location.pathname}?share=${encodeURIComponent(share.id)}`
}

async function copy(label: string, value: string) {
  try {
    await navigator.clipboard.writeText(value)
    toasts.success(`${label} copied`)
  } catch {
    window.prompt('Copy to clipboard:', value)
  }
}

async function createShare(download: boolean) {
  const path = console_.selectedPath.value
  if (!path) return
  const share = await console_.createShare(path, download)
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
  <section v-if="!console_.isSharedView.value" class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Shares</h2>

    <div class="flex flex-wrap gap-2">
      <button
        class="btn btn-sm"
        :disabled="!console_.selectedPath || !console_.project"
        title="Create a browser-only share for the selected entry"
        @click="createShare(false)"
      >
        Share selected…
      </button>
      <button
        class="btn btn-sm"
        :disabled="!console_.selectedPath || !console_.project || !!selectedEntry?.is_dir"
        title="Create a direct-download share for the selected file"
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
