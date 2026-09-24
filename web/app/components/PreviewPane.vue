<script setup lang="ts">
const consoleStore = useConsole()
const { previewKind, previewUrl, previewHex, previewMeta, editorContent, editorDirty, selectedPath, selectedEntry, busy, previewLoading } = consoleStore

async function downloadSelected() {
  const entry = selectedEntry.value
  // The pane button, unlike the row menu, never pre-filters by kind: only
  // regular files have bytes worth downloading.
  if (!entry || entry.is_dir || entry.is_symlink) return
  await consoleStore.downloadEntry(entry)
}

// Transparency-friendly checkerboard behind images.
const checker =
  'bg-[conic-gradient(#241f1b_0_25%,#1b1714_0_50%,#241f1b_0_75%,#1b1714_0)] bg-[length:16px_16px]'

/**
 * Non-previewable selections get the centered state card instead of an
 * empty text box: nothing selected, a directory, or a symlink.
 */
const stateCard = computed<{ icon: string; title: string; hint: string } | null>(() => {
  const entry = selectedEntry.value
  if (!selectedPath.value || !entry) {
    return {
      icon: '📄',
      title: 'No file selected',
      hint: 'Pick an entry from the directory pane. Its preview will appear here.',
    }
  }
  if (entry.is_dir) {
    return {
      icon: '🗂',
      title: 'Directory selected',
      hint: 'Directories have no content to preview. Browse or open the files inside via the ⋮ menu.',
    }
  }
  if (entry.is_symlink) {
    return {
      icon: '↪',
      title: 'Symlink selected',
      hint: `Points to ${entry.symlink_target ?? 'an unknown target'}. Preview the target file itself.`,
    }
  }
  return null
})

// One branch decision for the whole pane: loading and state cards win over
// the kind-specific renderers below.
const view = computed(() => {
  if (previewLoading.value) return 'loading'
  if (stateCard.value) return 'state'
  return previewKind.value
})
</script>

<template>
  <div class="relative flex min-h-40 min-w-0 flex-1 flex-col gap-2 lg:min-h-0">
    <!-- Loading veil -->
    <div
      v-if="view === 'loading'"
      class="absolute inset-0 z-10 flex flex-col items-center justify-center gap-3 rounded-lg bg-shell/70 backdrop-blur-[1px]"
      role="status"
      aria-label="Loading preview"
    >
      <span class="h-8 w-8 animate-spin rounded-full border-2 border-hair border-t-ember motion-reduce:animate-none" />
      <span class="font-mono text-xs text-mist">Loading preview…</span>
    </div>

    <!-- Non-previewable selection: centered state card -->
    <div
      v-else-if="view === 'state'"
      class="flex w-full flex-1 flex-col items-center justify-center gap-3 px-6 py-10 text-center"
    >
      <span aria-hidden="true" class="text-3xl opacity-60">{{ stateCard?.icon }}</span>
      <p class="text-sm font-medium">{{ stateCard?.title }}</p>
      <p class="max-w-xs text-xs leading-relaxed text-mist">{{ stateCard?.hint }}</p>
    </div>

    <!-- Text: fills the pane's height, wraps, scrolls when longer -->
    <textarea
      v-else-if="view === 'text'"
      v-model="editorContent"
      wrap="soft"
      spellcheck="false"
      class="min-h-24 min-w-0 w-full flex-1 resize-none overflow-y-auto rounded-lg border border-hair bg-[#191512] p-3 font-mono text-sm leading-relaxed text-parchment placeholder:text-mist/40 focus:border-ember focus:outline-none"
      placeholder="Select a file to preview it here"
      @input="editorDirty = true"
    />

    <!-- Binary hex dump -->
    <pre
      v-else-if="view === 'binary'"
      class="min-h-0 w-full flex-1 overflow-auto rounded-lg border border-hair bg-[#191512] p-3 font-mono text-xs leading-5 text-mist"
    >{{ previewHex }}</pre>

    <!-- Image -->
    <div
      v-else-if="view === 'image'"
      class="flex min-h-0 w-full flex-1 items-center justify-center overflow-hidden rounded-lg border border-hair p-3"
      :class="checker"
    >
      <img
        :src="previewUrl"
        :alt="`Preview of ${selectedPath}`"
        class="max-h-full max-w-full object-contain drop-shadow-[0_2px_8px_rgba(0,0,0,0.5)]"
      >
    </div>

    <!-- Video -->
    <div v-else-if="view === 'video'" class="flex min-h-0 w-full flex-1 items-center justify-center">
      <video :src="previewUrl" controls class="max-h-full max-w-full rounded-lg shadow-lg" />
    </div>

    <!-- Audio -->
    <div v-else-if="view === 'audio'" class="flex w-full items-center justify-center py-6">
      <audio :src="previewUrl" controls class="w-full max-w-md" />
    </div>

    <!-- PDF -->
    <iframe
      v-else-if="view === 'pdf'"
      :src="previewUrl"
      title="PDF preview"
      class="mx-auto aspect-[8.5/11] w-full max-w-xl self-start overflow-hidden rounded-lg border border-hair bg-white"
    />

    <!-- Preview fetch failed (not "too large": say what happened) -->
    <div
      v-else-if="view === 'error'"
      class="flex w-full flex-1 flex-col items-center justify-center gap-3 px-6 py-10 text-center"
    >
      <span aria-hidden="true" class="text-3xl opacity-60">⚠️</span>
      <p class="text-sm font-medium">Preview failed</p>
      <p class="max-w-xs text-xs leading-relaxed text-mist">
        The file could not be fetched for preview. See the error notification for details.
      </p>
    </div>

    <!-- Too large -->
    <div
      v-else
      class="flex w-full flex-1 flex-col items-center justify-center gap-3 px-6 py-10 text-center"
    >
      <span aria-hidden="true" class="text-3xl opacity-60">📦</span>
      <p class="text-sm font-medium">Too large to preview</p>
      <p class="max-w-xs text-xs leading-relaxed text-mist">
        This file is {{ formatBytes(previewMeta.total) }}. Use the server-side
        Append / Patch / Truncate actions from the ⋮ menu, or download it natively.
      </p>
      <button class="btn btn-sm" :disabled="!selectedPath || busy" @click="downloadSelected">
        ⬇ Download {{ formatBytes(previewMeta.total) }}
      </button>
    </div>
  </div>
</template>
