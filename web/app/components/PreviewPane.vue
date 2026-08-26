<script setup lang="ts">
import { formatBytes } from '~/composables/use-format'

const console_ = useConsole()
const { previewKind, previewUrl, previewHex, previewMeta, editorContent, editorDirty, selectedPath } = console_

const meta = computed(() => {
  const { shown, total } = previewMeta.value
  if (previewKind.value === 'too-large') return `file is ${formatBytes(total)} — too large to auto-preview`
  if (previewKind.value === 'binary') return `first ${formatBytes(shown)} of ${formatBytes(total)}`
  if (previewKind.value === 'text' && shown < total) return `first ${formatBytes(shown)} of ${formatBytes(total)} — use Read range for more`
  return ''
})
</script>

<template>
  <!-- Text: the editable surface -->
  <textarea
    v-if="previewKind === 'text'"
    v-model="editorContent"
    spellcheck="false"
    class="min-h-64 w-full flex-1 resize-y rounded-lg border border-hair bg-[#191512] p-3 font-mono text-sm leading-relaxed text-parchment placeholder:text-mist/40 focus:border-ember focus:outline-none lg:min-h-0"
    placeholder="Select a file to preview it here"
    @input="editorDirty = true"
  />

  <!-- Images -->
  <div v-else-if="previewKind === 'image'" class="flex min-h-0 flex-1 items-center justify-center rounded-lg border border-hair bg-[#191512] p-2">
    <img
      :src="previewUrl"
      :alt="`Preview of ${selectedPath}`"
      class="max-h-[60vh] max-w-full rounded object-contain"
    >
  </div>

  <!-- Video -->
  <div v-else-if="previewKind === 'video'" class="flex min-h-0 flex-1 items-center justify-center rounded-lg border border-hair bg-black/40 p-2">
    <video :src="previewUrl" controls class="max-h-[60vh] w-full rounded" />
  </div>

  <!-- Audio -->
  <div v-else-if="previewKind === 'audio'" class="flex min-h-24 flex-1 items-center justify-center rounded-lg border border-hair bg-[#191512] px-4">
    <audio :src="previewUrl" controls class="w-full" />
  </div>

  <!-- PDF -->
  <iframe
    v-else-if="previewKind === 'pdf'"
    :src="previewUrl"
    title="PDF preview"
    class="min-h-64 w-full flex-1 rounded-lg border border-hair bg-white lg:min-h-0"
  />

  <!-- Binary: hex dump -->
  <div v-else-if="previewKind === 'binary'" class="flex min-h-0 flex-1 flex-col gap-1">
    <pre class="min-h-64 w-full flex-1 overflow-auto rounded-lg border border-hair bg-[#191512] p-3 font-mono text-xs leading-5 text-mist lg:min-h-0">{{ previewHex }}</pre>
  </div>

  <!-- Too large: explicit, with guidance -->
  <div
    v-else
    class="flex min-h-40 flex-1 flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-hair px-6 py-10 text-center"
  >
    <span aria-hidden="true" class="text-3xl opacity-60">📦</span>
    <p class="text-sm font-medium">Too large to preview</p>
    <p class="max-w-xs text-xs leading-relaxed text-mist">
      This file is {{ formatBytes(previewMeta.total) }}. Use the range bar below to read specific
      offsets, or Append / Patch / Truncate which work server-side without downloading.
    </p>
  </div>

  <p v-if="meta" class="font-mono text-xs text-mist/80">
    <span v-if="editorDirty && previewKind === 'text'" class="mr-1 text-ember" title="Unsaved changes">●</span>
    {{ meta }}
  </p>
</template>
