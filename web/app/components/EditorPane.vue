<script setup lang="ts">
const consoleStore = useConsole()
const { selectedPath, selectedEntry, canEditFile, isSaveable, busy, editorIsText, previewKind, previewLoading, previewMeta, editorDirty } = consoleStore

// Right-aligned status on the PREVIEW header line.
const meta = computed(() => {
  const { shown, total } = previewMeta.value
  if (!selectedPath.value || !selectedEntry.value) return ''
  if (selectedEntry.value.is_dir || selectedEntry.value.is_symlink) return ''
  if (previewKind.value === 'too-large') return `file is ${formatBytes(total)}. Too large to auto-preview.`
  if (previewKind.value === 'binary') return `first ${formatBytes(shown)} of ${formatBytes(total)}`
  if (previewKind.value === 'text' && shown < total) return `first ${formatBytes(shown)} of ${formatBytes(total)}`
  return ''
})

// The Save button distinguishes "not text" from "text but truncated": only
// a complete text fetch with a CAS token may be PUT back.
const saveTitle = computed(() => {
  if (!canEditFile.value) return 'Select a file to save'
  if (isSaveable.value) return 'Save (Cmd/Ctrl+S)'
  if (editorIsText.value) return 'Only complete text previews are saveable (this one is truncated)'
  return 'Only text previews are saveable'
})

async function reload() {
  const entry = selectedEntry.value
  if (!entry || entry.is_dir || entry.is_symlink) return
  if (previewKind.value === 'text' && selectedPath.value) {
    await consoleStore.readFile(selectedPath.value)
    return
  }
  await consoleStore.loadPreview(entry)
}
</script>

<template>
  <section class="flex h-full min-h-0 min-w-0 flex-col gap-3">
    <header class="flex items-baseline justify-between gap-3">
      <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Preview</h2>
      <p
        v-if="meta || editorDirty"
        class="min-w-0 truncate text-right font-mono text-xs text-mist"
        :title="meta"
      >
        <span v-if="editorDirty && previewKind === 'text'" class="mr-1 text-ember" title="Unsaved changes">●</span>
        {{ meta }}
      </p>
    </header>

    <PreviewPane />

    <!-- Actions directly under the preview content -->
    <div class="flex items-center gap-2">
      <button
        class="btn btn-sm"
        :disabled="!selectedPath || busy || previewLoading"
        title="Reload the preview from the server"
        @click="reload"
      >
        ⟳ Reload
      </button>
      <button
        class="btn btn-solid btn-sm ml-auto"
        :disabled="!canEditFile || !isSaveable || busy"
        :title="saveTitle"
        @click="consoleStore.saveFile()"
      >
        Save <kbd class="hidden text-[10px] opacity-70 sm:inline">⌘S</kbd>
      </button>
    </div>

    <!-- Details for the selected entry -->
    <div class="min-h-0 space-y-4 border-t border-hair pt-3">
      <EntryDetails />
      <XattrPanel />
    </div>
  </section>
</template>
