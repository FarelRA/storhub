<script setup lang="ts">
const console_ = useConsole()
const { selectedPath, selectedEntry, canEditFile, busy, editorIsText, previewKind, previewLoading } = console_

async function reload() {
  const entry = selectedEntry.value
  if (!entry || entry.is_dir || entry.is_symlink) return
  if (previewKind.value === 'text' && selectedPath.value) {
    await console_.readFile(selectedPath.value)
    return
  }
  await console_.loadPreview(entry)
}
</script>

<template>
  <section class="flex h-full min-h-0 min-w-0 flex-col gap-3">
    <header class="flex items-baseline justify-between gap-3">
      <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Preview</h2>
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
        :disabled="!canEditFile || !editorIsText || busy"
        :title="editorIsText ? 'Save (Cmd/Ctrl+S)' : 'Only text previews are saveable'"
        @click="console_.saveFile()"
      >
        Save <kbd class="hidden text-[10px] opacity-70 sm:inline">⌘S</kbd>
      </button>
    </div>

    <!-- Details for the selected entry -->
    <div class="min-h-0 border-t border-hair pt-3">
      <EntryDetails />
    </div>
  </section>
</template>
