<script setup lang="ts">
const console_ = useConsole()
const { selectedPath, selectedEntry, canEditFile, canReadRanges, busy, editorIsText, previewKind } = console_

const rangeOffset = ref(0)
const rangeLength = ref(4096)

async function readRange() {
  await console_.readFile(selectedPath.value!, {
    offset: Math.floor(Number(rangeOffset.value)),
    length: Math.floor(Number(rangeLength.value)),
  })
}
</script>

<template>
  <section class="flex h-full min-h-0 flex-col gap-3">
    <header class="flex items-baseline justify-between gap-3">
      <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Preview</h2>
      <p class="min-w-0 truncate font-mono text-xs text-mist" :title="selectedPath ?? undefined">
        {{ selectedPath || 'select an entry' }}
      </p>
    </header>

    <div class="flex flex-wrap gap-2 max-sm:[&>*]:flex-1">
      <button class="btn btn-sm" :disabled="!selectedPath || busy" title="Load as editable text" @click="console_.readFile(selectedPath!)">
        Read as text
      </button>
      <button class="btn btn-sm" :disabled="!canEditFile || busy" @click="console_.openModal('append')">
        Append
      </button>
      <button class="btn btn-sm" :disabled="!canEditFile || busy" @click="console_.openModal('patch')">
        Patch
      </button>
      <button class="btn btn-sm" :disabled="!canEditFile || busy" @click="console_.openModal('truncate')">
        Truncate
      </button>
      <ConfirmRemove />
      <button
        v-if="previewKind !== 'text' && selectedPath && !selectedEntry?.is_dir && !selectedEntry?.is_symlink"
        class="btn btn-sm"
        :disabled="busy"
        title="Re-run the automatic preview"
        @click="console_.loadPreview(selectedEntry!)"
      >
        Auto preview
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

    <PreviewPane />

    <div class="grid grid-cols-[1fr_1fr_auto] items-end gap-2 max-sm:grid-cols-1">
      <label class="block">
        <span class="field-label">Offset</span>
        <input v-model.number="rangeOffset" type="number" min="0" step="1" class="input font-mono" >
      </label>
      <label class="block">
        <span class="field-label">Length</span>
        <input v-model.number="rangeLength" type="number" min="1" step="1" class="input font-mono" >
      </label>
      <button class="btn btn-sm" :disabled="!canReadRanges || busy" @click="readRange()">Read range</button>
    </div>
  </section>
</template>
