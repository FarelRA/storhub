<script setup lang="ts">
const console_ = useConsole()
const { editorContent, editorDirty, selectedPath, selectedEntry, canEditFile, canReadRanges, busy } = console_
const rangeOffset = ref(0)
const rangeLength = ref(4096)

async function readRange() {
  await console_.readFile(selectedPath.value!, {
    offset: Math.floor(Number(rangeOffset.value)),
    length: Math.floor(Number(rangeLength.value)),
  })
}

function markDirty() {
  editorDirty.value = true
}
</script>

<template>
  <section class="flex h-full min-h-0 flex-col gap-3">
    <header class="flex items-baseline justify-between gap-3">
      <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Editor</h2>
      <p class="min-w-0 truncate font-mono text-xs text-mist" :title="selectedPath ?? undefined">
        {{ selectedPath || 'select an entry' }}
        <span v-if="editorDirty" class="ml-1 text-ember" title="Unsaved changes">●</span>
      </p>
    </header>

    <div class="flex flex-wrap gap-2 max-sm:[&>*]:flex-1">
      <button class="btn btn-sm" :disabled="!selectedPath || busy" @click="console_.readFile(selectedPath!)">
        Read
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
      <button class="btn btn-solid btn-sm ml-auto" :disabled="!canEditFile || busy" @click="console_.saveFile()">
        Save <kbd class="hidden text-[10px] opacity-70 sm:inline">⌘S</kbd>
      </button>
    </div>

    <textarea
      v-model="editorContent"
      spellcheck="false"
      class="min-h-64 w-full flex-1 resize-y rounded-lg border border-hair bg-[#191512] p-3 font-mono text-sm leading-relaxed text-parchment placeholder:text-mist/40 focus:border-ember focus:outline-none lg:min-h-0"
      :placeholder="selectedEntry?.is_symlink ? 'Symlink target' : 'File content appears here'"
      @input="markDirty"
    />

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
