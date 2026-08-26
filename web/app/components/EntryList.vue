<script setup lang="ts">
import type { EntryInfo } from '~/utils/api-types'

defineProps<{
  entries: EntryInfo[]
  selectedPath: string
}>()

const emit = defineEmits<{ select: [entry: EntryInfo] }>()

function glyph(entry: EntryInfo): string {
  if (entry.is_dir) return '▸'
  if (entry.is_symlink) return '↪'
  return '▪'
}

function glyphClass(entry: EntryInfo): string {
  if (entry.is_dir) return 'text-ember'
  if (entry.is_symlink) return 'text-sage'
  return 'text-mist/60'
}
</script>

<template>
  <ul class="flex flex-col gap-0.5">
    <li v-for="entry in entries" :key="entry.path">
      <button
        type="button"
        class="flex w-full items-center gap-3 rounded-lg border px-3 py-2.5 text-left transition-colors motion-reduce:transition-none"
        :class="
          selectedPath === entry.path
            ? 'border-hair bg-surface shadow-[inset_2px_0_0_0_var(--color-ember)]'
            : 'border-transparent hover:border-hair hover:bg-surface'
        "
        @click="emit('select', entry)"
      >
        <span aria-hidden="true" class="w-4 shrink-0 text-center" :class="glyphClass(entry)">
          {{ glyph(entry) }}
        </span>
        <span class="min-w-0 flex-1">
          <span class="block truncate font-medium">{{ entry.path.split('/').pop() }}</span>
          <span class="block truncate text-xs text-mist">
            {{ entry.is_dir ? 'directory' : entry.is_symlink ? `symlink → ${entry.symlink_target ?? '?'}` : 'file' }}
          </span>
        </span>
        <span v-if="!entry.is_dir && !entry.is_symlink" class="shrink-0 font-mono text-xs tabular-nums text-mist">
          {{ formatBytes(entry.size) }}
        </span>
      </button>
    </li>
  </ul>
</template>
