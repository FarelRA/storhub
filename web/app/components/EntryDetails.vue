<script setup lang="ts">
import type { EntryInfo } from '~/utils/api-types'

const console_ = useConsole()

const rows = computed(() => {
  const entry = console_.selectedEntry.value as EntryInfo | null
  if (!entry) {
    // Always render the full key set so the panel never jumps around.
    return ['path', 'kind', 'mode', 'uid / gid', 'size', 'inode', 'links', 'modified'].map((key) => ({
      key,
      value: '—',
    }))
  }
  return [
    { key: 'path', value: entry.path || '/' },
    { key: 'kind', value: entry.is_dir ? 'directory' : entry.is_symlink ? 'symlink' : 'file' },
    { key: 'mode', value: formatMode(entry.mode) },
    { key: 'uid / gid', value: `${entry.uid ?? '—'} / ${entry.gid ?? '—'}` },
    { key: 'size', value: entry.is_dir ? '—' : formatBytes(entry.size) },
    { key: 'inode', value: String(entry.inode ?? '—') },
    { key: 'links', value: String(entry.nlink ?? '—') },
    ...(entry.symlink_target ? [{ key: 'target', value: entry.symlink_target }] : []),
    { key: 'modified', value: relativeTime(entry.modified_at) },
  ]
})
</script>

<template>
  <section class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Entry details</h2>
    <dl class="grid grid-cols-[88px_1fr] gap-x-3 gap-y-1.5 text-sm">
      <template v-for="row in rows" :key="row.key">
        <dt class="text-xs leading-6 text-mist">{{ row.key }}</dt>
        <dd class="min-w-0 break-words font-mono text-xs leading-6" :title="row.value">{{ row.value }}</dd>
      </template>
    </dl>
  </section>
</template>
