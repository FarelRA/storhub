<script setup lang="ts">
import type { ProjectStats } from '~/utils/api-types'

defineProps<{ stats: ProjectStats }>()

// Mirrors shfs.FSStats exactly: files, directories, inodes, bytes,
// releases, assets. The server sends no timestamps, so there is no
// "Last modified" tile.
const tiles = computed(() => (stats: ProjectStats) => [
  { label: 'Files', value: formatCount(stats.files) },
  { label: 'Directories', value: formatCount(stats.directories) },
  { label: 'Inodes', value: formatCount(stats.inodes) },
  { label: 'Size', value: formatBytes(stats.bytes) },
  { label: 'Releases', value: formatCount(stats.releases) },
  { label: 'Assets', value: formatCount(stats.assets) },
])
</script>

<template>
  <div class="grid grid-cols-2 gap-2">
    <div v-for="tile in tiles(stats)" :key="tile.label" class="card px-3 py-2.5">
      <span class="block text-xs text-mist">{{ tile.label }}</span>
      <strong class="mt-0.5 block font-mono text-base font-semibold tabular-nums">{{ tile.value }}</strong>
    </div>
  </div>
</template>
