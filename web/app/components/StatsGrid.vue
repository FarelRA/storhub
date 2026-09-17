<script setup lang="ts">
import type { ProjectStats } from '~/utils/api-types'

const props = defineProps<{ stats: ProjectStats }>()

// Mirrors shfs.FSStats exactly: files, directories, inodes, bytes,
// releases, assets. The server sends no timestamps, so there is no
// "Last modified" tile.
const tiles = computed(() => [
  { label: 'Files', value: formatCount(props.stats.files) },
  { label: 'Directories', value: formatCount(props.stats.directories) },
  { label: 'Inodes', value: formatCount(props.stats.inodes) },
  { label: 'Size', value: formatBytes(props.stats.bytes) },
  { label: 'Releases', value: formatCount(props.stats.releases) },
  { label: 'Assets', value: formatCount(props.stats.assets) },
])
</script>

<template>
  <div class="grid grid-cols-2 gap-2">
    <div v-for="tile in tiles" :key="tile.label" class="card px-3 py-2.5">
      <span class="block text-xs text-mist">{{ tile.label }}</span>
      <strong class="mt-0.5 block font-mono text-base font-semibold tabular-nums">{{ tile.value }}</strong>
    </div>
  </div>
</template>
