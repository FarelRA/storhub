<script setup lang="ts">
import type { ProjectStats } from '~/utils/api-types'

defineProps<{ stats: ProjectStats }>()

const tiles = computed(() => (stats: ProjectStats) => [
  { label: 'Files', value: formatCount(stats.files) },
  { label: 'Directories', value: formatCount(stats.directories) },
  { label: 'Size', value: formatBytes(stats.bytes) },
  { label: 'Releases', value: formatCount(stats.releases) },
])
</script>

<template>
  <div class="grid grid-cols-2 gap-2">
    <div v-for="tile in tiles(stats)" :key="tile.label" class="card px-3 py-2.5">
      <span class="block text-xs text-mist">{{ tile.label }}</span>
      <strong class="mt-0.5 block font-mono text-base font-semibold tabular-nums">{{ tile.value }}</strong>
    </div>
    <p v-if="stats.last_modified" class="col-span-2 text-xs text-mist">
      Last modified {{ relativeTime(stats.last_modified) }}
    </p>
  </div>
</template>
