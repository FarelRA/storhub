<script setup lang="ts">
import type { ProjectStats } from '~/utils/api-types'

const props = defineProps<{ stats: ProjectStats }>()

const consoleStore = useConsole()

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

// Loading and loaded-empty render the same dashes: show an explicit loading
// line while busy so the two states stay distinguishable.
const isLoading = computed(() => consoleStore.busy.value && tiles.value.every(tile => tile.value === '-'))
</script>

<template>
  <div>
    <p v-if="isLoading" class="mb-2 font-mono text-xs text-mist" role="status">Loading stats…</p>
    <div class="grid grid-cols-2 gap-2">
      <div v-for="tile in tiles" :key="tile.label" class="card px-3 py-2.5">
        <span class="block text-xs text-mist">{{ tile.label }}</span>
        <strong class="mt-0.5 block font-mono text-base font-semibold tabular-nums">{{ tile.value }}</strong>
      </div>
    </div>
  </div>
</template>
