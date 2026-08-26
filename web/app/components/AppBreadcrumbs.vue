<script setup lang="ts">
const props = defineProps<{
  project: string
  path: string
}>()

const emit = defineEmits<{ navigate: [path: string] }>()

const segments = computed(() => {
  const parts = props.path ? props.path.split('/') : []
  return parts.map((name, index) => ({
    name,
    target: parts.slice(0, index + 1).join('/'),
  }))
})
</script>

<template>
  <nav aria-label="Directory breadcrumb" class="min-w-0 flex-1 overflow-x-auto font-mono text-sm">
    <ol class="flex items-center gap-1 whitespace-nowrap">
      <li>
        <button
          type="button"
          class="rounded px-1 py-0.5 text-mist hover:text-ember"
          :aria-disabled="!path"
          @click="emit('navigate', '')"
        >
          {{ project || 'no project' }}
        </button>
      </li>
      <li v-for="(segment, index) in segments" :key="segment.target" class="flex items-center gap-1">
        <span aria-hidden="true" class="text-hair">/</span>
        <button
          v-if="index < segments.length - 1"
          type="button"
          class="rounded px-1 py-0.5 text-mist hover:text-ember"
          @click="emit('navigate', segment.target)"
        >
          {{ segment.name }}
        </button>
        <span v-else aria-current="page" class="px-1 py-0.5 font-medium text-parchment">
          {{ segment.name }}
        </span>
      </li>
      <li v-if="!segments.length" aria-hidden="true">
        <span class="text-hair">/</span>
      </li>
    </ol>
  </nav>
</template>
