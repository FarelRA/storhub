<script setup lang="ts">
defineProps<{ icon?: string; title: string; hint?: string }>()
</script>

<template>
  <div class="flex flex-col items-center justify-center gap-2 rounded-xl border border-dashed border-hair px-6 py-12 text-center">
    <span aria-hidden="true" class="text-3xl opacity-60">{{ icon ?? '∅' }}</span>
    <p class="text-sm font-medium">{{ title }}</p>
    <p v-if="hint" class="max-w-xs text-xs leading-relaxed text-mist">{{ hint }}</p>
  </div>
</template>
