<script setup lang="ts">
defineProps<{ open: boolean; width?: number }>()
const emit = defineEmits<{ close: [] }>()

function onKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') emit('close')
}

onMounted(() => window.addEventListener('keydown', onKeydown))
onUnmounted(() => window.removeEventListener('keydown', onKeydown))
</script>

<template>
  <!-- Mobile-only backdrop; the aside itself must stay in the flex row so it
       is a true first column on desktop. -->
  <Teleport to="body">
    <Transition
      enter-active-class="transition-opacity duration-200 motion-reduce:transition-none"
      enter-from-class="opacity-0"
      leave-active-class="transition-opacity duration-150 motion-reduce:transition-none"
      leave-to-class="opacity-0"
    >
      <div
        v-if="open"
        class="fixed inset-0 z-40 bg-black/60 lg:hidden"
        aria-hidden="true"
        @click="emit('close')"
      />
    </Transition>
  </Teleport>

  <!-- First column of the app shell: slides on <lg, pinned static at lg+.
       Width comes from the resizable-panel store (desktop only).
       max-lg:z-50 keeps the drawer ABOVE its own backdrop (which lives at
       the end of <body> and would otherwise paint over it at equal z). -->
  <aside
    class="shrink-0 border-r border-hair bg-[#1b1714] p-4
           max-lg:fixed max-lg:inset-y-0 max-lg:left-0 max-lg:z-50 max-lg:w-72 max-lg:overflow-y-auto
           max-lg:transition-transform max-lg:duration-200 max-lg:ease-out max-lg:motion-reduce:transition-none
           lg:sticky lg:top-14 lg:h-[calc(100dvh-3.5rem)] lg:self-start lg:overflow-y-auto lg:[width:var(--panel-w,18rem)]"
    :class="open ? 'max-lg:translate-x-0' : 'max-lg:-translate-x-full'"
    :style="{ '--panel-w': width ? `${width}px` : undefined }"
    aria-label="Console controls"
  >
    <slot />
  </aside>
</template>
