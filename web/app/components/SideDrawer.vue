<script setup lang="ts">
defineProps<{ open: boolean }>()
const emit = defineEmits<{ close: [] }>()

function onKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') emit('close')
}

onMounted(() => window.addEventListener('keydown', onKeydown))
onUnmounted(() => window.removeEventListener('keydown', onKeydown))
</script>

<template>
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

    <!-- Same element as the desktop sidebar: below lg it slides in/out,
         at lg the parent layout pins it statically. -->
    <aside
      class="fixed inset-y-0 left-0 z-40 w-72 shrink-0 overflow-y-auto border-r border-hair bg-[#1b1714] p-4 transition-transform duration-200 ease-out lg:static lg:z-auto lg:translate-x-0 lg:overflow-y-auto motion-reduce:transition-none"
      :class="open ? 'translate-x-0' : '-translate-x-full'"
      aria-label="Console controls"
    >
      <slot />
    </aside>
  </Teleport>
</template>
