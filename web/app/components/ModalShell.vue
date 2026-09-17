<script setup lang="ts">
import { useFocusTrap } from '~/composables/use-focus-trap'

const props = defineProps<{
  open: boolean
  title: string
}>()

const emit = defineEmits<{ close: []; submit: [] }>()

useEscape(() => {
  if (props.open) emit('close')
})

const panel = ref<HTMLElement | null>(null)
const trapTab = useFocusTrap(panel)
let trigger: HTMLElement | null = null

watch(
  () => props.open,
  async (open) => {
    if (!open) {
      // Restore focus to whatever opened the dialog so keyboard users do not
      // land on the top of the page after closing.
      trigger?.focus()
      trigger = null
      return
    }
    trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null
    await nextTick()
    panel.value?.querySelector<HTMLElement>('input, textarea, select')?.focus()
  },
)
</script>

<template>
  <Teleport to="body">
    <div
      v-if="open"
      class="fixed inset-0 z-50 overflow-y-auto bg-black/70 p-4 backdrop-blur-[2px] motion-reduce:backdrop-blur-none sm:p-6"
      @mousedown.self="emit('close')"
    >
      <div
        ref="panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="modal-title"
        class="card mx-auto my-[7vh] w-full max-w-lg p-4 shadow-2xl"
        @keydown="trapTab"
      >
        <div class="mb-4 flex items-start justify-between gap-4">
          <h2 id="modal-title" class="font-mono text-base font-semibold">{{ title }}</h2>
          <button type="button" class="btn btn-sm" aria-label="Close dialog" @click="emit('close')">✕</button>
        </div>

        <form @submit.prevent="$emit('submit')">
          <slot />
        </form>
      </div>
    </div>
  </Teleport>
</template>
