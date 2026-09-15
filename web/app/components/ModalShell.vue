<script setup lang="ts">
const props = defineProps<{
  open: boolean
  title: string
}>()

const emit = defineEmits<{ close: []; submit: [] }>()

function onKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') emit('close')
}

watch(
  () => props.open,
  (open) => {
    if (import.meta.client) {
      if (open) window.addEventListener('keydown', onKeydown)
      else window.removeEventListener('keydown', onKeydown)
    }
  },
)

onUnmounted(() => window.removeEventListener('keydown', onKeydown))

const panel = ref<HTMLElement | null>(null)
let trigger: HTMLElement | null = null

const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'

function trapTab(event: KeyboardEvent) {
  if (event.key !== 'Tab' || !panel.value) return
  const focusable = [...panel.value.querySelectorAll<HTMLElement>(FOCUSABLE)].filter(
    (el) => el.offsetParent !== null,
  )
  if (!focusable.length) return
  const first = focusable[0]
  const last = focusable.at(-1)
  if (!first || !last) return
  const active = document.activeElement
  if (event.shiftKey && (active === first || !panel.value.contains(active))) {
    event.preventDefault()
    last.focus()
  } else if (!event.shiftKey && active === last) {
    event.preventDefault()
    first.focus()
  }
}

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
