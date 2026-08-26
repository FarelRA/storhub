<script setup lang="ts">
import type { PanelKey } from '~/composables/use-panel-widths'

const props = defineProps<{ panel: PanelKey }>()

const { panels, setWidth, resetPanel, persist } = usePanelWidths()

const dragging = ref(false)
let startX = 0
let startWidth = 0

function onPointerDown(event: PointerEvent) {
  dragging.value = true
  startX = event.clientX
  startWidth = panels.value[props.panel]
  const el = event.currentTarget as HTMLElement
  el.setPointerCapture(event.pointerId)
}

function onPointerMove(event: PointerEvent) {
  if (!dragging.value) return
  setWidth(props.panel, startWidth + (event.clientX - startX))
}

function endDrag() {
  if (!dragging.value) return
  dragging.value = false
  persist()
}

function onKeydown(event: KeyboardEvent) {
  const step = event.shiftKey ? 48 : 16
  switch (event.key) {
    case 'ArrowLeft':
      event.preventDefault()
      setWidth(props.panel, panels.value[props.panel] - step)
      persist()
      break
    case 'ArrowRight':
      event.preventDefault()
      setWidth(props.panel, panels.value[props.panel] + step)
      persist()
      break
    case 'Home':
      event.preventDefault()
      resetPanel(props.panel)
      persist()
      break
  }
}
</script>

<template>
  <div
    role="separator"
    :aria-orientation="'vertical'"
    :aria-label="`Resize ${panel} panel`"
    :aria-valuenow="panels[panel]"
    tabindex="0"
    class="group relative z-10 hidden w-1 shrink-0 cursor-col-resize bg-transparent
           transition-colors motion-reduce:transition-none
           hover:bg-ember/50 focus-visible:bg-ember focus-visible:outline-none
           data-[dragging]:bg-ember lg:block lg:self-stretch"
    :data-dragging="dragging ? '' : undefined"
    @pointerdown="onPointerDown"
    @pointermove="onPointerMove"
    @pointerup="endDrag"
    @pointercancel="endDrag"
    @dblclick="resetPanel(panel); persist()"
    @keydown="onKeydown"
  >
    <!-- Wider hit target without widening the visible line -->
    <span class="absolute inset-y-0 -left-1.5 -right-1.5" aria-hidden="true" />
    <!-- Drag shield: swallows pointer events (iframes!) for the whole page -->
    <div v-if="dragging" class="fixed inset-0 z-[70] cursor-col-resize" aria-hidden="true" />
  </div>
</template>
