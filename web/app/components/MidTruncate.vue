<script setup lang="ts">
/**
 * Proportional middle truncation.
 *
 * When the name exceeds its column, BOTH sides give up width in proportion
 * to their natural share (a long head yields more pixels than a short tail,
 * so neither side is sacrificed first), keeping the extension/suffix at the
 * end: "Doraemon the M…phony (2024).mkv".
 *
 * Measurement uses canvas text metrics with the element's own font - no DOM
 * thrash - recomputed on container resize, text change, and font load.
 */
const props = defineProps<{ text: string; tailLength?: number }>()

const root = ref<HTMLElement | null>(null)
const headShow = ref('')
const tailShow = ref('')
const truncated = ref(false)

let ctx: CanvasRenderingContext2D | null | undefined

function measurer(el: HTMLElement): CanvasRenderingContext2D | null {
  if (ctx !== undefined) return ctx
  const canvas = document.createElement('canvas')
  ctx = canvas.getContext('2d')
  void el
  return ctx
}

function textWidth(el: HTMLElement, str: string): number {
  const c = measurer(el)
  if (!c) return str.length // last-resort estimate
  const style = getComputedStyle(el)
  c.font = `${style.fontWeight} ${style.fontSize} ${style.fontFamily}`
  return c.measureText(str).width
}

/** Longest prefix of s that fits maxPx (binary search over character cuts). */
function fitPrefix(s: string, maxPx: number, el: HTMLElement): string {
  if (s === '' || textWidth(el, s) <= maxPx) return s
  let lo = 0
  let hi = s.length
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2)
    if (textWidth(el, s.slice(0, mid)) <= maxPx) lo = mid
    else hi = mid - 1
  }
  return s.slice(0, lo)
}

/** Longest suffix of s that fits maxPx. */
function fitSuffix(s: string, maxPx: number, el: HTMLElement): string {
  if (s === '' || textWidth(el, s) <= maxPx) return s
  let lo = 0
  let hi = s.length
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2)
    if (textWidth(el, s.slice(s.length - mid)) <= maxPx) lo = mid
    else hi = mid - 1
  }
  return s.slice(s.length - lo)
}

function recompute() {
  const el = root.value
  if (!el) return
  const width = el.clientWidth
  if (width <= 0) return

  const natural = textWidth(el, props.text)
  if (natural <= width) {
    truncated.value = false
    return
  }

  const ext = props.text.match(/\.([A-Za-z0-9]{1,8})$/)
  const preferredTail = Math.min(
    props.text.length,
    props.tailLength ?? Math.max(ext ? 8 : 4, Math.ceil(props.text.length / 3)),
  )
  const headPart = props.text.slice(0, props.text.length - preferredTail)
  const tailPart = props.text.slice(-preferredTail)

  const dotsWidth = textWidth(el, '…')
  const avail = Math.max(width - dotsWidth, 4)
  const headNatural = textWidth(el, headPart)
  const tailNatural = textWidth(el, tailPart)
  const total = headNatural + tailNatural || 1

  // Proportional budget: each side loses width relative to its own share.
  const headBudget = (avail * headNatural) / total
  const tailBudget = avail - headBudget

  headShow.value = fitPrefix(headPart, headBudget, el)
  tailShow.value = fitSuffix(tailPart, tailBudget, el)
  truncated.value = true
}

let observer: ResizeObserver | null = null

onMounted(() => {
  observer = new ResizeObserver(() => recompute())
  if (root.value) observer.observe(root.value)
  recompute()
  // Web fonts landing later change metrics; remeasure once they settle.
  document.fonts?.ready.then(() => recompute()).catch(() => {})
})
onUnmounted(() => {
  observer?.disconnect()
  observer = null
})
watch(() => props.text, recompute)
</script>

<template>
  <span ref="root" :title="text" class="block min-w-0 overflow-hidden whitespace-nowrap">
    <template v-if="!truncated">{{ text }}</template>
    <template v-else>{{ headShow }}…{{ tailShow }}</template>
  </span>
</template>
