/**
 * Single viewport oracle for the whole console: one 1024px (Tailwind lg)
 * breakpoint answer instead of three competing ones (EntryList's
 * coarse+no-hover+small compound, SideDrawer's min-width:1024px, the
 * gutter's lg:block CSS). Module-scoped singletons: every consumer reads
 * the same matchMedia result.
 */
const isDesktop = ref(false)
const isCoarsePointer = ref(false)
const hasNoHover = ref(false)
const hasTouch = ref(false)
let listening = false

function refresh(): void {
  isDesktop.value = window.matchMedia('(min-width: 1024px)').matches
  isCoarsePointer.value = window.matchMedia('(pointer: coarse)').matches
  hasNoHover.value = window.matchMedia('(hover: none)').matches
  hasTouch.value = navigator.maxTouchPoints > 0
}

export function useBreakpoint() {
  if (import.meta.client && !listening) {
    listening = true
    refresh()
    window.addEventListener('resize', refresh)
    // Devices that toggle coarse/hover at runtime (docked tablets).
    for (const query of ['(pointer: coarse)', '(hover: none)', '(min-width: 1024px)']) {
      try {
        window.matchMedia(query).addEventListener('change', refresh)
      } catch {
        // Older browsers without MediaQueryList.addEventListener.
      }
    }
  }
  return { isDesktop, isCoarsePointer, hasNoHover, hasTouch }
}
