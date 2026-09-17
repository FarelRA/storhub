/**
 * Shared Tab focus trap for modal overlays, extracted from ModalShell.
 * Returns the keydown handler to bind on the dialog panel; focus restore
 * stays with the shell (it owns the trigger element).
 */
const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'

export function useFocusTrap(panel: Ref<HTMLElement | null>): (event: KeyboardEvent) => void {
  return function trapTab(event: KeyboardEvent): void {
    if (event.key !== 'Tab' || !panel.value) return
    const focusable = [...panel.value.querySelectorAll<HTMLElement>(FOCUSABLE)].filter(
      el => el.offsetParent !== null,
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
}
