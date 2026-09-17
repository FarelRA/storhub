/**
 * Shared Escape-to-close: one global keydown registration per overlay
 * instead of the hand-rolled add/remove dance that lived in ModalShell,
 * SideDrawer, and the entry menu. The handler no-ops while `enabled` is
 * false so menus can reuse it without mounting churn.
 */
export function useEscape(onClose: () => void, enabled: Ref<boolean> | boolean = true): void {
  function handler(event: KeyboardEvent) {
    const active = typeof enabled === 'boolean' ? enabled : enabled.value
    if (active && event.key === 'Escape') onClose()
  }
  if (import.meta.client) {
    onMounted(() => window.addEventListener('keydown', handler))
    onUnmounted(() => window.removeEventListener('keydown', handler))
  }
}
