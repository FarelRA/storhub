interface ConfirmOptions {
  title: string
  body: string
  confirmLabel?: string
  danger?: boolean
}

const pending = ref<ConfirmOptions | null>(null)
let resolver: ((ok: boolean) => void) | null = null

/**
 * Promise-based replacement for window.confirm(). The single ConfirmDialog
 * instance mounted in pages/index.vue renders whatever is asked here.
 */
export function useConfirm() {
  async function ask(options: ConfirmOptions): Promise<boolean> {
    pending.value = options
    return new Promise<boolean>((resolve) => {
      resolver = resolve
    })
  }
  function settle(ok: boolean) {
    pending.value = null
    resolver?.(ok)
    resolver = null
  }
  return { pending, ask, settle }
}
