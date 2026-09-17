import { TIMEOUTS } from '~/utils/limits'

export type ToastKind = 'success' | 'error' | 'info'

export interface Toast {
  id: number
  kind: ToastKind
  text: string
}

const toasts = ref<Toast[]>([])
let nextId = 1

function dismiss(id: number) {
  toasts.value = toasts.value.filter((toast) => toast.id !== id)
}

function push(kind: ToastKind, text: string) {
  const toast: Toast = { id: nextId++, kind, text }
  toasts.value = [...toasts.value.slice(-4), toast]
  setTimeout(() => dismiss(toast.id), kind === 'error' ? TIMEOUTS.TOAST_ERROR_MS : TIMEOUTS.TOAST_MS)
}

export function useToasts() {
  return {
    toasts,
    dismiss,
    success: (text: string) => push('success', text),
    error: (text: string) => push('error', text),
    info: (text: string) => push('info', text),
  }
}
