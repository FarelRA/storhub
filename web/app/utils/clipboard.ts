/** Clipboard helper with a legacy fallback; returns whether it succeeded. */
export async function copyText(value: string): Promise<boolean> {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value)
      return true
    } catch {
      // fall through to prompt
    }
  }
  window.prompt('Copy to clipboard:', value)
  return false
}
