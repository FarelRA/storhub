/**
 * Pure anonymous-download guard, extracted for testability: downloads carry
 * the bearer header only when one exists (use-console) and must refuse
 * early only when the server would 401. Open servers (authEnabled === false)
 * and shared views serve bytes without a token, so blocking on `!token`
 * alone strands anonymous users with a bogus "Not authenticated" toast.
 */
export function canDownloadWithoutToken(
  authEnabled: boolean | undefined,
  hasToken: boolean,
  sharedMode: boolean,
): boolean {
  if (sharedMode || hasToken) return true
  return authEnabled === false
}
