import { describe, expect, it } from 'vitest'
import { joinApiPath } from '../app/utils/url'

describe('joinApiPath', () => {
  it.each([
    ['/api/v1', '/auth/login', '/api/v1/auth/login'],
    ['/api/v1', '/projects/a', '/api/v1/projects/a'],
    ['', '/auth/login', '/auth/login'],
    ['/', '/auth/login', '/auth/login'],
    ['', '/projects/a', '/projects/a'],
    // Already-prefixed input (from the url() helper) is idempotent.
    ['/api/v1', '/api/v1/projects/a', '/api/v1/projects/a'],
    ['/api', '/api/shares/tok', '/api/shares/tok'],
    // Trailing slashes on the configured base are normalized away.
    ['/api/v1///', '/nodes', '/api/v1/nodes'],
    // Non-root-relative garbage passes through untouched.
    ['/api/v1', 'https://other.example/x', 'https://other.example/x'],
    // Bare root resolves to the API root, never a double slash.
    ['/api/v1', '/', '/api/v1'],
    ['', '/', '/'],
  ])('joinApiPath(%j, %j) -> %j', (base, path, expected) => {
    expect(joinApiPath(base, path)).toBe(expected)
  })
})
