import { describe, expect, it } from 'vitest'
import { formatBytes, formatMode, normalizePath, parentPath, relativeTime, toDatetimeLocal } from '../app/composables/use-format'
import { ApiError } from '../app/utils/api-types'

describe('formatBytes', () => {
  it.each([
    [undefined, '—'],
    [0, '0 B'],
    [512, '512 B'],
    [1024, '1.0 KB'],
    [1536, '1.5 KB'],
    [1492725682, '1.4 GB'],
    [1024 ** 3 * 3, '3.0 GB'],
  ])('formats %s as %s', (input, expected) => {
    expect(formatBytes(input as number | undefined)).toBe(expected)
  })
})

describe('normalizePath / parentPath', () => {
  it('cleans traversal segments', () => {
    expect(normalizePath('a//b/../c/./')).toBe('a/c')
    expect(normalizePath('../../etc')).toBe('etc')
    expect(parentPath('a/b/c')).toBe('a/b')
    expect(parentPath('a')).toBe('')
  })
})

describe('relativeTime', () => {
  it('renders past and future relative to now', () => {
    const now = Date.parse('2026-08-26T12:00:00Z')
    expect(relativeTime(new Date(now - 90_000).toISOString(), now)).toContain('minute')
    expect(relativeTime(new Date(now + 3.6e6).toISOString(), now)).toContain('hour')
    expect(relativeTime(undefined)).toBe('—')
  })
})

describe('formatMode', () => {
  it('renders octal with leading zero', () => {
    expect(formatMode(0o644)).toBe('0644')
    expect(formatMode(0o755)).toBe('0755')
    expect(formatMode(undefined)).toBe('—')
  })
})

describe('toDatetimeLocal', () => {
  it('formats unix seconds for datetime-local inputs and rejects junk', () => {
    expect(toDatetimeLocal(0)).toBe('')
    const value = toDatetimeLocal(1750000000)
    expect(value).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/)
  })
})

describe('ApiError', () => {
  it('carries status and message', () => {
    const error = new ApiError(403, 'denied', { error: { message: 'denied' } })
    expect(error.status).toBe(403)
    expect(error.message).toBe('denied')
    expect(error.name).toBe('ApiError')
  })
})
