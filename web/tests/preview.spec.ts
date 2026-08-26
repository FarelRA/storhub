import { describe, expect, it } from 'vitest'
import {
  PREVIEW_MAX_BYTES,
  SNIFF_BYTES,
  classify,
  extOf,
  kindFromExtension,
  kindFromMagic,
  looksTextual,
  mimeForKind,
  toHexDump,
} from '../app/utils/preview'

const enc = (text: string) => new TextEncoder().encode(text)

describe('extOf / kindFromExtension', () => {
  it('extracts lowercase extensions and ignores dotfiles', () => {
    expect(extOf('a/b/Photo.JPG')).toBe('jpg')
    expect(extOf('noext')).toBe('')
    expect(extOf('.bashrc')).toBe('')
    expect(kindFromExtension('movie.mkv')).toBe('video')
    expect(kindFromExtension('notes.txt')).toBeNull()
  })
})

describe('kindFromMagic', () => {
  const u8 = (...bytes: number[]) => Uint8Array.from(bytes)
  const riff = (sub: string) => [...enc('RIFF'), 0, 0, 0, 0, ...enc(sub)]

  it.each([
    ['png', u8(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a), 'image'],
    ['jpeg', u8(0xff, 0xd8, 0xff, 0xe0), 'image'],
    ['gif', enc('GIF89a'), 'image'],
    ['pdf', enc('%PDF-1.7 …'), 'pdf'],
    ['webm', u8(0x1a, 0x45, 0xdf, 0xa3), 'video'],
    ['mp4', [...u8(0, 0, 0), 0x18, ...enc('ftypisom')], 'video'],
    ['wav', Uint8Array.from(riff('WAVE')), 'audio'],
    ['avi', Uint8Array.from(riff('AVI ')), 'video'],
    ['webp', Uint8Array.from(riff('WEBP')), 'image'],
    ['zip', u8(0x50, 0x4b, 0x03, 0x04), 'binary'],
    ['elf', u8(0x7f, 0x45, 0x4c, 0x46), 'binary'],
  ])('detects %s', (_name, input, expected) => {
    expect(kindFromMagic(Uint8Array.from(input as number[]))).toBe(expected)
  })

  it('returns null for unknown bytes', () => {
    expect(kindFromMagic(enc('hello world, nothing magic'))).toBeNull()
  })
})

describe('looksTextual / classify', () => {
  const u8 = (...bytes: number[]) => Uint8Array.from(bytes)
  it('accepts utf-8 text and rejects NULs or invalid sequences', () => {
    expect(looksTextual(enc('# heading\nplain text ✓\n'))).toBe(true)
    expect(looksTextual(enc('bin\x00ary'))).toBe(false)
    expect(looksTextual(Uint8Array.from([0xff, 0xfe, 0xfa]))).toBe(false)
  })

  it('classify prefers magic over extension hints and falls back to text', () => {
    expect(classify(u8(0x89, 0x50, 0x4e, 0x47))).toBe('image')
    expect(classify(enc('{ "json": true }\n'))).toBe('text')
    expect(classify(u8(0x50, 0x4b, 0x03, 0x04, 1, 2, 3))).toBe('binary')
  })
})

describe('mimeForKind', () => {
  it('maps special cases and falls back to octet-stream', () => {
    expect(mimeForKind('image', 'svg')).toBe('image/svg+xml')
    expect(mimeForKind('audio', 'mp3')).toBe('audio/mpeg')
    expect(mimeForKind('video', 'mkv')).toBe('video/mp4')
    expect(mimeForKind('text', '')).toBe('application/octet-stream')
  })
})

describe('toHexDump', () => {
  const bytes = Uint8Array.from([...enc('AB'), 0x00, 0x7f])
  it('renders offset, hex pairs, and ascii column with dots for non-printables', () => {
    const dump = toHexDump(bytes, { baseOffset: 0x10 })
    const line = dump.split('\n')[0]!
    expect(line.startsWith('00000010')).toBe(true)
    expect(line).toContain('41 42 00 7f')
    expect(line.endsWith('|AB..|')).toBe(true)
  })

  it('honors maxRows and pads short rows with blanks', () => {
    const dump = toHexDump(bytes, { bytesPerRow: 8, maxRows: 1 })
    expect(dump.split('\n')).toHaveLength(1)
    expect(dump).toMatch(/\|\s*$/)
  })
})

describe('thresholds are coherent', () => {
  it('sniff window never exceeds the inline cap', () => {
    expect(SNIFF_BYTES).toBeLessThanOrEqual(PREVIEW_MAX_BYTES)
  })
})
