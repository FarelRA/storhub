/**
 * Client-side preview classification: decide HOW a file can be previewed
 * before pulling its bytes, then sniff magic numbers / UTF-8 validity.
 * Pure functions - no DOM, no fetch - so everything is unit-testable.
 */

export const PREVIEW_MAX_BYTES = 2 * 1024 * 1024
export const SNIFF_BYTES = 64 * 1024

export type PreviewKind = 'text' | 'binary' | 'image' | 'video' | 'audio' | 'pdf' | 'too-large'

export function extOf(path: string): string {
  const name = path.split('/').pop() ?? ''
  const dot = name.lastIndexOf('.')
  if (dot <= 0 || dot === name.length - 1) return ''
  return name.slice(dot + 1).toLowerCase()
}

const EXT_KINDS: Record<string, Exclude<PreviewKind, 'text' | 'binary'>> = {
  png: 'image', jpg: 'image', jpeg: 'image', gif: 'image', webp: 'image',
  svg: 'image', avif: 'image', bmp: 'image', ico: 'image',
  mp4: 'video', m4v: 'video', webm: 'video', mkv: 'video', mov: 'video', avi: 'video',
  mp3: 'audio', wav: 'audio', ogg: 'audio', oga: 'audio', flac: 'audio', m4a: 'audio', opus: 'audio',
  pdf: 'pdf',
}

export function kindFromExtension(path: string): PreviewKind | null {
  return EXT_KINDS[extOf(path)] ?? null
}

export function mimeForKind(kind: PreviewKind, ext: string): string {
  switch (kind) {
    case 'pdf': return 'application/pdf'
    case 'image':
      if (ext === 'svg') return 'image/svg+xml'
      if (ext === 'jpg' || ext === 'jpeg') return 'image/jpeg'
      if (ext === 'ico') return 'image/x-icon'
      if (ext === 'avif') return 'image/avif'
      return `image/${ext}`
    case 'video':
      if (ext === 'mkv' || ext === 'mov' || ext === 'avi') return 'video/mp4'
      if (ext === 'm4v') return 'video/mp4'
      return `video/${ext === 'mp4' ? 'mp4' : ext}`
    case 'audio':
      if (ext === 'mp3') return 'audio/mpeg'
      if (ext === 'm4a') return 'audio/mp4'
      if (ext === 'opus') return 'audio/ogg'
      return `audio/${ext}`
    default: return 'application/octet-stream'
  }
}

/** (magic bytes, stream offset, resolved kind) triplets, first match wins. */
const MAGIC: Array<{ bytes: number[]; offset: number; kind: Exclude<PreviewKind, 'text'> }> = [
  { bytes: [0x89, 0x50, 0x4e, 0x47], offset: 0, kind: 'image' },
  { bytes: [0xff, 0xd8, 0xff], offset: 0, kind: 'image' },
  { bytes: [0x47, 0x49, 0x46, 0x38], offset: 0, kind: 'image' }, // GIF87a/89a
  { bytes: [0x25, 0x50, 0x44, 0x46], offset: 0, kind: 'pdf' }, // %PDF
  { bytes: [0x42, 0x4d], offset: 0, kind: 'image' }, // BMP
  { bytes: [0x00, 0x00, 0x01, 0x00], offset: 0, kind: 'image' }, // ICO
  { bytes: [0x1a, 0x45, 0xdf, 0xa3], offset: 0, kind: 'video' }, // Matroska/WebM
  { bytes: [0x66, 0x4c, 0x61, 0x43], offset: 0, kind: 'audio' }, // fLaC
  { bytes: [0x4f, 0x67, 0x67, 0x53], offset: 0, kind: 'audio' }, // OggS
  { bytes: [0x7f, 0x45, 0x4c, 0x46], offset: 0, kind: 'binary' }, // ELF
  { bytes: [0x50, 0x4b, 0x03, 0x04], offset: 0, kind: 'binary' }, // ZIP family
  { bytes: [0x52, 0x49, 0x46, 0x46], offset: 0, kind: 'audio' }, // RIFF… refined below
  { bytes: [0x66, 0x74, 0x79, 0x70], offset: 4, kind: 'video' }, // ….ftyp (MP4/MOV/M4V)
]

export function kindFromMagic(bytes: Uint8Array): PreviewKind | null {
  for (const sig of MAGIC) {
    if (sig.offset + sig.bytes.length > bytes.length) continue
    const hit = sig.bytes.every((b, i) => bytes[sig.offset + i] === b)
    if (!hit) continue
    if (sig.bytes[0] === 0x52 && bytes.length >= 12) {
      // RIFF container: subtype decides WAV vs AVI vs WEBP.
      const sub = String.fromCharCode(...bytes.slice(8, 12))
      if (sub === 'WAVE') return 'audio'
      if (sub === 'AVI ') return 'video'
      if (sub === 'WEBP') return 'image'
      return 'binary'
    }
    return sig.kind
  }
  return null
}

/** True when the buffer decodes as UTF-8 without NULs or heavy control chars. */
export function looksTextual(bytes: Uint8Array): boolean {
  let controls = 0
  for (const b of bytes) {
    if (b === 0) return false
    if (b < 0x20 && b !== 0x09 && b !== 0x0a && b !== 0x0d && b !== 0x0c) controls++
  }
  try {
    new TextDecoder('utf-8', { fatal: true }).decode(bytes)
  } catch {
    return false
  }
  return controls / Math.max(1, bytes.length) < 0.05
}

export function classify(bytes: Uint8Array): PreviewKind {
  const magic = kindFromMagic(bytes)
  if (magic) return magic
  return looksTextual(bytes) ? 'text' : 'binary'
}

function asciiColumn(slice: Uint8Array): string {
  let out = ''
  for (const b of slice) out += b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.'
  return out
}

export function toHexDump(
  bytes: Uint8Array,
  opts: { baseOffset?: number; bytesPerRow?: number; maxRows?: number } = {},
): string {
  const per = opts.bytesPerRow ?? 16
  const base = opts.baseOffset ?? 0
  const rows = Math.min(Math.ceil(bytes.length / per), opts.maxRows ?? Number.POSITIVE_INFINITY)
  const lines: string[] = []
  for (let row = 0; row < rows; row++) {
    const slice = bytes.subarray(row * per, (row + 1) * per)
    const hexParts: string[] = []
    for (let i = 0; i < per; i += 1) {
      hexParts.push(i < slice.length ? slice[i]!.toString(16).padStart(2, '0') : '  ')
      if (i === 7) hexParts.push(' ')
    }
    lines.push(
      `${(base + row * per).toString(16).padStart(8, '0')}  ${hexParts.join(' ')}  |${asciiColumn(slice)}|`,
    )
  }
  return lines.join('\n')
}
