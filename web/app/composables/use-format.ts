/** Human-friendly formatting shared across the console. */

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'] as const

export function formatBytes(value: number | undefined | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) return '—'
  if (value < 1024) return `${value} B`
  let amount = value
  let unit: (typeof BYTE_UNITS)[number] = 'B'
  for (const candidate of BYTE_UNITS) {
    unit = candidate
    if (amount < 1024) break
    if (candidate !== BYTE_UNITS.at(-1)) amount /= 1024
  }
  const digits = amount >= 100 ? 0 : 1
  return `${amount.toFixed(digits)} ${unit}`
}

export function formatCount(value: number | undefined | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) return '—'
  return Number(value).toLocaleString()
}

export function formatMode(mode: number | undefined): string {
  if (mode === undefined) return '—'
  return `0${(mode & 0o777).toString(8).padStart(3, '0')}`
}

function parseDate(input: string | number | undefined | null): Date | null {
  if (input === undefined || input === null || input === '') return null
  if (typeof input === 'number') {
    const ms = input > 1e12 ? input : input * 1000
    const date = new Date(ms)
    return Number.isNaN(date.getTime()) ? null : date
  }
  const date = new Date(input)
  return Number.isNaN(date.getTime()) ? null : date
}

export function formatDateTime(input: string | number | undefined | null): string {
  const date = parseDate(input)
  if (!date) return '—'
  return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

export function relativeTime(input: string | number | undefined | null, now = Date.now()): string {
  const date = parseDate(input)
  if (!date) return '—'
  const diffMs = date.getTime() - now
  const abs = Math.abs(diffMs)
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
  const divisions: Array<[Intl.RelativeTimeFormatUnit, number]> = [
    ['year', 31_536_000_000],
    ['month', 2_592_000_000],
    ['week', 604_800_000],
    ['day', 86_400_000],
    ['hour', 3_600_000],
    ['minute', 60_000],
    ['second', 1_000],
  ]
  for (const [unit, size] of divisions) {
    if (abs >= size || unit === 'second') {
      return rtf.format(Math.round(diffMs / size), unit)
    }
  }
  return formatDateTime(date.getTime())
}

export function toDatetimeLocal(input: number | undefined): string {
  if (!input) return ''
  const date = new Date(input * 1000)
  if (Number.isNaN(date.getTime())) return ''
  const pad = (n: number) => String(n).padStart(2, '0')
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}`
  )
}

export function normalizePath(path: string): string {
  const clean: string[] = []
  for (const part of path.split('/')) {
    if (!part || part === '.') continue
    if (part === '..') {
      clean.pop()
      continue
    }
    clean.push(part)
  }
  return clean.join('/')
}

export function parentPath(path: string): string {
  const current = normalizePath(path)
  return current.split('/').slice(0, -1).join('/')
}
