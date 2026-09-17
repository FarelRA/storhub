/**
 * Drag-and-drop directory traversal for the page: turn DataTransferItems
 * into {file, relPath} pairs, preserving dropped folder structure via the
 * webkit entry API. The page keeps pickFiles/onPicked/onDrop wiring only.
 */
export interface FsReader {
  readEntries(cb: (entries: FsEntry[]) => void, err?: (e: unknown) => void): void
}

export interface FsEntry {
  name: string
  isFile: boolean
  isDirectory: boolean
  file(cb: (file: File) => void, err?: (e: unknown) => void): void
  createReader(): FsReader
}

export interface DroppedFile {
  file: File
  relPath: string
}

export async function walkEntry(entry: FsEntry, prefix: string, out: DroppedFile[]): Promise<void> {
  if (entry.isFile) {
    const file = await new Promise<File>((resolve, reject) => entry.file(resolve, reject))
    out.push({ file, relPath: prefix ? `${prefix}/${entry.name}` : entry.name })
    return
  }
  const reader = entry.createReader()
  for (;;) {
    const batch: FsEntry[] = await new Promise((resolve, reject) => reader.readEntries(resolve, reject))
    if (!batch.length) break
    const nextPrefix = prefix ? `${prefix}/${entry.name}` : entry.name
    for (const child of batch) await walkEntry(child, nextPrefix, out)
  }
}

/** Collect dropped files, preferring entry traversal (keeps folders). */
export async function collectDroppedFiles(dataTransfer: DataTransfer | null): Promise<DroppedFile[]> {
  const out: DroppedFile[] = []
  if (!dataTransfer) return out
  const items = Array.from(dataTransfer.items ?? [])
  let usedEntries = false
  for (const item of items) {
    const getter = (item as DataTransferItem & { webkitGetAsEntry?: () => FsEntry | null }).webkitGetAsEntry
    const entry = typeof getter === 'function' ? getter.call(item) : null
    if (!entry) continue
    usedEntries = true
    await walkEntry(entry, '', out)
  }
  if (!usedEntries) {
    for (const file of Array.from(dataTransfer.files ?? [])) out.push({ file, relPath: file.name })
  }
  return out
}
