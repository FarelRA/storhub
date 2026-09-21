#!/usr/bin/env node
// Copies `nuxt generate` output (.output/public) into the Go embed directory.
// internal/rest/static/dist is git-ignored: release builds regenerate it from
// source with a pinned bun, while Go-only checkouts carry only the committed
// placeholder.txt (which keeps `go:embed` compiling) and serve ui_not_built
// until a real bundle is built via `bun run build:embed`. This script replaces
// the placeholder with the real bundle.
import { cpSync, existsSync, rmSync, readdirSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const source = join(root, '.output', 'public')
const target = resolve(root, '..', 'internal', 'rest', 'static', 'dist')

if (!existsSync(source) || readdirSync(source).length === 0) {
  console.error(`build:embed: ${source} is empty - run \`nuxt generate\` first`)
  process.exit(1)
}

rmSync(target, { recursive: true, force: true })
cpSync(source, target, { recursive: true })
console.log(`build:embed: copied bundle to ${target}`)
