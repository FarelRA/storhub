#!/usr/bin/env node
// Copies `nuxt generate` output (.output/public) into the Go embed directory.
// The placeholder index.html committed there keeps `go build/test` working on
// machines without bun; this script replaces it with the real bundle.
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
