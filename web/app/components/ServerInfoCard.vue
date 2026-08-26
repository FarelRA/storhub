<script setup lang="ts">
/**
 * Self-describing server: renders GET {basePath} (service/version/base path)
 * so the console documents the very API it drives. Fetched once, lazily,
 * on first project load; hidden entirely if the probe fails.
 */
const console_ = useConsole()

interface ApiInfo {
  service?: string
  version?: string
  base_path?: string
}

const info = ref<ApiInfo | null>(null)
const loaded = ref(false)

async function load() {
  if (loaded.value || !console_.project.value) return
  const { getJSON } = useApi()
  try {
    info.value = await getJSON<ApiInfo>('/')
  } catch {
    return // quietly: an informational card must never nag
  } finally {
    loaded.value = true
  }
}

watch(() => console_.project.value, load, { immediate: true })
</script>

<template>
  <section v-if="info" class="space-y-1.5 pb-5 text-xs text-mist">
    <h2 class="font-mono text-xs font-semibold tracking-wide uppercase">Server</h2>
    <p><span class="text-mist/70">service</span> <code class="font-mono">{{ info.service ?? '?' }}</code></p>
    <p><span class="text-mist/70">api</span> <code class="font-mono">{{ info.base_path }}</code> <span v-if="info.version" class="chip ml-1">{{ info.version }}</span></p>
    <p class="leading-relaxed text-mist/70">
      Every action in this console is a plain call to that API - anything you can click here,
      you can script with curl.
    </p>
  </section>
</template>
