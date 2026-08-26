<script setup lang="ts">
const console_ = useConsole()
const { xattrs, selectedPath, canWrite } = console_
const toasts = useToasts()
const { ask } = useConfirm()

async function copyValue(value: string) {
  try {
    await navigator.clipboard.writeText(value)
    toasts.success('Value copied')
  } catch {
    toasts.error('Clipboard unavailable')
  }
}

async function removeFirst() {
  const name = xattrs.value[0]?.name
  if (!name) return
  const ok = await ask({
    title: 'Remove xattr',
    body: `Remove extended attribute "${name}"?`,
    confirmLabel: 'Remove',
    danger: true,
  })
  if (ok && selectedPath.value) await console_.removeXattr(selectedPath.value, name)
}
</script>

<template>
  <section class="space-y-3">
    <div class="flex items-center justify-between gap-2">
      <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Extended attributes</h2>
      <button class="btn btn-sm" :disabled="!selectedPath" @click="console_.loadXattrs()">Reload</button>
    </div>

    <p v-if="!xattrs.length" class="text-sm text-mist">
      {{ selectedPath ? 'No attributes on this entry.' : 'Select an entry first.' }}
    </p>

    <ul v-else class="flex max-h-56 flex-col gap-1.5 overflow-y-auto pr-1">
      <li v-for="item in xattrs" :key="item.name">
        <button
          type="button"
          class="card w-full px-3 py-2 text-left hover:border-ember"
          title="Click to copy value"
          @click="copyValue(item.value)"
        >
          <span class="block truncate font-mono text-xs font-medium">{{ item.name }}</span>
          <span class="mt-0.5 block truncate font-mono text-xs text-mist">{{ item.value || '(empty)' }}</span>
        </button>
      </li>
    </ul>

    <div class="flex flex-wrap gap-2">
      <button class="btn btn-sm" :disabled="!selectedPath || !canWrite" @click="console_.openModal('xattr-set')">
        Set
      </button>
      <button class="btn btn-danger btn-sm" :disabled="!xattrs.length || !canWrite" @click="removeFirst">
        Remove first…
      </button>
    </div>
  </section>
</template>
