<script setup lang="ts">
const { selectedPath, selectedEntry, canWrite, busy, removeSelected } = useConsole()
const { ask } = useConfirm()

async function removeCurrent() {
  const entry = selectedEntry.value
  if (!entry || !canWrite.value) return
  const ok = await ask({
    title: 'Remove entry',
    body: `Permanently remove "${entry.path}"${entry.is_dir ? ' and everything inside it' : ''}?`,
    confirmLabel: 'Remove',
    danger: true,
  })
  if (ok) await removeSelected(entry)
}
</script>

<template>
  <button class="btn btn-danger btn-sm" :disabled="!selectedPath || !canWrite || busy" @click="removeCurrent">
    Remove
  </button>
</template>
