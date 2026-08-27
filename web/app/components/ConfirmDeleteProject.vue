<script setup lang="ts">
const { project, isAdmin, busy, deleteProject } = useConsole()
const { ask } = useConfirm()

const emit = defineEmits<{ deleted: [] }>()

async function confirmDelete() {
  const name = project.value
  const ok = await ask({
    title: 'Delete project',
    body: `Type nothing twice - this deletes the GitHub repository backing "${name}" along with every file, revision, and share. This cannot be undone.`,
    confirmLabel: `Delete ${name}`,
    danger: true,
  })
  if (ok && (await deleteProject())) emit('deleted')
}
</script>

<template>
  <button v-if="isAdmin" class="btn btn-danger btn-sm w-full" :disabled="!project || busy" @click="confirmDelete">
    Delete project…
  </button>
</template>
