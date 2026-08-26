<script setup lang="ts">
const { pending, settle } = useConfirm()

const options = computed(() => pending.value)
</script>

<template>
  <ModalShell
    :open="!!options"
    :title="options?.title ?? ''"
    @close="settle(false)"
    @submit="settle(true)"
  >
    <p v-if="options" class="text-sm leading-relaxed text-mist">{{ options.body }}</p>
    <div v-if="options" class="mt-5 flex justify-end gap-2">
      <button type="button" class="btn" @click="settle(false)">Cancel</button>
      <button
        type="submit"
        class="btn"
        :class="options.danger ? 'btn-danger' : 'btn-solid'"
      >
        {{ options.confirmLabel ?? 'Confirm' }}
      </button>
    </div>
  </ModalShell>
</template>
