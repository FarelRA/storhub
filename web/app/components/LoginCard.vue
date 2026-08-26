<script setup lang="ts">
const console_ = useConsole()
const username = ref('')
const password = ref('')
const submitting = ref(false)

async function submit() {
  submitting.value = true
  try {
    await console_.login(username.value.trim(), password.value)
    password.value = ''
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <section class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Sign in</h2>
    <form class="flex flex-col gap-3" @submit.prevent="submit">
      <label class="block">
        <span class="field-label">Username</span>
        <input
          v-model.trim="username"
          type="text"
          autocomplete="username"
          autocapitalize="off"
          class="input"
          required
        >
      </label>
      <label class="block">
        <span class="field-label">Password</span>
        <input v-model="password" type="password" autocomplete="current-password" class="input" required >
      </label>
      <button type="submit" class="btn btn-solid" :disabled="submitting">
        {{ submitting ? 'Signing in…' : 'Sign in' }}
      </button>
    </form>
  </section>
</template>
