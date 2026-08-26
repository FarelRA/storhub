<script setup lang="ts">
const props = defineProps<{
  project: string
  path: string
}>()

const emit = defineEmits<{ navigate: [path: string] }>()

const editing = ref(false)
const draft = ref('/')
const inputEl = ref<HTMLInputElement | null>(null)

function startEdit() {
  draft.value = `/${props.path}`
  editing.value = true
  void nextTick(() => {
    inputEl.value?.focus()
    inputEl.value?.select()
  })
}

function cancelEdit() {
  editing.value = false
}

function commitEdit() {
  editing.value = false
  const raw = draft.value.trim()
  if (!raw || raw === '/') {
    emit('navigate', '')
    return
  }
  emit('navigate', normalizePath(raw))
}

const segments = computed(() => {
  const parts = props.path ? props.path.split('/') : []
  return parts.map((name, index) => ({
    name,
    target: parts.slice(0, index + 1).join('/'),
  }))
})
</script>

<template>
  <nav aria-label="Location" class="flex min-w-0 flex-1 items-center gap-1">
    <!-- Breadcrumb view -->
    <div v-if="!editing" class="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto whitespace-nowrap font-mono text-sm">
      <button
        type="button"
        class="rounded px-1 py-0.5 text-mist hover:text-ember"
        :disabled="!project"
        @click="emit('navigate', '')"
      >
        {{ project || 'no project' }}
      </button>
      <span aria-hidden="true" class="text-hair">/</span>
      <template v-for="(segment, index) in segments" :key="segment.target">
        <span v-if="index > 0" aria-hidden="true" class="text-hair">/</span>
        <button
          v-if="index < segments.length - 1"
          type="button"
          class="rounded px-1 py-0.5 text-mist hover:text-ember"
          @click="emit('navigate', segment.target)"
        >
          {{ segment.name }}
        </button>
        <span v-else aria-current="page" class="px-1 py-0.5 font-medium text-parchment">
          {{ segment.name }}
        </span>
      </template>

      <button
        type="button"
        class="ml-auto shrink-0 rounded px-1.5 py-0.5 text-mist hover:text-ember"
        :disabled="!project"
        title="Type a path"
        aria-label="Edit path"
        @click="startEdit"
      >
        ✎
      </button>
    </div>

    <!-- Typed-path edit -->
    <div v-else class="flex min-w-0 flex-1 items-center gap-1">
      <input
        ref="inputEl"
        v-model="draft"
        type="text"
        class="input min-w-0 flex-1 px-2 py-1 font-mono text-sm"
        placeholder="/folder/subdir"
        aria-label="Path"
        @keydown.enter.prevent="commitEdit"
        @keydown.esc.prevent="cancelEdit"
      >
      <button type="button" class="btn btn-sm shrink-0" @click="commitEdit">Go</button>
      <button type="button" class="btn btn-sm shrink-0" @click="cancelEdit">✕</button>
    </div>
  </nav>
</template>
