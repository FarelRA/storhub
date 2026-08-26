<script setup lang="ts">
const { toasts, dismiss } = useToasts()

const styles: Record<string, string> = {
  success: 'border-sage/60 text-sage',
  error: 'border-clay text-clay-soft',
  info: 'border-hair text-mist',
}
</script>

<template>
  <Teleport to="body">
    <div
      aria-live="polite"
      role="status"
      class="pointer-events-none fixed z-[60] flex flex-col gap-2 max-sm:inset-x-4 max-sm:bottom-4 sm:right-4 sm:top-16 sm:w-96"
    >
      <TransitionGroup
        enter-active-class="transition duration-200 ease-out motion-reduce:transition-none"
        enter-from-class="translate-y-2 opacity-0"
        leave-active-class="transition duration-150 ease-in motion-reduce:transition-none"
        leave-to-class="opacity-0"
      >
        <div
          v-for="toast in toasts"
          :key="toast.id"
          class="pointer-events-auto flex items-start justify-between gap-3 rounded-lg border bg-raise/95 px-3.5 py-2.5 text-sm shadow-xl backdrop-blur"
          :class="styles[toast.kind]"
        >
          <span class="min-w-0 break-words">{{ toast.text }}</span>
          <button
            type="button"
            class="-mr-1 shrink-0 rounded px-1 text-mist/70 hover:text-parchment"
            aria-label="Dismiss notification"
            @click="dismiss(toast.id)"
          >
            ✕
          </button>
        </div>
      </TransitionGroup>
    </div>
  </Teleport>
</template>
