<script setup lang="ts">
const { revisions, canWrite, rollbackRevision } = useConsole()
const { ask } = useConfirm()

async function rollback(sha: string) {
  const ok = await ask({
    title: 'Roll back metadata',
    body: `Rewind project metadata to commit ${sha.slice(0, 10)}? Later commits remain in history.`,
    confirmLabel: 'Roll back',
    danger: true,
  })
  if (ok) await rollbackRevision(sha)
}
</script>

<template>
  <section class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Metadata revisions</h2>

    <p v-if="!revisions.length" class="text-sm text-mist">No revisions loaded.</p>

    <!-- Original list, untouched. content-visibility makes the browser skip
         layout/paint for offscreen rows, so huge histories stay cheap while
         looking and scrolling exactly like before. -->
    <ul class="flex max-h-72 flex-col gap-1.5 overflow-y-auto pr-1">
      <li
        v-for="revision in revisions"
        :key="revision.commit_sha"
        class="card [content-visibility:auto] [contain-intrinsic-size:auto_72px] px-3 py-2"
      >
        <div class="flex items-center justify-between gap-2">
          <code class="font-mono text-xs text-ember">{{ revision.commit_sha.slice(0, 10) }}</code>
          <button
            type="button"
            class="btn btn-danger btn-sm"
            :disabled="!canWrite"
            title="Roll metadata back to this revision"
            @click="rollback(revision.commit_sha)"
          >
            Roll back
          </button>
        </div>
        <p v-if="revision.message" class="mt-1 line-clamp-2 text-xs break-words text-mist">{{ revision.message }}</p>
        <p v-if="revision.committed_at" class="mt-0.5 text-xs text-mist/70" :title="String(revision.committed_at)">
          {{ relativeTime(revision.committed_at) }}
        </p>
      </li>
    </ul>
  </section>
</template>
