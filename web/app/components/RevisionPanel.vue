<script setup lang="ts">
// Rollback and per-path revert are admin-only endpoints on the server
// (rest_auth.go denies non-admins), so the UI gates them with isAdmin,
// not the broader canWrite.
const { revisions, canWrite, isAdmin, rollbackRevision, revertPath, selectedPath } = useConsole()
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

async function revertSelectedPath(sha: string) {
  const path = selectedPath.value
  if (!path) return
  const ok = await ask({
    title: 'Revert this path',
    body: `Restore ${path} to its state at commit ${sha.slice(0, 10)}? Every other path is left untouched; the revert is recorded as a new commit.`,
    confirmLabel: 'Revert path',
    danger: true,
  })
  if (ok) await revertPath(path, sha)
}
</script>

<template>
  <section class="space-y-3">
    <h2 class="font-mono text-xs font-semibold tracking-wide text-mist uppercase">Metadata revisions</h2>

    <p v-if="!revisions.length" class="text-sm text-mist">No revisions loaded.</p>

    <p v-if="selectedPath" class="text-xs text-mist">
      Revert actions apply to the selected path: <code class="font-mono text-ember">{{ selectedPath }}</code>
    </p>
    <p v-else class="text-xs text-mist/70">Select a file or folder to enable per-path revert.</p>

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
          <div v-if="isAdmin" class="flex items-center gap-1.5">
            <button
              type="button"
              class="btn btn-sm"
              :disabled="!canWrite || !selectedPath"
              :title="selectedPath ? `Revert ${selectedPath} to this revision` : 'Select a path first'"
              @click="revertSelectedPath(revision.commit_sha)"
            >
              Revert path
            </button>
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
        </div>
        <p v-if="revision.message" class="mt-1 line-clamp-2 text-xs break-words text-mist">{{ revision.message }}</p>
        <p v-if="revision.committed_at" class="mt-0.5 text-xs text-mist/70" :title="formatDateTime(revision.committed_at)">
          {{ relativeTime(revision.committed_at) }}
        </p>
      </li>
    </ul>
  </section>
</template>
