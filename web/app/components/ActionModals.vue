<script setup lang="ts">
import type { ModalKind } from '~/composables/use-console'

const console_ = useConsole()
const { modalOpen, modalKind, modalForm: form, modalError } = console_

const isPathKind = computed(() =>
  ['mkdir', 'create-file'].includes(modalKind.value),
)
const isNewPathKind = computed(() => ['rename', 'move', 'copy', 'link', 'symlink'].includes(modalKind.value))
const isMetaKind = computed(() => ['chmod', 'chown', 'utimes', 'xattr-set', 'xattr-remove'].includes(modalKind.value))
const isTextOpKind = computed(() => ['append', 'patch', 'truncate'].includes(modalKind.value))

const submitLabel = computed(() => {
  const map: Partial<Record<ModalKind, string>> = {
    mkdir: 'Create',
    'create-file': 'Create',
    rename: 'Rename',
    move: 'Move',
    copy: 'Copy',
    link: 'Link',
    symlink: 'Link',
    append: 'Append',
    patch: 'Apply patch',
    truncate: 'Truncate',
  }
  return map[modalKind.value] ?? 'Apply'
})

const isMoveOrCopy = computed(() => ['move', 'copy'].includes(modalKind.value))
const bulkCount = computed(() => console_.selectedPaths.value.size)
</script>

<template>
  <ModalShell
    :open="modalOpen"
    :title="modalOpen ? console_.modalTitle(modalKind) : ''"
    @close="console_.closeModal()"
    @submit="console_.submitModal()"
  >
    <p
      v-if="modalError"
      class="mb-3 rounded-lg border border-clay bg-clay/15 px-3 py-2 text-sm text-clay-soft"
      role="alert"
    >
      {{ modalError }}
    </p>

    <div v-if="isPathKind" class="flex flex-col gap-3">
      <label class="block">
        <span class="field-label">Path</span>
        <input v-model="form.path" type="text" class="input font-mono" placeholder="docs/readme.md" required >
      </label>
    </div>

    <div v-else-if="isNewPathKind" class="flex flex-col gap-3">
      <template v-if="isMoveOrCopy && bulkCount > 1">
        <p class="text-sm text-mist">{{ modalKind === 'move' ? 'Move' : 'Copy' }} {{ bulkCount }} items to:</p>
        <ul class="max-h-24 overflow-y-auto rounded border border-hair bg-surface px-2 py-1 font-mono text-xs">
          <li v-for="p in [...console_.selectedPaths.value]" :key="p" class="truncate">{{ p }}</li>
        </ul>
        <label class="block">
          <span class="field-label">Destination directory</span>
          <input v-model="form.newPath" type="text" class="input font-mono" placeholder="target/folder" required >
        </label>
      </template>
      <template v-else>
        <label v-if="modalKind !== 'symlink'" class="block">
          <span class="field-label">Existing path</span>
          <input :value="modalKind === 'rename' ? form.path : console_.selectedPath.value" type="text" class="input font-mono opacity-60" disabled >
        </label>
        <label class="block">
          <span class="field-label">{{ modalKind === 'symlink' ? 'Link path' : modalKind === 'move' ? 'Destination' : modalKind === 'copy' ? 'Destination' : 'New path' }}</span>
          <input v-model="form.newPath" type="text" class="input font-mono" required >
        </label>
        <label v-if="modalKind === 'symlink'" class="block">
          <span class="field-label">Target</span>
          <input v-model="form.target" type="text" class="input font-mono" placeholder="../other/file.txt" required >
        </label>
      </template>
    </div>

    <div v-else-if="isMetaKind" class="flex flex-col gap-3">
      <label v-if="modalKind === 'chmod'" class="block">
        <span class="field-label">Mode (octal)</span>
        <input v-model="form.mode" type="text" inputmode="numeric" pattern="[0-7]{3,4}" class="input font-mono" placeholder="0644" required >
      </label>
      <template v-if="modalKind === 'chown'">
        <label class="block">
          <span class="field-label">UID</span>
          <input v-model.number="form.uid" type="number" min="0" class="input font-mono" >
        </label>
        <label class="block">
          <span class="field-label">GID</span>
          <input v-model.number="form.gid" type="number" min="0" class="input font-mono" >
        </label>
      </template>
      <template v-if="modalKind === 'utimes'">
        <label class="block">
          <span class="field-label">Accessed at</span>
          <input v-model="form.atime" type="datetime-local" class="input" >
        </label>
        <label class="block">
          <span class="field-label">Modified at</span>
          <input v-model="form.mtime" type="datetime-local" class="input" >
        </label>
      </template>
      <template v-if="modalKind === 'xattr-set' || modalKind === 'xattr-remove'">
        <label class="block">
          <span class="field-label">Attribute name</span>
          <input v-model="form.name" type="text" class="input font-mono" placeholder="user.checksum" required >
        </label>
        <label v-if="modalKind === 'xattr-set'" class="block">
          <span class="field-label">Value</span>
          <textarea v-model="form.value" rows="3" class="input font-mono resize-y" />
        </label>
      </template>
    </div>

    <div v-else-if="isTextOpKind" class="flex flex-col gap-3">
      <p class="font-mono text-xs text-mist">{{ console_.selectedPath.value }}</p>
      <div v-if="modalKind === 'patch'" class="grid grid-cols-2 gap-3">
        <label class="block">
          <span class="field-label">Offset (bytes)</span>
          <input v-model.number="form.offset" type="number" min="0" step="1" class="input font-mono" >
        </label>
        <label class="block">
          <span class="field-label">Delete size (bytes)</span>
          <input v-model.number="form.deleteSize" type="number" min="0" step="1" class="input font-mono" >
        </label>
      </div>
      <label v-if="modalKind !== 'truncate'" class="block">
        <span class="field-label">{{ modalKind === 'append' ? 'Text to append' : 'Replacement bytes' }}</span>
        <textarea v-model="form.text" rows="6" class="input font-mono resize-y" />
      </label>
      <label v-else class="block">
        <span class="field-label">New file size in bytes</span>
        <input v-model="form.text" type="number" min="0" step="1" class="input font-mono" placeholder="0" required >
      </label>
    </div>

    <div class="mt-5 flex justify-end gap-2">
      <button type="button" class="btn" @click="console_.closeModal()">Cancel</button>
      <button type="submit" class="btn btn-solid">{{ submitLabel }}</button>
    </div>
  </ModalShell>
</template>
