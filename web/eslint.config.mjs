// @ts-check
import withNuxt from './.nuxt/eslint.config.mjs'

export default withNuxt(
  {
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      'vue/max-attributes-per-line': 'off',
      'vue/singleline-html-element-content-newline': 'off',
    },
  },
  {
    // Auto-import everywhere: display/format helpers come from Nuxt
    // auto-import (~/composables/use-format), never from manual imports in
    // SFCs. Two import styles for the same helper caused drift before.
    files: ['app/components/**/*.vue', 'app/pages/**/*.vue'],
    rules: {
      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: ['~/composables/use-format', '@/composables/use-format'],
              message: 'Use the Nuxt auto-imported helper instead of importing use-format manually.',
            },
          ],
        },
      ],
    },
  },
  {
    ignores: ['dist/**', '.output/**', '.nuxt/**'],
  },
)
