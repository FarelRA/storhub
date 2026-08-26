import tailwindcss from '@tailwindcss/vite'

export default defineNuxtConfig({
  compatibilityDate: '2025-07-15',
  ssr: false,
  spaLoadingTemplate: false,
  modules: ['@nuxt/eslint'],
  css: ['~/assets/css/main.css'],
  vite: {
    plugins: [tailwindcss()],
    server: {
      // Dev-only: proxy API calls to a local storhub REST server.
      proxy: {
        '/api': { target: 'http://127.0.0.1:8080', changeOrigin: true },
      },
    },
  },
  app: {
    head: {
      title: 'StorHub Console',
      htmlAttrs: { lang: 'en', class: 'dark' },
      meta: [
        { charset: 'utf-8' },
        { name: 'viewport', content: 'width=device-width, initial-scale=1, viewport-fit=cover' },
        { name: 'theme-color', content: '#161311' },
        { name: 'description', content: 'Browser console for the StorHub REST API.' },
      ],
      link: [{ rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }],
      script: [
        // Runtime configuration injected by the Go server (basePath, authEnabled).
        // Plain <head> scripts execute before the SPA bundle boots.
        { src: '/config.js', tagPosition: 'head' },
      ],
    },
  },
})
