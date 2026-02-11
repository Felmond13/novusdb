<template>
  <div class="flex-1 flex flex-col overflow-hidden">
    <div class="px-6 py-4 border-b border-gray-800 flex items-center justify-between">
      <div>
        <h2 class="text-lg font-semibold text-white">Export SQL</h2>
        <p class="text-xs text-gray-500">Backup complet de la base en SQL reproductible</p>
      </div>
      <div class="flex items-center gap-2">
        <button @click="reload" class="px-3 py-1.5 text-xs text-gray-400 hover:text-white border border-gray-700 rounded-lg hover:bg-gray-800 transition-colors">
          Rafraîchir
        </button>
        <button @click="copy" class="px-3 py-1.5 text-xs text-gray-400 hover:text-white border border-gray-700 rounded-lg hover:bg-gray-800 transition-colors">
          {{ copied ? 'Copié !' : 'Copier' }}
        </button>
        <button @click="download" class="px-4 py-1.5 bg-brand-600 hover:bg-brand-700 text-white text-xs font-medium rounded-lg transition-colors">
          Télécharger .sql
        </button>
      </div>
    </div>

    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <p class="text-gray-600 text-sm">Chargement...</p>
    </div>

    <div v-else class="flex-1 overflow-auto px-6 py-4">
      <pre class="bg-gray-900 border border-gray-800 rounded-lg p-4 text-xs text-gray-300 font-mono whitespace-pre-wrap">{{ dump || '-- Base vide' }}</pre>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { fetchDump } from '../api.js'

const dump = ref('')
const loading = ref(true)
const copied = ref(false)

async function reload() {
  loading.value = true
  try {
    dump.value = await fetchDump() || ''
  } catch { /* ignore */ }
  loading.value = false
}

async function copy() {
  await navigator.clipboard.writeText(dump.value)
  copied.value = true
  setTimeout(() => { copied.value = false }, 2000)
}

function download() {
  const blob = new Blob([dump.value], { type: 'text/sql' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'doclite_backup.sql'
  a.click()
  URL.revokeObjectURL(url)
}

onMounted(reload)
</script>
