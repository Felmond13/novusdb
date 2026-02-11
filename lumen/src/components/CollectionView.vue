<template>
  <div class="flex-1 flex flex-col overflow-hidden">
    <!-- Header -->
    <div class="px-6 py-4 border-b border-gray-800 flex items-center justify-between">
      <div>
        <h2 class="text-lg font-semibold text-white">{{ collection }}</h2>
        <p class="text-xs text-gray-500">{{ docs.length }} document(s)</p>
      </div>
      <div class="flex items-center gap-2">
        <button
          @click="viewMode = viewMode === 'table' ? 'json' : 'table'"
          class="px-3 py-1.5 text-xs text-gray-400 hover:text-white border border-gray-700 rounded-lg hover:bg-gray-800 transition-colors"
        >
          {{ viewMode === 'table' ? 'Vue JSON' : 'Vue Table' }}
        </button>
        <button
          @click="reload"
          :disabled="loading"
          class="px-3 py-1.5 text-xs text-gray-400 hover:text-white border border-gray-700 rounded-lg hover:bg-gray-800 transition-colors"
        >
          Rafraîchir
        </button>
      </div>
    </div>

    <!-- Loading -->
    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <div class="text-gray-600 text-sm">Chargement...</div>
    </div>

    <!-- Error -->
    <div v-else-if="error" class="px-6 py-4">
      <p class="text-sm text-red-400">{{ error }}</p>
    </div>

    <!-- Table View -->
    <div v-else-if="viewMode === 'table' && docs.length" class="flex-1 overflow-auto px-6 py-3">
      <div class="overflow-x-auto rounded-lg border border-gray-800">
        <table class="w-full text-sm">
          <thead>
            <tr class="bg-gray-900">
              <th class="px-4 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider border-b border-gray-800">#</th>
              <th
                v-for="col in columns"
                :key="col"
                class="px-4 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider border-b border-gray-800"
              >{{ col }}</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="(doc, i) in docs"
              :key="i"
              class="border-b border-gray-800/50 hover:bg-gray-900/50 transition-colors cursor-pointer"
              @click="selected = doc"
            >
              <td class="px-4 py-2 text-xs text-gray-600 font-mono">{{ i + 1 }}</td>
              <td
                v-for="col in columns"
                :key="col"
                class="px-4 py-2 font-mono text-xs max-w-xs truncate"
              >
                <span v-if="doc[col] === null || doc[col] === undefined" class="text-gray-600 italic">null</span>
                <span v-else-if="typeof doc[col] === 'object'" class="text-brand-400">{{ shortJSON(doc[col]) }}</span>
                <span v-else-if="typeof doc[col] === 'boolean'" :class="doc[col] ? 'text-emerald-400' : 'text-red-400'">{{ doc[col] }}</span>
                <span v-else-if="typeof doc[col] === 'number'" class="text-amber-400">{{ doc[col] }}</span>
                <span v-else class="text-gray-300">{{ doc[col] }}</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- JSON View -->
    <div v-else-if="viewMode === 'json' && docs.length" class="flex-1 overflow-auto px-6 py-3 space-y-3">
      <div
        v-for="(doc, i) in docs"
        :key="i"
        class="bg-gray-900 border border-gray-800 rounded-lg p-4 cursor-pointer hover:border-gray-700 transition-colors"
        @click="selected = doc"
      >
        <div class="flex items-center justify-between mb-2">
          <span class="text-xs text-gray-500 font-mono">#{{ i + 1 }}</span>
        </div>
        <pre class="text-xs text-gray-300 font-mono whitespace-pre-wrap">{{ JSON.stringify(doc, null, 2) }}</pre>
      </div>
    </div>

    <!-- Empty state -->
    <div v-else-if="!loading" class="flex-1 flex items-center justify-center">
      <div class="text-center">
        <svg class="w-12 h-12 mx-auto text-gray-800 mb-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <ellipse cx="12" cy="5" rx="9" ry="3"/>
          <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
          <path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3"/>
        </svg>
        <p class="text-gray-600 text-sm">Collection vide</p>
      </div>
    </div>

    <!-- Detail modal -->
    <div v-if="selected" class="fixed inset-0 bg-black/60 flex items-center justify-center z-50" @click.self="selected = null">
      <div class="bg-gray-900 border border-gray-700 rounded-xl p-6 max-w-3xl w-full max-h-[80vh] overflow-auto shadow-2xl mx-4">
        <div class="flex items-center justify-between mb-4">
          <h3 class="text-sm font-semibold text-white">Document</h3>
          <button @click="selected = null" class="text-gray-500 hover:text-white text-lg">&times;</button>
        </div>
        <pre class="text-xs text-gray-300 font-mono whitespace-pre-wrap">{{ JSON.stringify(selected, null, 2) }}</pre>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { execQuery } from '../api.js'

const props = defineProps({ collection: String })

const docs = ref([])
const error = ref('')
const loading = ref(false)
const viewMode = ref('table')
const selected = ref(null)

const columns = computed(() => {
  if (!docs.value.length) return []
  const keys = new Set()
  for (const doc of docs.value) {
    for (const k of Object.keys(doc)) keys.add(k)
  }
  return [...keys]
})

function shortJSON(obj) {
  const s = JSON.stringify(obj)
  return s.length > 60 ? s.substring(0, 60) + '...' : s
}

async function reload() {
  if (!props.collection) return
  loading.value = true
  error.value = ''
  try {
    const res = await execQuery(`SELECT * FROM ${props.collection}`)
    if (res.error) {
      error.value = res.error
    } else {
      docs.value = res.docs || []
    }
  } catch {
    error.value = 'Connexion impossible'
  } finally {
    loading.value = false
  }
}

watch(() => props.collection, () => {
  selected.value = null
  reload()
}, { immediate: true })
</script>
