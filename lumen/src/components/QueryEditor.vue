<template>
  <div class="flex-1 flex flex-col overflow-hidden">
    <!-- Header -->
    <div class="px-6 py-4 border-b border-gray-800 flex items-center justify-between">
      <h2 class="text-lg font-semibold text-white">Requêtes SQL</h2>
      <div class="flex items-center gap-2">
        <span v-if="execTime !== null" class="text-xs text-gray-500">{{ execTime }}ms</span>
        <button
          @click="run"
          :disabled="loading"
          class="px-4 py-1.5 bg-brand-600 hover:bg-brand-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-2"
        >
          <svg v-if="!loading" class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>
          <svg v-else class="w-3.5 h-3.5 animate-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v4m0 12v4m-10-10h4m12 0h4"/></svg>
          Exécuter
        </button>
      </div>
    </div>

    <!-- SQL Input -->
    <div class="px-6 py-3 border-b border-gray-800">
      <textarea
        v-model="sql"
        @keydown.ctrl.enter="run"
        placeholder="SELECT * FROM collection WHERE ..."
        rows="4"
        class="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-3 text-sm text-gray-100 font-mono placeholder-gray-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500 resize-y"
      ></textarea>
      <p class="mt-1 text-xs text-gray-600">Ctrl+Enter pour exécuter</p>
    </div>

    <!-- Error -->
    <div v-if="error" class="px-6 py-3 bg-red-950/50 border-b border-red-900/50">
      <p class="text-sm text-red-400 font-mono">{{ error }}</p>
    </div>

    <!-- Results -->
    <div class="flex-1 overflow-auto">
      <!-- Rows affected -->
      <div v-if="rowsAffected > 0 && !docs.length" class="px-6 py-4">
        <p class="text-sm text-emerald-400">{{ rowsAffected }} ligne(s) affectée(s)</p>
      </div>

      <!-- Results table -->
      <div v-if="docs.length" class="px-6 py-3">
        <div class="text-xs text-gray-500 mb-2">{{ docs.length }} document(s)</div>
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
                class="border-b border-gray-800/50 hover:bg-gray-900/50 transition-colors"
              >
                <td class="px-4 py-2 text-xs text-gray-600 font-mono">{{ i + 1 }}</td>
                <td
                  v-for="col in columns"
                  :key="col"
                  class="px-4 py-2 text-gray-300 font-mono text-xs"
                >
                  <span v-if="doc[col] === null || doc[col] === undefined" class="text-gray-600 italic">null</span>
                  <span v-else-if="typeof doc[col] === 'object'" class="text-brand-400 cursor-pointer" @click="inspect(doc[col])">{{ JSON.stringify(doc[col]).substring(0, 80) }}{{ JSON.stringify(doc[col]).length > 80 ? '...' : '' }}</span>
                  <span v-else-if="typeof doc[col] === 'boolean'" :class="doc[col] ? 'text-emerald-400' : 'text-red-400'">{{ doc[col] }}</span>
                  <span v-else-if="typeof doc[col] === 'number'" class="text-amber-400">{{ doc[col] }}</span>
                  <span v-else class="text-gray-300">{{ doc[col] }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Empty state -->
      <div v-if="!docs.length && !error && !rowsAffected && executed" class="px-6 py-12 text-center">
        <p class="text-gray-600">Aucun résultat</p>
      </div>
    </div>

    <!-- Inspector modal -->
    <div v-if="inspecting !== null" class="fixed inset-0 bg-black/60 flex items-center justify-center z-50" @click.self="inspecting = null">
      <div class="bg-gray-900 border border-gray-700 rounded-xl p-6 max-w-2xl max-h-[80vh] overflow-auto shadow-2xl">
        <div class="flex items-center justify-between mb-4">
          <h3 class="text-sm font-semibold text-white">Document</h3>
          <button @click="inspecting = null" class="text-gray-500 hover:text-white">&times;</button>
        </div>
        <pre class="text-xs text-gray-300 font-mono whitespace-pre-wrap">{{ JSON.stringify(inspecting, null, 2) }}</pre>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { execQuery } from '../api.js'

const sql = ref('')
const docs = ref([])
const error = ref('')
const rowsAffected = ref(0)
const loading = ref(false)
const execTime = ref(null)
const executed = ref(false)
const inspecting = ref(null)

const columns = computed(() => {
  if (!docs.value.length) return []
  const keys = new Set()
  for (const doc of docs.value) {
    for (const k of Object.keys(doc)) keys.add(k)
  }
  return [...keys]
})

function inspect(obj) {
  inspecting.value = obj
}

async function run() {
  if (!sql.value.trim()) return
  loading.value = true
  error.value = ''
  docs.value = []
  rowsAffected.value = 0
  executed.value = false
  const t0 = performance.now()
  try {
    const res = await execQuery(sql.value.trim())
    execTime.value = Math.round(performance.now() - t0)
    if (res.error) {
      error.value = res.error
    } else {
      docs.value = res.docs || []
      rowsAffected.value = res.rows_affected || 0
    }
    executed.value = true
  } catch (e) {
    error.value = 'Connexion impossible au serveur DocLite'
    execTime.value = null
  } finally {
    loading.value = false
  }
}
</script>
