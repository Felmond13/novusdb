<template>
  <div class="flex-1 flex flex-col overflow-hidden">
    <div class="px-6 py-4 border-b border-gray-800">
      <h2 class="text-lg font-semibold text-white">Schéma</h2>
      <p class="text-xs text-gray-500">Structure de toutes les collections</p>
    </div>

    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <p class="text-gray-600 text-sm">Chargement...</p>
    </div>

    <div v-else class="flex-1 overflow-auto px-6 py-4 space-y-4">
      <div
        v-for="(fields, col) in schema"
        :key="col"
        class="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden"
      >
        <div class="px-4 py-3 border-b border-gray-800 flex items-center gap-3">
          <svg class="w-4 h-4 text-brand-500 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <ellipse cx="12" cy="5" rx="9" ry="3"/>
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
            <path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3"/>
          </svg>
          <h3 class="text-sm font-semibold text-white">{{ col }}</h3>
          <span class="text-xs text-gray-600">{{ Object.keys(fields).length }} champ(s)</span>
        </div>
        <table class="w-full text-sm">
          <tbody>
            <tr
              v-for="(type, field) in fields"
              :key="field"
              class="border-b border-gray-800/50 last:border-0"
            >
              <td class="px-4 py-2 text-gray-300 font-mono text-xs">{{ field }}</td>
              <td class="px-4 py-2 text-right">
                <span :class="typeColor(type)" class="text-xs font-mono px-2 py-0.5 rounded-full bg-gray-800">{{ type }}</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="!Object.keys(schema).length" class="text-center py-12">
        <p class="text-gray-600 text-sm">Aucune collection</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { fetchSchema } from '../api.js'

const schema = ref({})
const loading = ref(true)

function typeColor(t) {
  const map = {
    string: 'text-emerald-400',
    int64: 'text-amber-400',
    float64: 'text-amber-400',
    bool: 'text-purple-400',
    document: 'text-brand-400',
    array: 'text-pink-400',
  }
  return map[t] || 'text-gray-400'
}

onMounted(async () => {
  try {
    schema.value = await fetchSchema() || {}
  } catch { /* ignore */ }
  loading.value = false
})
</script>
