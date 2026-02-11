<template>
  <div class="flex-1 flex flex-col overflow-hidden">
    <div class="px-6 py-4 border-b border-gray-800 flex items-center justify-between">
      <div>
        <h2 class="text-lg font-semibold text-white">Cache LRU</h2>
        <p class="text-xs text-gray-500">Statistiques du page cache en mémoire</p>
      </div>
      <button @click="reload" class="px-3 py-1.5 text-xs text-gray-400 hover:text-white border border-gray-700 rounded-lg hover:bg-gray-800 transition-colors">
        Rafraîchir
      </button>
    </div>

    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <p class="text-gray-600 text-sm">Chargement...</p>
    </div>

    <div v-else class="flex-1 overflow-auto px-6 py-6">
      <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <div v-for="stat in stats" :key="stat.label" class="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <p class="text-xs text-gray-500 uppercase tracking-wider mb-1">{{ stat.label }}</p>
          <p class="text-2xl font-bold" :class="stat.color">{{ stat.value }}</p>
        </div>
      </div>

      <!-- Hit rate bar -->
      <div class="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <div class="flex items-center justify-between mb-3">
          <p class="text-sm font-medium text-white">Taux de succès</p>
          <p class="text-sm font-mono" :class="hitRateColor">{{ cache.hit_rate || '0%' }}</p>
        </div>
        <div class="w-full h-3 bg-gray-800 rounded-full overflow-hidden">
          <div
            class="h-full rounded-full transition-all duration-500"
            :class="hitRateBarColor"
            :style="{ width: cache.hit_rate || '0%' }"
          ></div>
        </div>
        <div class="flex justify-between mt-2 text-xs text-gray-600">
          <span>0%</span>
          <span>100%</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { fetchCache } from '../api.js'

const cache = ref({})
const loading = ref(true)

const stats = computed(() => [
  { label: 'Hits', value: cache.value.hits ?? '-', color: 'text-emerald-400' },
  { label: 'Misses', value: cache.value.misses ?? '-', color: 'text-red-400' },
  { label: 'Pages en cache', value: cache.value.size ?? '-', color: 'text-brand-400' },
  { label: 'Capacité', value: cache.value.capacity ?? '-', color: 'text-gray-300' },
])

const hitRateColor = computed(() => {
  const rate = parseFloat(cache.value.hit_rate) || 0
  if (rate >= 80) return 'text-emerald-400'
  if (rate >= 50) return 'text-amber-400'
  return 'text-red-400'
})

const hitRateBarColor = computed(() => {
  const rate = parseFloat(cache.value.hit_rate) || 0
  if (rate >= 80) return 'bg-emerald-500'
  if (rate >= 50) return 'bg-amber-500'
  return 'bg-red-500'
})

async function reload() {
  loading.value = true
  try {
    cache.value = await fetchCache() || {}
  } catch { /* ignore */ }
  loading.value = false
}

onMounted(reload)
</script>
