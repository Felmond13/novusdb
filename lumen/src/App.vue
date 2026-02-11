<template>
  <div class="flex h-screen overflow-hidden">
    <!-- Sidebar -->
    <aside class="w-64 flex-shrink-0 bg-gray-900 border-r border-gray-800 flex flex-col">
      <!-- Logo -->
      <div class="px-5 py-4 border-b border-gray-800">
        <div class="flex items-center gap-3">
          <div class="w-8 h-8 rounded-lg bg-brand-600 flex items-center justify-center">
            <svg class="w-4 h-4 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
              <circle cx="12" cy="12" r="3"/>
              <path d="M12 2v4m0 12v4m-10-10h4m12 0h4m-3.5-6.5l-2.8 2.8m-5.4 5.4l-2.8 2.8m0-11l2.8 2.8m5.4 5.4l2.8 2.8"/>
            </svg>
          </div>
          <div>
            <h1 class="text-sm font-bold text-white tracking-wide">Lumen</h1>
            <p class="text-[10px] text-gray-500 uppercase tracking-widest">NovusDB Studio</p>
          </div>
        </div>
      </div>

      <!-- Navigation -->
      <nav class="flex-1 overflow-y-auto py-3 px-3 space-y-1">
        <button
          v-for="tab in tabs"
          :key="tab.id"
          @click="activeTab = tab.id"
          :class="[
            'w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors',
            activeTab === tab.id
              ? 'bg-brand-600/20 text-brand-400'
              : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'
          ]"
        >
          <span v-html="tab.icon" class="w-4 h-4 flex-shrink-0"></span>
          {{ tab.label }}
        </button>

        <!-- Collections -->
        <div class="pt-4 pb-1 px-3">
          <p class="text-[10px] font-semibold text-gray-600 uppercase tracking-widest">Collections</p>
        </div>
        <button
          v-for="col in collections"
          :key="col"
          @click="selectCollection(col)"
          :class="[
            'w-full flex items-center gap-3 px-3 py-1.5 rounded-lg text-sm transition-colors',
            activeTab === 'collection' && selectedCollection === col
              ? 'bg-brand-600/20 text-brand-400'
              : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800'
          ]"
        >
          <svg class="w-3.5 h-3.5 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <ellipse cx="12" cy="5" rx="9" ry="3"/>
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
            <path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3"/>
          </svg>
          {{ col }}
        </button>

        <!-- Views -->
        <div v-if="views.length" class="pt-4 pb-1 px-3">
          <p class="text-[10px] font-semibold text-gray-600 uppercase tracking-widest">Vues</p>
        </div>
        <button
          v-for="v in views"
          :key="v"
          @click="selectView(v)"
          :class="[
            'w-full flex items-center gap-3 px-3 py-1.5 rounded-lg text-sm transition-colors',
            activeTab === 'collection' && selectedCollection === v
              ? 'bg-brand-600/20 text-brand-400'
              : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800'
          ]"
        >
          <svg class="w-3.5 h-3.5 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
            <circle cx="12" cy="12" r="3"/>
          </svg>
          {{ v }}
        </button>
      </nav>

      <!-- Status -->
      <div class="px-4 py-3 border-t border-gray-800 text-xs text-gray-600">
        <div class="flex items-center gap-2">
          <span :class="connected ? 'text-emerald-500' : 'text-red-500'">●</span>
          {{ connected ? 'Connecté' : 'Déconnecté' }}
        </div>
      </div>
    </aside>

    <!-- Main Content -->
    <main class="flex-1 overflow-hidden flex flex-col">
      <!-- Query Editor -->
      <QueryEditor v-if="activeTab === 'query'" />

      <!-- Collection View -->
      <CollectionView
        v-if="activeTab === 'collection'"
        :collection="selectedCollection"
      />

      <!-- Schema -->
      <SchemaView v-if="activeTab === 'schema'" />

      <!-- Cache Stats -->
      <CacheView v-if="activeTab === 'cache'" />

      <!-- Dump -->
      <DumpView v-if="activeTab === 'dump'" />
    </main>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { fetchCollections, fetchViews } from './api.js'
import QueryEditor from './components/QueryEditor.vue'
import CollectionView from './components/CollectionView.vue'
import SchemaView from './components/SchemaView.vue'
import CacheView from './components/CacheView.vue'
import DumpView from './components/DumpView.vue'

const activeTab = ref('query')
const collections = ref([])
const views = ref([])
const selectedCollection = ref('')
const connected = ref(false)

const tabs = [
  { id: 'query', label: 'Requêtes', icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>' },
  { id: 'schema', label: 'Schéma', icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>' },
  { id: 'cache', label: 'Cache', icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>' },
  { id: 'dump', label: 'Export SQL', icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' },
]

function selectCollection(col) {
  selectedCollection.value = col
  activeTab.value = 'collection'
}

function selectView(v) {
  selectedCollection.value = v
  activeTab.value = 'collection'
}

async function refresh() {
  try {
    const [cols, vws] = await Promise.all([fetchCollections(), fetchViews()])
    collections.value = cols || []
    views.value = vws || []
    connected.value = true
  } catch {
    connected.value = false
  }
}

onMounted(() => {
  refresh()
  setInterval(refresh, 5000)
})
</script>
