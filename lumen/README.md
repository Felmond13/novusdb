# Lumen — NovusDB Studio

Web admin interface for **NovusDB**, built with Vue 3 + Vite + Tailwind CSS.

![Vue 3](https://img.shields.io/badge/Vue-3-4FC08D?logo=vue.js)
![Tailwind CSS](https://img.shields.io/badge/Tailwind-3-38BDF8?logo=tailwindcss)

---

## Features

- **SQL Editor**: execute queries with syntax-highlighted results (Ctrl+Enter)
- **Collection Browser**: table or JSON view, click to inspect a document
- **Schema**: structure of all collections with color-coded types
- **LRU Cache**: real-time statistics (hits, misses, hit rate)
- **SQL Export**: full `.dump` with copy and `.sql` download
- **Sidebar**: collections and views listed dynamically
- Native **dark mode**

---

## Getting Started

### 1. Start the NovusDB server

```bash
cd ..
./NovusDB-server -db ma_base.db -addr :8080
```

### 2. Start Lumen

```bash
cd lumen
npm install
npm run dev
```

Open **http://localhost:5173**

> The Vite proxy redirects `/api/*` to `http://localhost:8080` automatically.

---

## Production Build

```bash
npm run build
```

Static files are generated in `dist/`.

---

## Architecture

```
lumen/
├── src/
│   ├── api.js              # HTTP client (fetch → NovusDB server)
│   ├── App.vue             # Shell: sidebar + routing
│   ├── main.js             # Entry point
│   ├── style.css           # Tailwind imports
│   └── components/
│       ├── QueryEditor.vue  # SQL editor + results
│       ├── CollectionView.vue # Document browser
│       ├── SchemaView.vue   # Collection structure
│       ├── CacheView.vue    # LRU cache stats
│       └── DumpView.vue     # SQL export
├── package.json
├── vite.config.js
├── tailwind.config.js
└── postcss.config.js
```
