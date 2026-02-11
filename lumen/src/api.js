const BASE = '/api'

export async function fetchCollections() {
  const res = await fetch(`${BASE}/collections`)
  return res.json()
}

export async function fetchViews() {
  const res = await fetch(`${BASE}/views`)
  return res.json()
}

export async function fetchSchema() {
  const res = await fetch(`${BASE}/schema`)
  return res.json()
}

export async function fetchCache() {
  const res = await fetch(`${BASE}/cache`)
  return res.json()
}

export async function fetchDump() {
  const res = await fetch(`${BASE}/dump`)
  return res.text()
}

export async function execQuery(sql) {
  const res = await fetch(`${BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql })
  })
  return res.json()
}

export async function insertJSON(collection, jsonStr) {
  const res = await fetch(`${BASE}/insert/${encodeURIComponent(collection)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: jsonStr
  })
  return res.json()
}
