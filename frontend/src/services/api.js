const API_BASE = import.meta.env.VITE_API_BASE
  || (window.location.port === '5173' ? 'http://localhost:8080/api' : '/api')

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  })
  const body = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(body.message || `HTTP ${response.status}`)
  }
  return body
}

export const api = {
  accounts: () => request('/accounts'),
  nodes: () => request('/nodes'),
  toggleNode: (id) => request(`/nodes/${id}/toggle`, { method: 'POST' }),
  stats: () => request('/stats'),
  payment: (payload) => request('/payment', { method: 'POST', body: JSON.stringify(payload) }),
  simulateRetries: (payload) => request('/simulate-retries', { method: 'POST', body: JSON.stringify(payload) }),
  benchmarkLatest: () => request('/benchmark/latest'),
  benchmarkCompare: (payload) => request('/benchmark/compare', { method: 'POST', body: JSON.stringify(payload) }),
  dedup: () => request('/deduplication-table'),
  wal: () => request('/wal-log'),
  transactions: () => request('/transactions'),
  replayWal: () => request('/recovery/replay-wal', { method: 'POST' }),
  reset: () => request('/reset', { method: 'POST' }),
}
