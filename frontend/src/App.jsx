import { useEffect, useRef, useState } from 'react'
import { api } from './services/api'
import StatsGrid from './components/StatsGrid'
import NodePanel from './components/NodePanel'
import PaymentConsole from './components/PaymentConsole'
import BenchmarkPanel from './components/BenchmarkPanel'
import DataTable from './components/DataTable'
import Toast from './components/Toast'

export default function App() {
  const [stats, setStats] = useState(null)
  const [nodes, setNodes] = useState([])
  const [accounts, setAccounts] = useState([])
  const [dedup, setDedup] = useState([])
  const [wal, setWal] = useState([])
  const [transactions, setTransactions] = useState([])
  const [benchmark, setBenchmark] = useState({ runs: [], windows: [], checkpoints: [] })
  const [benchmarkRunning, setBenchmarkRunning] = useState(false)
  const [benchmarkProgress, setBenchmarkProgress] = useState(null)
  const [toast, setToast] = useState(null)
  const benchmarkStreamRef = useRef(null)
  const benchmarkRunningRef = useRef(false)

  const showToast = (message, type = 'success') => {
    setToast({ message, type })
    window.setTimeout(() => setToast(null), 3000)
  }

  async function refreshAll() {
    const [statsData, nodeData, accountData, dedupData, walData, txnData, benchmarkData] = await Promise.all([
      api.stats(),
      api.nodes(),
      api.accounts(),
      api.dedup(),
      api.wal(),
      api.transactions(),
      api.benchmarkLatest(),
    ])
    setStats(statsData)
    setNodes(nodeData)
    setAccounts(accountData)
    setDedup(dedupData)
    setWal(walData)
    setTransactions(txnData)
    if (!benchmarkRunningRef.current) setBenchmark(benchmarkData)
  }

  useEffect(() => {
    refreshAll().catch((error) => showToast(error.message, 'error'))
    const timer = window.setInterval(() => refreshAll().catch(() => {}), 10000)
    return () => {
      window.clearInterval(timer)
      benchmarkStreamRef.current?.close()
    }
  }, [])

  function setBenchmarkRunningState(value) {
    benchmarkRunningRef.current = value
    setBenchmarkRunning(value)
  }

  function closeBenchmarkStream() {
    benchmarkStreamRef.current?.close()
    benchmarkStreamRef.current = null
  }

  function stopBenchmarkStream(source) {
    if (benchmarkStreamRef.current !== source) return false
    setBenchmarkProgress(null)
    setBenchmarkRunningState(false)
    closeBenchmarkStream()
    return true
  }

  function applyBenchmarkProgress(progressEvent) {
    setBenchmarkProgress(progressEvent)
    if (!progressEvent.window) return
    setBenchmark((current) => ({
      ...current,
      windows: upsertWindow(current.windows || [], progressEvent.window),
    }))
  }

  async function completeBenchmarkStream(source, payload) {
    if (!stopBenchmarkStream(source)) return
    setBenchmark(payload)
    await refreshAll()
    showToast('Live benchmark complete', 'success')
  }

  function failBenchmarkStream(source, message) {
    if (!stopBenchmarkStream(source)) return
    showToast(message, 'error')
    refreshAll().catch(() => {})
  }

  function attachBenchmarkStreamHandlers(source, payload) {
    source.addEventListener('started', (event) => {
      const data = parseSse(event)
      setBenchmark({ runs: [], windows: [], checkpoints: [], dataset: data.dataset })
      setBenchmarkProgress({ mode: 'ALO', eventIndex: 0, totalEvents: data.dataset?.eventCount || payload.eventCount })
    })

    source.addEventListener('progress', (event) => {
      applyBenchmarkProgress(parseSse(event))
    })

    source.addEventListener('run', (event) => {
      const data = parseSse(event)
      setBenchmark((current) => ({
        ...current,
        runs: upsertRun(current.runs || [], data.run),
        windows: mergeWindows(current.windows || [], data.windows || []),
      }))
    })

    source.addEventListener('complete', (event) => {
      completeBenchmarkStream(source, parseSse(event)).catch((error) => showToast(error.message, 'error'))
    })

    source.addEventListener('error', (event) => {
      const data = parseSse(event)
      failBenchmarkStream(source, data.message || 'Benchmark stream failed')
    })

    source.onerror = () => {
      failBenchmarkStream(source, 'Benchmark stream disconnected')
    }
  }

  async function handleRunBenchmark(payload) {
    closeBenchmarkStream()
    setBenchmarkRunningState(true)
    setBenchmarkProgress({ mode: 'ALO', eventIndex: 0, totalEvents: payload.eventCount })
    setBenchmark({ runs: [], windows: [], checkpoints: [], dataset: payload })
    showToast('Streaming EOS vs ALO benchmark...', 'warning')

    const source = new EventSource(api.benchmarkStreamUrl({ ...payload, streamDelayMs: 25 }))
    benchmarkStreamRef.current = source
    attachBenchmarkStreamHandlers(source, payload)
  }

  async function handleReset() {
    await api.reset()
    showToast('Database reset', 'warning')
    await refreshAll()
  }

  async function handleReplayWal() {
    const result = await api.replayWal()
    showToast(`WAL replay: ${result.pendingFound} pending`, 'success')
    await refreshAll()
  }

  return (
    <>
      <header>
        <div className="logo">
          <div className="logo-icon">EOS</div>
          <div>
            <h1>EOS <span>Payment</span> System</h1>
            <p>Spring Boot · React · Exactly-Once Semantics · Topic 117</p>
          </div>
        </div>
        <div className="header-badges">
          <span className="badge badge-cyan">EOS Enabled</span>
          <span className="badge badge-purple">WAL Recovery</span>
        </div>
      </header>

      <main className="container">
        <StatsGrid stats={stats} />
        <div className="main-grid">
          <aside>
            <NodePanel nodes={nodes} onToggle={async (id) => {
              try {
                await api.toggleNode(id)
                showToast(`${id} toggled`, 'warning')
              } catch (error) {
                showToast(error.message, 'error')
              }
              await refreshAll()
            }} />
            <PaymentConsole
              accounts={accounts}
              onSubmit={async (payload) => {
                try {
                  const result = await api.payment(payload)
                  showToast(result.status === 'SUCCESS' ? 'Payment processed' : result.status, result.status === 'SUCCESS' ? 'success' : 'warning')
                  await refreshAll()
                  return result
                } catch (error) {
                  showToast(error.message, 'error')
                  return { status: 'ERROR', message: error.message }
                }
              }}
              onRetry={async (payload) => {
                try {
                  const result = await api.simulateRetries(payload)
                  showToast('Retry simulation complete', 'success')
                  await refreshAll()
                  return result
                } catch (error) {
                  showToast(error.message, 'error')
                  return { status: 'ERROR', message: error.message }
                }
              }}
              onReset={handleReset}
            />
          </aside>

          <section className="right-panel">
            <BenchmarkPanel
              payload={benchmark}
              isRunning={benchmarkRunning}
              progress={benchmarkProgress}
              onRun={handleRunBenchmark}
              onReplayWal={handleReplayWal}
            />
            <div className="two-col">
              <DataTable
                title="De-dup Table"
                icon="ID"
                columns={['Idempotency Key', 'Status', 'Node', 'Event Time']}
                rows={dedup.map((r) => [shortId(r.idempotencyKey), tag(r.resultStatus, 'success'), r.nodeId, fmtMs(r.eventTimeMs)])}
              />
              <DataTable
                title="WAL Log"
                icon="WAL"
                columns={['Seq#', 'Operation', 'Node', 'Status']}
                rows={wal.map((r) => [`#${r.sequenceNumber}`, r.operationType, r.nodeId, tag(r.status, r.status === 'COMMITTED' ? 'committed' : 'pending')])}
              />
            </div>
            <DataTable
              title="Committed Transactions"
              icon="TX"
              columns={['Transaction ID', 'Sender', 'Receiver', 'Amount', 'Node']}
              rows={transactions.map((t) => [shortId(t.transactionId), t.senderAccount, t.receiverAccount, fmtMoney(t.amount), t.nodeId])}
            />
          </section>
        </div>
      </main>
      <Toast toast={toast} />
    </>
  )
}

function tag(text, type) {
  return <span className={`tag tag-${type}`}>{text}</span>
}

function shortId(id) {
  return id ? `${id.substring(0, 8)}...` : '-'
}

function fmtMoney(value) {
  return `${Number(value || 0).toLocaleString('vi-VN')} VND`
}

function fmtMs(value) {
  return value ? new Date(Number(value)).toLocaleTimeString('vi-VN', { hour12: false }) : '-'
}

function parseSse(event) {
  try {
    return JSON.parse(event.data || '{}')
  } catch {
    return {}
  }
}

function upsertRun(rows, run) {
  if (!run?.mode) return rows
  return [...rows.filter((row) => row.mode !== run.mode), run]
}

function upsertWindow(rows, windowRow) {
  const key = windowKey(windowRow)
  return [...rows.filter((row) => windowKey(row) !== key), windowRow].sort(compareWindows)
}

function mergeWindows(rows, incoming) {
  return incoming.reduce((acc, row) => upsertWindow(acc, row), rows)
}

function windowKey(row) {
  return `${row.mode}-${row.windowStartMs}`
}

function compareWindows(a, b) {
  const time = Number(a.windowStartMs || 0) - Number(b.windowStartMs || 0)
  if (time !== 0) return time
  return String(a.mode || '').localeCompare(String(b.mode || ''))
}
