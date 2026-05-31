import { useEffect, useState } from 'react'
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
  const [toast, setToast] = useState(null)

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
    setBenchmark(benchmarkData)
  }

  useEffect(() => {
    refreshAll().catch((error) => showToast(error.message, 'error'))
    const timer = window.setInterval(() => refreshAll().catch(() => {}), 10000)
    return () => window.clearInterval(timer)
  }, [])

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
              onRun={async (payload) => {
                showToast('Running EOS vs ALO benchmark...', 'warning')
                const result = await api.benchmarkCompare(payload)
                setBenchmark(result)
                await refreshAll()
                showToast('Benchmark complete', 'success')
              }}
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
