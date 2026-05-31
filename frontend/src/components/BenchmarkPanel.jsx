import { Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler)

export default function BenchmarkPanel({ payload, onRun, onReplayWal }) {
  const runs = payload?.runs || []
  const windows = payload?.windows || []
  const eos = runs.find((r) => r.mode === 'EOS')
  const alo = runs.find((r) => r.mode === 'ALO')
  const labels = [...new Set(windows.map((w) => String(w.windowStartMs)))].sort((a, b) => Number(a) - Number(b))
  const eosDuplicateCharges = duplicateCharges(eos)
  const aloDuplicateCharges = duplicateCharges(alo)

  async function runBenchmark(event) {
    event.preventDefault()
    const form = new FormData(event.currentTarget)
    await onRun({
      eventCount: Number(form.get('eventCount')),
      duplicateRate: Number(form.get('duplicateRate')) / 100,
      windowSizeSec: Number(form.get('windowSizeSec')),
      seed: 117,
    })
  }

  return (
    <div className="big-panel">
      <div className="panel-header">
        <h2><span className="panel-icon">BM</span> Live Stream Benchmark</h2>
        <button className="icon-btn" onClick={onReplayWal}>Replay WAL</button>
      </div>
      <form className="benchmark-controls" onSubmit={runBenchmark}>
        <Field name="eventCount" label="Payment Events" defaultValue="160" />
        <Field name="duplicateRate" label="Duplicate Rate (%)" defaultValue="30" />
        <Field name="windowSizeSec" label="Window (sec)" defaultValue="10" />
        <button className="btn btn-primary">Run Compare</button>
      </form>

      <div className="compare-grid">
        <Metric label="EOS TPS" value={eos?.tps || 0} />
        <Metric label="ALO TPS" value={alo?.tps || 0} tone="warning" />
        <Metric label="Duplicate Charges" value={`${eosDuplicateCharges} / ${aloDuplicateCharges}`} tone="danger" />
        <Metric label="Duplicates Blocked" value={eos?.duplicatesBlocked || 0} tone="success" />
        <Metric label="WAL / Success" value={`${Number(eos?.walOverheadPct || 0).toFixed(0)}%`} tone="purple" />
        <Metric label="Late Events" value={eos?.lateEvents || 0} tone="danger" />
        <Metric label="Backpressure" value={eos?.backpressureDelayed || 0} tone="warning" />
      </div>

      <LatencyTable eos={eos} alo={alo} />

      <div className="chart-grid">
        <ChartBox title="TPS per Tumbling Window" labels={labels} windows={windows} field="tps" />
        <ChartBox title="Average Latency per Window (ms)" labels={labels} windows={windows} field="avgLatencyMs" />
      </div>

      <div className="window-table-wrap">
        <table>
          <thead><tr><th>Mode</th><th>Window</th><th>Events</th><th>Success</th><th>Dupes</th><th>Late</th><th>Avg Latency</th><th>TPS</th><th>WAL</th></tr></thead>
          <tbody>
            {windows.length === 0 && <tr><td colSpan="9" className="empty">Run benchmark to see tumbling-window state</td></tr>}
            {windows.map((w) => (
              <tr key={`${w.mode}-${w.windowStartMs}`}>
                <td><span className={`tag ${w.mode === 'EOS' ? 'tag-success' : 'tag-pending'}`}>{w.mode}</span></td>
                <td>{fmtWindow(w.windowStartMs)} - {fmtWindow(w.windowEndMs)}</td>
                <td>{w.totalEvents}</td>
                <td className="tone-success">{w.processedSuccess}</td>
                <td className="tone-warning">{w.duplicatesBlocked}</td>
                <td className="tone-danger">{w.lateEvents}</td>
                <td>{Number(w.avgLatencyMs).toFixed(2)} ms</td>
                <td>{Number(w.tps).toFixed(2)}</td>
                <td>{w.walEntries}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function Field({ name, label, defaultValue }) {
  return (
    <label className="form-group">
      <span>{label}</span>
      <input name={name} type="number" defaultValue={defaultValue} />
    </label>
  )
}

function Metric({ label, value, tone }) {
  return <div className="compare-card"><div className="k">{label}</div><div className={`v ${tone ? `tone-${tone}` : ''}`}>{value}</div></div>
}

function LatencyTable({ eos, alo }) {
  const rows = [
    ['avg latency', 'avgLatencyMs'],
    ['p50 latency', 'p50LatencyMs'],
    ['p95 latency', 'p95LatencyMs'],
    ['p99 latency', 'p99LatencyMs'],
    ['WAL write latency', 'walWriteLatencyMs'],
  ]

  return (
    <div className="latency-table-wrap">
      <table>
        <thead><tr><th>Metric</th><th>EOS</th><th>ALO</th></tr></thead>
        <tbody>
          {rows.map(([label, field]) => (
            <tr key={field}>
              <td>{label}</td>
              <td className="tone-success">{formatLatency(eos, field)}</td>
              <td className="tone-warning">{field === 'walWriteLatencyMs' ? 'N/A' : formatLatency(alo, field)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ChartBox({ title, labels, windows, field }) {
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { labels: { color: '#647084', boxWidth: 10 } },
      title: { display: true, text: title, color: '#17202c', font: { size: 13, weight: '700' } },
    },
    scales: {
      x: { ticks: { color: '#647084', maxRotation: 0 }, grid: { color: 'rgba(100,112,132,.16)' } },
      y: { beginAtZero: true, ticks: { color: '#647084' }, grid: { color: 'rgba(100,112,132,.16)' } },
    },
  }
  const data = {
    labels: labels.map(fmtWindow),
    datasets: [
      dataset('EOS', labels, windows, field, '#15803d', 'rgba(21,128,61,.10)'),
      dataset('ALO', labels, windows, field, '#b45309', 'rgba(180,83,9,.08)'),
    ],
  }
  return <div className="chart-box"><Line options={options} data={data} /></div>
}

function dataset(mode, labels, windows, field, borderColor, backgroundColor) {
  const byWindow = new Map(windows.filter((w) => w.mode === mode).map((w) => [String(w.windowStartMs), Number(w[field] || 0)]))
  return { label: mode, data: labels.map((label) => byWindow.get(String(label)) || 0), borderColor, backgroundColor, tension: 0.35, fill: true }
}

function fmtWindow(ms) {
  return new Date(Number(ms)).toLocaleTimeString('vi-VN', { hour12: false })
}

function formatLatency(run, field) {
  if (!run) return '-'
  return `${Number(run[field] || 0).toFixed(2)} ms`
}

function duplicateCharges(run) {
  if (!run) return 0
  return Math.max(0, Number(run.processedSuccess || 0) - Number(run.uniquePayments || 0))
}
