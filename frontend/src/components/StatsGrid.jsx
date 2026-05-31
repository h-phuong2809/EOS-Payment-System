export default function StatsGrid({ stats }) {
  const eos = stats?.latestEos || {}
  const alo = stats?.latestAlo || {}
  const walEntries = Number(stats?.walEntries ?? 0)
  const processed = Number(stats?.processed ?? 0)
  const walRatio = processed > 0 ? `${walEntries} / ${processed}` : `${walEntries} / 0`
  const cards = [
    ['Total Requests', stats?.totalRequests ?? 0],
    ['Processed', stats?.processed ?? 0],
    ['Duplicates Blocked', stats?.duplicatesBlocked ?? 0, 'warning'],
    ['Transactions', stats?.transactions ?? 0, 'success'],
    ['WAL Entries', stats?.walEntries ?? 0],
    ['WAL / Processed', walRatio, 'purple'],
    ['De-dup State Size', stats?.dedupTableSize ?? 0],
    ['Avg Event Lag', `${Number(eos.avgEventLagMs || 0).toFixed(1)} ms`],
    ['Out-of-Order Events', eos.outOfOrderCount ?? 0, 'danger'],
    ['Late Events', eos.lateEvents ?? 0, 'danger'],
    ['Backpressure Delayed', eos.backpressureDelayed ?? 0, 'warning'],
    ['EOS Latest TPS', eos.tps ?? 0],
    ['ALO Latest TPS', alo.tps ?? 0, 'warning'],
  ]

  return (
    <div className="stats-grid">
      {cards.map(([label, value, tone]) => (
        <div className="stat-card" key={label}>
          <div className={`val ${tone ? `tone-${tone}` : ''}`}>{value}</div>
          <div className="lbl">{label}</div>
        </div>
      ))}
    </div>
  )
}
