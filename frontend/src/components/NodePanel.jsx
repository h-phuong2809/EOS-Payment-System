export default function NodePanel({ nodes, onToggle }) {
  return (
    <div className="panel">
      <div className="panel-header">
        <h2><span className="panel-icon">ND</span> Distributed Nodes</h2>
      </div>
      <div className="panel-body">
        <div className="nodes-grid">
          {nodes.map((node) => (
            <button className={`node-card ${node.status?.toLowerCase()}`} key={node.nodeId} onClick={() => onToggle(node.nodeId)}>
              <div className="nd-icon" aria-hidden="true" />
              <div className="nd-name">{node.nodeId}</div>
              <div className="nd-status">{node.status}</div>
              <div className="nd-count">{node.processedCount} txns</div>
            </button>
          ))}
        </div>
        <div className="info-box">
          <strong>Node failure</strong> rejects writes before payment processing.
        </div>
      </div>
    </div>
  )
}
