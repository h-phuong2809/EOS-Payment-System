export default function DataTable({ title, icon, columns, rows }) {
  return (
    <div className="big-panel">
      <div className="panel-header"><h2><span className="table-icon">{icon}</span> {title}</h2></div>
      <div className="data-table-wrap">
        <table>
          <thead><tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr></thead>
          <tbody>
            {rows.length === 0 && <tr><td colSpan={columns.length} className="empty">No entries</td></tr>}
            {rows.map((row, index) => (
              <tr key={index}>{row.map((cell, cellIndex) => <td key={cellIndex}>{cell}</td>)}</tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
