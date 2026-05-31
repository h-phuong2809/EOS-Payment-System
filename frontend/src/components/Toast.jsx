export default function Toast({ toast }) {
  return <div id="toast" className={toast ? `show ${toast.type}` : ''}>{toast?.message}</div>
}
