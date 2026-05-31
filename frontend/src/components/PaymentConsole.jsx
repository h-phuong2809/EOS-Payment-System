import { useMemo, useState } from 'react'

export default function PaymentConsole({ accounts, onSubmit, onRetry, onReset }) {
  const [tab, setTab] = useState('single')
  const [mode, setMode] = useState('EOS')
  const [retryMode, setRetryMode] = useState('EOS')
  const [result, setResult] = useState(null)
  const [retryResult, setRetryResult] = useState(null)
  const first = accounts[0]?.accountId || 'ACC001'
  const second = accounts[1]?.accountId || 'ACC002'
  const accountOptions = useMemo(() => accounts.length ? accounts : [
    { accountId: 'ACC001', ownerName: 'Nguyen Van An' },
    { accountId: 'ACC002', ownerName: 'Tran Thi Bich' },
  ], [accounts])

  async function submitSingle(event) {
    event.preventDefault()
    const form = new FormData(event.currentTarget)
    const payload = Object.fromEntries(form.entries())
    payload.amount = Number(payload.amount)
    payload.mode = mode
    if (!payload.idempotencyKey) delete payload.idempotencyKey
    setResult(await onSubmit(payload))
  }

  async function submitRetry(event) {
    event.preventDefault()
    const form = new FormData(event.currentTarget)
    const payload = Object.fromEntries(form.entries())
    payload.amount = Number(payload.amount)
    payload.retries = Number(payload.retries)
    payload.mode = retryMode
    setRetryResult(await onRetry(payload))
  }

  return (
    <div className="panel payment-panel">
      <div className="panel-header"><h2><span className="panel-icon">PAY</span> Payment Console</h2></div>
      <div className="panel-body">
        <div className="tabs">
          <button className={tab === 'single' ? 'active' : ''} onClick={() => setTab('single')}>Single</button>
          <button className={tab === 'retry' ? 'active' : ''} onClick={() => setTab('retry')}>Retry Sim</button>
        </div>

        {tab === 'single' && (
          <form onSubmit={submitSingle}>
            <ModeToggle mode={mode} setMode={setMode} />
            <div className="inline-grid">
              <Select name="senderAccount" label="Sender" options={accountOptions} defaultValue={first} />
              <Select name="receiverAccount" label="Receiver" options={accountOptions} defaultValue={second} />
            </div>
            <div className="inline-grid">
              <Field name="amount" label="Amount (VND)" type="number" defaultValue="100000" />
              <Field name="nodeId" label="Node" as="select" options={['NODE_A', 'NODE_B', 'NODE_C']} />
            </div>
            <Field name="idempotencyKey" label="Idempotency Key" placeholder="auto UUID if empty" />
            <button className="btn btn-primary">Submit Payment</button>
            <button type="button" className="btn btn-danger" onClick={onReset}>Reset Database</button>
            {result && <pre className={`result-box show ${result.status === 'SUCCESS' ? 'result-success' : 'result-duplicate'}`}>{JSON.stringify(result, null, 2)}</pre>}
          </form>
        )}

        {tab === 'retry' && (
          <form onSubmit={submitRetry}>
            <ModeToggle mode={retryMode} setMode={setRetryMode} />
            <div className="inline-grid">
              <Select name="sender" label="Sender" options={accountOptions} defaultValue={first} />
              <Select name="receiver" label="Receiver" options={accountOptions} defaultValue={second} />
            </div>
            <div className="inline-grid">
              <Field name="amount" label="Amount (VND)" type="number" defaultValue="100000" />
              <Field name="retries" label="Retries" type="number" defaultValue="4" />
            </div>
            <button className="btn btn-warning">Run Retry Simulation</button>
            {retryResult && <pre className="result-box show result-success">{JSON.stringify(retryResult, null, 2)}</pre>}
          </form>
        )}
      </div>
    </div>
  )
}

function ModeToggle({ mode, setMode }) {
  return (
    <div className="mode-toggle">
      <button type="button" className={`mode-btn eos ${mode === 'EOS' ? 'active' : ''}`} onClick={() => setMode('EOS')}>EOS</button>
      <button type="button" className={`mode-btn alo ${mode === 'ALO' ? 'active' : ''}`} onClick={() => setMode('ALO')}>ALO</button>
    </div>
  )
}

function Select({ name, label, options, defaultValue }) {
  return (
    <label className="form-group">
      <span>{label}</span>
      <select name={name} defaultValue={defaultValue}>
        {options.map((item) => <option key={item.accountId} value={item.accountId}>{item.accountId} - {item.ownerName}</option>)}
      </select>
    </label>
  )
}

function Field({ name, label, as, options, ...props }) {
  return (
    <label className="form-group">
      <span>{label}</span>
      {as === 'select' ? (
        <select name={name}>{options.map((item) => <option key={item} value={item}>{item}</option>)}</select>
      ) : (
        <input name={name} {...props} />
      )}
    </label>
  )
}
