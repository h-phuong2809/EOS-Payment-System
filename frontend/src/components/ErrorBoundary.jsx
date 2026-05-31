import React from 'react'

export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  render() {
    if (this.state.error) {
      return (
        <div className="boot-fallback">
          <div className="boot-logo">EOS</div>
          <h1>EOS Payment System</h1>
          <p>React render failed. Open DevTools Console for details.</p>
          <pre>{String(this.state.error?.message || this.state.error)}</pre>
        </div>
      )
    }
    return this.props.children
  }
}
