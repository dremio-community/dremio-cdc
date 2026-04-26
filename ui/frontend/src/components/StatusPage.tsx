import { useEffect, useState } from 'react'
import { Activity, AlertCircle, CheckCircle, Loader, Pause, Play, RefreshCw, Square } from 'lucide-react'
import { getStatus, startEngine, stopEngine, resetOffset, EngineStatus, WorkerStatus } from '../api/client'

export default function StatusPage() {
  const [status, setStatus] = useState<EngineStatus | null>(null)
  const [error, setError] = useState('')
  const [actionMsg, setActionMsg] = useState('')

  const refresh = async () => {
    try {
      setStatus(await getStatus())
      setError('')
    } catch (e: any) {
      setError(e.message)
    }
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 2500)
    return () => clearInterval(id)
  }, [])

  const handleStart = async () => {
    try {
      await startEngine()
      setActionMsg('Engine starting…')
      setTimeout(() => setActionMsg(''), 3000)
    } catch (e: any) { setActionMsg(e.message) }
  }

  const handleStop = async () => {
    try {
      await stopEngine()
      setActionMsg('Engine stopping…')
      setTimeout(() => setActionMsg(''), 3000)
    } catch (e: any) { setActionMsg(e.message) }
  }

  const handleReset = async (source: string, table: string) => {
    if (!confirm(`Reset offset for ${source}/${table}? This will re-snapshot on next start.`)) return
    await resetOffset(source, table)
    setActionMsg(`Offset reset for ${table}`)
    setTimeout(() => setActionMsg(''), 3000)
  }

  const state = status?.engine_state ?? 'stopped'
  const isRunning = state === 'running' || state === 'starting'
  const sum = status?.summary

  const avgLag = (() => {
    const lags = (status?.workers ?? []).map(w => w.lag_seconds).filter(l => l !== null) as number[]
    return lags.length ? lags.reduce((a, b) => a + b, 0) / lags.length : null
  })()

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Status</h1>
          <p style={S.subtitle}>Live replication status across all tables</p>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          {actionMsg && <span style={S.actionMsg}>{actionMsg}</span>}
          <button style={S.btnSecondary} onClick={refresh}><RefreshCw size={14} /></button>
          {isRunning
            ? <button style={S.btnDanger} onClick={handleStop}><Square size={14} /> Stop</button>
            : <button style={S.btnPrimary} onClick={handleStart}><Play size={14} /> Start</button>
          }
        </div>
      </div>

      {/* Engine state banner */}
      <div style={{ ...S.banner, ...bannerStyle(state) }}>
        {stateIcon(state)}
        <span style={{ fontWeight: 600 }}>Engine: {state}</span>
        {status?.engine_started_at && (
          <span style={{ color: '#94a3b8', fontSize: 12 }}>· up {reltime(status.engine_started_at)}</span>
        )}
        {status && <span style={{ color: '#94a3b8', fontSize: 12, marginLeft: 4 }}>· {status.config_path}</span>}
        <span style={{ marginLeft: 'auto', fontSize: 11, color: '#475569' }}>
          Prometheus metrics at <code style={{ color: '#60a5fa' }}>/metrics</code>
        </span>
      </div>

      {/* Summary bar */}
      {sum && (
        <div style={S.summaryBar}>
          <SummaryTile label="Total events" value={sum.total_events.toLocaleString()} />
          <SummaryTile
            label="Total errors"
            value={sum.total_errors.toLocaleString()}
            color={sum.total_errors > 0 ? '#f87171' : undefined}
          />
          <SummaryTile label="Active workers" value={`${sum.active_workers} / ${sum.total_workers}`} />
          <SummaryTile
            label="Avg lag"
            value={avgLag !== null ? `${avgLag.toFixed(1)}s` : '—'}
            color={lagColor(avgLag)}
          />
        </div>
      )}

      {error && <div style={S.error}><AlertCircle size={14} /> {error}</div>}

      {/* Worker grid */}
      {!status?.workers?.length ? (
        <div style={S.empty}>
          <Activity size={32} color="#334155" />
          <p>No workers running. Configure sources and start the engine.</p>
        </div>
      ) : (
        <div style={S.grid}>
          {status.workers.map(w => (
            <WorkerCard key={`${w.source}/${w.table}`} w={w} onReset={handleReset} />
          ))}
        </div>
      )}
    </div>
  )
}

function SummaryTile({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={S.summaryTile}>
      <div style={{ ...S.summaryVal, ...(color ? { color } : {}) }}>{value}</div>
      <div style={S.summaryLabel}>{label}</div>
    </div>
  )
}

function WorkerCard({ w, onReset }: { w: WorkerStatus; onReset: (s: string, t: string) => void }) {
  return (
    <div style={{ ...S.card, ...(w.state === 'error' ? S.cardError : {}) }}>
      <div style={S.cardHeader}>
        <div>
          <div style={S.cardSource}>{w.source}</div>
          <div style={S.cardTable}>{w.table}</div>
        </div>
        <StateBadge state={w.state} />
      </div>

      <div style={S.metrics}>
        <Metric label="Events" value={w.events_written.toLocaleString()} />
        <Metric label="Per min" value={w.events_per_minute.toLocaleString()} />
        <Metric
          label="Lag"
          value={w.lag_seconds !== null ? `${w.lag_seconds}s` : '—'}
          color={lagColor(w.lag_seconds)}
        />
        <Metric
          label="Batch"
          value={w.current_batch_size?.toLocaleString() ?? '—'}
        />
        <Metric
          label="Flush ms"
          value={w.last_flush_duration_ms > 0 ? w.last_flush_duration_ms.toFixed(0) : '—'}
        />
      </div>

      {/* Pipeline lag + error count */}
      {(w.pipeline_lag_seconds !== null || w.error_count > 0) && (
        <div style={{ display: 'flex', gap: 16, fontSize: 11, color: '#64748b' }}>
          {w.pipeline_lag_seconds !== null && (
            <span>pipeline lag: <strong style={{ color: '#94a3b8' }}>{w.pipeline_lag_seconds}s</strong></span>
          )}
          {w.error_count > 0 && (
            <span style={{ color: '#f87171' }}>errors: <strong>{w.error_count}</strong></span>
          )}
        </div>
      )}

      {/* Schema drift warning */}
      {w.schema_drift && (
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 6,
          fontSize: 11, color: '#fbbf24', background: '#1c1700',
          border: '1px solid #78350f', borderRadius: 6, padding: '6px 10px' }}>
          <AlertCircle size={12} style={{ marginTop: 1, flexShrink: 0 }} />
          <span><strong>Schema drift:</strong> {w.schema_drift}</span>
        </div>
      )}

      {/* Sparkline */}
      {w.rate_history.length > 1 && <Sparkline data={w.rate_history} />}

      {w.error && (
        <div style={S.workerError}><AlertCircle size={12} /> {w.error}</div>
      )}

      {w.last_offset && (
        <div style={S.offset}>offset: {w.last_offset.slice(0, 40)}{w.last_offset.length > 40 ? '…' : ''}</div>
      )}

      <div style={S.cardFooter}>
        <button style={S.btnReset} onClick={() => onReset(w.source, w.table)}>
          Reset offset
        </button>
        {w.started_at && (
          <span style={{ color: '#475569', fontSize: 11 }}>started {reltime(w.started_at)}</span>
        )}
      </div>
    </div>
  )
}

function Sparkline({ data }: { data: [number, number][] }) {
  const W = 120, H = 28, BAR_W = 4, GAP = 2
  const maxVal = Math.max(...data.map(d => d[1]), 1)
  const visible = data.slice(-Math.floor(W / (BAR_W + GAP)))
  return (
    <svg width={W} height={H} style={{ display: 'block' }}>
      {visible.map(([, epm], i) => {
        const bh = Math.max(2, (epm / maxVal) * (H - 2))
        return (
          <rect
            key={i}
            x={i * (BAR_W + GAP)}
            y={H - bh}
            width={BAR_W}
            height={bh}
            rx={1}
            fill="#3b82f6"
            opacity={0.7}
          />
        )
      })}
    </svg>
  )
}

function Metric({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={S.metric}>
      <div style={{ ...S.metricVal, ...(color ? { color } : {}) }}>{value}</div>
      <div style={S.metricLabel}>{label}</div>
    </div>
  )
}

function StateBadge({ state }: { state: string }) {
  const map: Record<string, { color: string; bg: string }> = {
    streaming:    { color: '#4ade80', bg: '#052e16' },
    snapshotting: { color: '#facc15', bg: '#1c1700' },
    paused:       { color: '#94a3b8', bg: '#1e293b' },
    error:        { color: '#f87171', bg: '#2d0a0a' },
    idle:         { color: '#64748b', bg: '#1e293b' },
  }
  const s = map[state] ?? map.idle
  return (
    <span style={{ ...S.badge, color: s.color, background: s.bg }}>
      {state}
    </span>
  )
}

function stateIcon(state: string) {
  if (state === 'running') return <CheckCircle size={16} color="#4ade80" />
  if (state === 'starting') return <Loader size={16} color="#facc15" />
  if (state === 'error') return <AlertCircle size={16} color="#f87171" />
  return <Pause size={16} color="#64748b" />
}

function bannerStyle(state: string): React.CSSProperties {
  if (state === 'running') return { borderColor: '#166534', background: '#052e16' }
  if (state === 'starting') return { borderColor: '#854d0e', background: '#1c1700' }
  if (state === 'error') return { borderColor: '#7f1d1d', background: '#2d0a0a' }
  return {}
}

function lagColor(lag: number | null): string | undefined {
  if (lag === null) return undefined
  if (lag < 5) return '#4ade80'
  if (lag < 30) return '#facc15'
  return '#f87171'
}

function reltime(ts: number) {
  const diff = Date.now() / 1000 - ts
  if (diff < 5) return 'just now'
  if (diff < 60) return `${Math.floor(diff)}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

const S: Record<string, React.CSSProperties> = {
  page: { padding: 32, maxWidth: 1200 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 24 },
  title: { fontSize: 22, fontWeight: 700, color: '#f1f5f9' },
  subtitle: { color: '#64748b', fontSize: 13, marginTop: 4 },
  banner: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '10px 14px', borderRadius: 8, border: '1px solid #1e293b',
    background: '#1e293b', marginBottom: 16, fontSize: 13,
  },
  summaryBar: { display: 'flex', gap: 12, marginBottom: 24 },
  summaryTile: {
    background: '#1e293b', border: '1px solid #334155', borderRadius: 8,
    padding: '12px 18px', flex: 1, minWidth: 100,
  },
  summaryVal: { fontSize: 20, fontWeight: 700, color: '#f1f5f9' },
  summaryLabel: { fontSize: 11, color: '#64748b', marginTop: 2 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 16 },
  card: {
    background: '#1e293b', border: '1px solid #334155', borderRadius: 10, padding: 18,
    display: 'flex', flexDirection: 'column', gap: 12,
  },
  cardError: { borderColor: '#7f1d1d', background: '#1a0f0f' },
  cardHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' },
  cardSource: { fontSize: 11, color: '#64748b', fontFamily: 'monospace', marginBottom: 2 },
  cardTable: { fontSize: 14, fontWeight: 600, color: '#e2e8f0', fontFamily: 'monospace' },
  badge: { fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 99 },
  metrics: { display: 'flex', gap: 12 },
  metric: { flex: 1 },
  metricVal: { fontSize: 17, fontWeight: 700, color: '#f1f5f9' },
  metricLabel: { fontSize: 11, color: '#64748b', marginTop: 2 },
  workerError: {
    display: 'flex', alignItems: 'center', gap: 6, color: '#f87171',
    fontSize: 12, background: '#2d0a0a', padding: '6px 10px', borderRadius: 6,
  },
  offset: { fontSize: 11, color: '#475569', fontFamily: 'monospace' },
  cardFooter: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  actionMsg: { color: '#94a3b8', fontSize: 12 },
  error: {
    display: 'flex', alignItems: 'center', gap: 8, color: '#f87171',
    background: '#2d0a0a', border: '1px solid #7f1d1d', borderRadius: 8,
    padding: '10px 14px', marginBottom: 20, fontSize: 13,
  },
  empty: {
    display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 12,
    color: '#475569', padding: '60px 0', textAlign: 'center',
  },
  btnPrimary: {
    display: 'flex', alignItems: 'center', gap: 6,
    background: '#2563eb', color: '#fff', border: 'none',
    padding: '8px 16px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13,
  },
  btnDanger: {
    display: 'flex', alignItems: 'center', gap: 6,
    background: '#dc2626', color: '#fff', border: 'none',
    padding: '8px 16px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13,
  },
  btnSecondary: {
    display: 'flex', alignItems: 'center', gap: 6,
    background: '#1e293b', color: '#94a3b8', border: '1px solid #334155',
    padding: '8px 10px', borderRadius: 6, cursor: 'pointer', fontSize: 13,
  },
  btnReset: {
    background: 'none', color: '#475569', border: '1px solid #334155',
    padding: '4px 10px', borderRadius: 4, cursor: 'pointer', fontSize: 11,
  },
}
