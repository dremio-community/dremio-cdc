import { useEffect, useState } from 'react'
import { AlertCircle, CheckCircle, Clock, RefreshCw, RotateCcw, Trash2, XCircle } from 'lucide-react'
import {
  getDLQ, retryDLQEntry, retryAllDLQ, discardDLQEntry, discardAllDLQ,
  DLQEntry, DLQStats,
} from '../api/client'

export default function DLQPage() {
  const [entries, setEntries] = useState<DLQEntry[]>([])
  const [stats, setStats] = useState<DLQStats | null>(null)
  const [actionMsg, setActionMsg] = useState('')
  const [error, setError] = useState('')

  const load = async () => {
    try {
      const d = await getDLQ()
      setEntries(d.entries)
      setStats(d.stats)
      setError('')
    } catch (e: any) { setError(e.message) }
  }

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  const act = async (fn: () => Promise<unknown>, msg: string) => {
    try { await fn(); setActionMsg(msg); setTimeout(() => setActionMsg(''), 2500); await load() }
    catch (e: any) { setError(e.message) }
  }

  const pending  = stats?.pending.entries  ?? 0
  const exhausted = stats?.exhausted.entries ?? 0
  const replayed = stats?.replayed.entries  ?? 0

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Dead Letter Queue</h1>
          <p style={S.subtitle}>Failed batches parked here — inspect, retry, or discard</p>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          {actionMsg && <span style={S.actionMsg}>{actionMsg}</span>}
          {error && <span style={{ color: 'var(--status-error)', fontSize: 12 }}>{error}</span>}
          <button style={S.btnSecondary} onClick={load}><RefreshCw size={14} /></button>
          {(pending + exhausted) > 0 && (
            <>
              <button style={S.btnWarning}
                onClick={() => act(retryAllDLQ, 'All exhausted entries reset to pending')}>
                <RotateCcw size={13} /> Retry all exhausted
              </button>
              <button style={S.btnDanger}
                onClick={() => { if (confirm('Discard all pending/exhausted entries?')) act(discardAllDLQ, 'All discarded') }}>
                <Trash2 size={13} /> Discard all
              </button>
            </>
          )}
        </div>
      </div>

      {/* Stats bar */}
      {stats && (
        <div style={S.statsBar}>
          <StatTile label="Pending"   value={stats.pending.entries}   events={stats.pending.events}   color="var(--status-warning)" />
          <StatTile label="Exhausted" value={stats.exhausted.entries} events={stats.exhausted.events} color="var(--status-error)" />
          <StatTile label="Replayed"  value={stats.replayed.entries}  events={stats.replayed.events}  color="var(--status-success)" />
          <StatTile label="Discarded" value={stats.discarded.entries} events={stats.discarded.events} color="var(--secondary-foreground)" />
        </div>
      )}

      {/* Entries table */}
      {entries.length === 0 ? (
        <div style={S.empty}>
          <CheckCircle size={32} color="var(--status-success)" />
          <p>No entries in the dead letter queue. All flushes succeeded.</p>
        </div>
      ) : (
        <div style={S.tableWrap}>
          <table style={S.table}>
            <thead>
              <tr>
                {['ID', 'Source / Table', 'Events', 'Status', 'Retries', 'Error', 'Created', 'Actions'].map(h => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {entries.map((e, i) => (
                <tr key={e.id} style={i % 2 === 0 ? S.trEven : {}}>
                  <td style={{ ...S.td, color: 'var(--muted-foreground)', fontFamily: 'monospace' }}>{e.id}</td>
                  <td style={{ ...S.td, fontFamily: 'monospace' }}>
                    <span style={{ color: 'var(--muted-foreground)', fontSize: 11 }}>{e.source}</span>
                    <br />
                    <span style={{ color: 'var(--foreground)' }}>{e.table}</span>
                  </td>
                  <td style={{ ...S.td, textAlign: 'right' as const }}>{e.event_count.toLocaleString()}</td>
                  <td style={S.td}><StatusBadge status={e.status} /></td>
                  <td style={{ ...S.td, textAlign: 'center' as const, color: 'var(--secondary-foreground)' }}>
                    {e.retry_count}/{e.max_retries}
                  </td>
                  <td style={{ ...S.td, maxWidth: 280 }}>
                    <span style={{ color: 'var(--status-error)', fontSize: 11, fontFamily: 'monospace',
                      display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {e.error ?? '—'}
                    </span>
                  </td>
                  <td style={{ ...S.td, color: 'var(--muted-foreground)', fontSize: 11 }}>{e.created_at.slice(0, 19)}</td>
                  <td style={S.td}>
                    <div style={{ display: 'flex', gap: 6 }}>
                      {e.status !== 'replayed' && e.status !== 'discarded' && (
                        <button style={S.actionBtn}
                          onClick={() => act(() => retryDLQEntry(e.id), `Entry ${e.id} queued for retry`)}>
                          <RotateCcw size={12} /> Retry
                        </button>
                      )}
                      {e.status !== 'discarded' && (
                        <button style={{ ...S.actionBtn, color: 'var(--destructive)' }}
                          onClick={() => act(() => discardDLQEntry(e.id), `Entry ${e.id} discarded`)}>
                          <Trash2 size={12} />
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function StatTile({ label, value, events, color }: {
  label: string; value: number; events: number; color: string
}) {
  return (
    <div style={S.statTile}>
      <div style={{ fontSize: 20, fontWeight: 700, color }}>{value}</div>
      <div style={{ fontSize: 11, color: 'var(--secondary-foreground)', marginTop: 2 }}>{label}</div>
      {events > 0 && <div style={{ fontSize: 10, color: 'var(--muted-foreground)', marginTop: 1 }}>{events.toLocaleString()} events</div>}
    </div>
  )
}

function StatusBadge({ status }: { status: DLQEntry['status'] }) {
  const map = {
    pending:   { color: 'var(--status-warning)', bg: 'var(--status-warning-bg)', icon: <Clock size={11} /> },
    replayed:  { color: 'var(--status-success)', bg: 'var(--status-success-bg)', icon: <CheckCircle size={11} /> },
    exhausted: { color: 'var(--status-error)', bg: 'var(--status-error-bg)', icon: <XCircle size={11} /> },
    discarded: { color: 'var(--secondary-foreground)', bg: 'var(--muted)', icon: <Trash2 size={11} /> },
  }
  const s = map[status] ?? map.discarded
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4,
      fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 99,
      color: s.color, background: s.bg }}>
      {s.icon}{status}
    </span>
  )
}

const S: Record<string, React.CSSProperties> = {
  page:     { padding: 32, maxWidth: 1200 },
  header:   { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 24 },
  title:    { fontSize: 22, fontWeight: 700, color: 'var(--foreground)' },
  subtitle: { color: 'var(--secondary-foreground)', fontSize: 13, marginTop: 4 },
  statsBar: { display: 'flex', gap: 12, marginBottom: 24 },
  statTile: { background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, padding: '12px 18px', flex: 1, minWidth: 120 },
  tableWrap: { background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 10, overflow: 'hidden' },
  table:    { width: '100%', borderCollapse: 'collapse' as const, fontSize: 12 },
  th:       { textAlign: 'left' as const, color: 'var(--muted-foreground)', fontWeight: 600, padding: '10px 14px', borderBottom: '1px solid var(--border)', fontSize: 11, textTransform: 'uppercase' as const, letterSpacing: '0.04em', background: 'var(--muted)' },
  td:       { padding: '10px 14px', color: 'var(--secondary-foreground)', verticalAlign: 'middle' as const, borderBottom: '1px solid var(--border)' },
  trEven:   { background: 'var(--background)' },
  empty:    { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 12, color: 'var(--secondary-foreground)', padding: '60px 0', textAlign: 'center' as const },
  actionMsg: { color: 'var(--muted-foreground)', fontSize: 12 },
  btnSecondary: { display: 'flex', alignItems: 'center', gap: 6, background: 'transparent', color: 'var(--secondary-foreground)', border: '1px solid var(--border)', padding: '8px 10px', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
  btnWarning: { display: 'flex', alignItems: 'center', gap: 6, background: 'var(--status-warning-bg)', color: 'var(--status-warning)', border: '1px solid var(--status-warning)', padding: '8px 14px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
  btnDanger: { display: 'flex', alignItems: 'center', gap: 6, background: 'var(--destructive)', color: '#fff', border: 'none', padding: '8px 14px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
  actionBtn: { display: 'inline-flex', alignItems: 'center', gap: 4, background: 'none', border: '1px solid var(--border)', color: 'var(--secondary-foreground)', padding: '3px 8px', borderRadius: 4, cursor: 'pointer', fontSize: 11 },
}
