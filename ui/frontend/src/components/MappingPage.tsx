import { useEffect, useState } from 'react'
import { AlertTriangle, ArrowRight, Columns, Database, Loader, RefreshCw } from 'lucide-react'
import { getMappings, Mapping } from '../api/client'

export default function MappingPage() {
  const [data, setData] = useState<{ mappings: Mapping[]; namespace: string; sink_mode: string } | null>(null)
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState<string | null>(null)

  const load = () => {
    setLoading(true)
    getMappings()
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [])

  const grouped = (data?.mappings ?? []).reduce<Record<string, Mapping[]>>((acc, m) => {
    acc[m.source_name] = [...(acc[m.source_name] ?? []), m]
    return acc
  }, {})

  // Detect target path collisions across sources
  const pathCounts = (data?.mappings ?? []).reduce<Record<string, string[]>>((acc, m) => {
    acc[m.target_path] = [...(acc[m.target_path] ?? []), m.source_name]
    return acc
  }, {})
  const collisions = new Set(
    Object.entries(pathCounts).filter(([, srcs]) => srcs.length > 1).map(([path]) => path)
  )

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Mappings</h1>
          <p style={S.subtitle}>Source tables → Dremio target paths</p>
        </div>
        <button style={S.btnSecondary} onClick={load} disabled={loading}>
          {loading ? <Loader size={13} /> : <RefreshCw size={13} />} Refresh
        </button>
      </div>

      {data && (
        <div style={S.infoBar}>
          <Database size={13} color="var(--primary)" />
          <span>Target namespace: <code style={S.code}>{data.namespace}</code></span>
          <span style={S.modeBadge}>{data.sink_mode === 'iceberg' ? 'Mode B — Iceberg' : 'Mode A — Dremio SQL'}</span>
          <span style={{ marginLeft: 'auto', color: 'var(--muted-foreground)', fontSize: 12 }}>
            {data.mappings.length} table{data.mappings.length !== 1 ? 's' : ''} mapped
          </span>
        </div>
      )}

      {collisions.size > 0 && (
        <div style={S.collisionWarning}>
          <AlertTriangle size={14} color="var(--status-warning)" />
          <span>
            <strong style={{ color: 'var(--status-warning)' }}>Target collision</strong> — {collisions.size} Dremio path{collisions.size !== 1 ? 's are' : ' is'} shared by multiple sources:{' '}
            {[...collisions].map(p => <code key={p} style={{ ...S.code, margin: '0 4px' }}>{p}</code>)}
          </span>
        </div>
      )}

      {loading && !data && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, color: 'var(--secondary-foreground)', padding: '40px 0' }}>
          <Loader size={16} /> Loading mappings…
        </div>
      )}

      {data?.mappings.length === 0 && (
        <div style={S.empty}>
          <Database size={36} color="var(--muted-foreground)" />
          <p>No sources configured yet. Add a source to see mappings.</p>
        </div>
      )}

      {Object.entries(grouped).map(([sourceName, mappings]) => {
        const srcType = mappings[0]?.source_type ?? ''
        return (
          <div key={sourceName} style={S.sourceBlock}>
            <div style={S.sourceHeader}>
              <span style={{ ...S.typeBadge, ...typeColor(srcType) }}>{srcType}</span>
              <span style={S.sourceName}>{sourceName}</span>
              <span style={S.tableCount}>{mappings.length} table{mappings.length !== 1 ? 's' : ''}</span>
            </div>

            <div style={S.mappingTable}>
              <div style={S.tableHead}>
                <span style={S.col1}>Source table</span>
                <span style={S.colArrow} />
                <span style={S.col2}>Dremio path</span>
                <span style={S.col3}>Columns</span>
              </div>
              {mappings.map(m => {
                const key = `${m.source_name}:${m.source_table}`
                const isExpanded = expanded === key
                return (
                  <div key={m.source_table}>
                    <div style={S.tableRow}>
                      <span style={{ ...S.col1, fontFamily: 'monospace', fontSize: 12, color: 'var(--secondary-foreground)' }}>
                        {m.source_table}
                      </span>
                      <span style={S.colArrow}><ArrowRight size={13} color="var(--muted-foreground)" /></span>
                      <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <span style={{ fontFamily: 'monospace', fontSize: 12, color: collisions.has(m.target_path) ? 'var(--status-warning)' : 'var(--accent)' }}>
                          {m.target_path}
                        </span>
                        {collisions.has(m.target_path) && <AlertTriangle size={12} color="var(--status-warning)" />}
                      </span>
                      <span style={S.col3}>
                        {m.all_columns ? (
                          <span style={S.allColsBadge}>all columns</span>
                        ) : (
                          <button style={S.colsBtn} onClick={() => setExpanded(isExpanded ? null : key)}>
                            <Columns size={10} />
                            {m.columns.length} col{m.columns.length !== 1 ? 's' : ''}
                            {isExpanded ? ' ▲' : ' ▼'}
                          </button>
                        )}
                      </span>
                    </div>
                    {isExpanded && (
                      <div style={S.colExpand}>
                        {m.columns.map(c => (
                          <span key={c} style={S.colPill}>{c}</span>
                        ))}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function typeColor(type: string): React.CSSProperties {
  const map: Record<string, React.CSSProperties> = {
    postgres: { background: 'var(--status-info-bg)', color: 'var(--status-info)' },
    mysql:    { background: 'var(--status-success-bg)', color: 'var(--status-success)' },
    mongodb:  { background: 'var(--muted)', color: 'var(--accent)' },
    dynamodb: { background: 'var(--selected)', color: 'var(--primary)' },
    debezium: { background: 'var(--status-warning-bg)', color: 'var(--status-warning)' },
  }
  return map[type] ?? {}
}

const S: Record<string, React.CSSProperties> = {
  page:        { padding: 32, maxWidth: 960 },
  header:      { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 },
  title:       { fontSize: 22, fontWeight: 700, color: 'var(--foreground)' },
  subtitle:    { color: 'var(--secondary-foreground)', fontSize: 13, marginTop: 4 },
  infoBar:     { display: 'flex', alignItems: 'center', gap: 12, background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 16px', marginBottom: 20, fontSize: 13, color: 'var(--secondary-foreground)' },
  code:        { fontFamily: 'monospace', fontSize: 12, color: 'var(--accent)', background: 'var(--muted)', padding: '1px 6px', borderRadius: 3 },
  modeBadge:   { background: 'var(--selected)', color: 'var(--accent)', fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 4 },
  empty:       { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16, padding: '80px 0', color: 'var(--secondary-foreground)' },
  sourceBlock: { background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 10, marginBottom: 16, overflow: 'hidden' },
  sourceHeader:{ display: 'flex', alignItems: 'center', gap: 10, padding: '12px 16px', borderBottom: '1px solid var(--border)', background: 'var(--muted)' },
  sourceName:  { fontWeight: 700, color: 'var(--foreground)', fontSize: 14 },
  tableCount:  { color: 'var(--muted-foreground)', fontSize: 12 },
  typeBadge:   { fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 4 },
  mappingTable:{ },
  tableHead:   { display: 'grid', gridTemplateColumns: '1fr 32px 1fr 140px', padding: '6px 16px', borderBottom: '1px solid var(--border)', background: 'var(--muted)' },
  tableRow:    { display: 'grid', gridTemplateColumns: '1fr 32px 1fr 140px', padding: '9px 16px', borderBottom: '1px solid var(--border)', alignItems: 'center' },
  col1:        { color: 'var(--muted-foreground)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase' as const, letterSpacing: '0.05em' },
  colArrow:    { display: 'flex', justifyContent: 'center', alignItems: 'center' },
  col2:        { color: 'var(--muted-foreground)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase' as const, letterSpacing: '0.05em' },
  col3:        { color: 'var(--muted-foreground)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase' as const, letterSpacing: '0.05em', textAlign: 'right' as const },
  allColsBadge:{ color: 'var(--muted-foreground)', fontSize: 11 },
  colsBtn:     { display: 'inline-flex', alignItems: 'center', gap: 4, background: 'var(--selected)', color: 'var(--accent)', border: 'none', padding: '3px 8px', borderRadius: 4, cursor: 'pointer', fontSize: 11 },
  colExpand:   { display: 'flex', flexWrap: 'wrap', gap: 4, padding: '6px 16px 10px 32px', background: 'var(--muted)', borderBottom: '1px solid var(--border)' },
  colPill:     { fontFamily: 'monospace', fontSize: 11, color: 'var(--secondary-foreground)', background: '#fff', border: '1px solid var(--border)', padding: '1px 6px', borderRadius: 3 },
  btnSecondary:   { display: 'flex', alignItems: 'center', gap: 6, background: 'transparent', color: 'var(--secondary-foreground)', border: '1px solid var(--border)', padding: '8px 14px', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
  collisionWarning: { display: 'flex', alignItems: 'flex-start', gap: 10, background: 'var(--status-warning-bg)', border: '1px solid var(--status-warning)', borderRadius: 8, padding: '10px 14px', marginBottom: 16, fontSize: 13, color: 'var(--foreground)', lineHeight: 1.6 },
}
