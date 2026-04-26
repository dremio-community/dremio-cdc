import { useEffect, useState } from 'react'
import { Loader, Save } from 'lucide-react'
import { getSettings, saveSettings, Settings } from '../api/client'

export default function SettingsPage() {
  const [s, setS] = useState<Settings>({
    batch_size: 500,
    batch_timeout_seconds: 10,
    snapshot_on_first_run: true,
    offset_db_path: './cdc_offsets.db',
    log_level: 'INFO',
    sink_mode: 'dremio',
  })
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)

  useEffect(() => { getSettings().then(d => setS(prev => ({ ...prev, ...d }))).catch(() => {}) }, [])

  const set = (k: keyof Settings, v: unknown) => setS(c => ({ ...c, [k]: v }))

  const handleSave = async () => {
    setSaving(true)
    try { await saveSettings(s); setSaved(true); setTimeout(() => setSaved(false), 2000) }
    catch {}
    setSaving(false)
  }

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Settings</h1>
          <p style={S.subtitle}>Batching, offsets, and logging</p>
        </div>
        <button style={S.btnPrimary} onClick={handleSave} disabled={saving}>
          {saving ? <Loader size={13} /> : <Save size={13} />}
          {saved ? 'Saved!' : 'Save'}
        </button>
      </div>

      <div style={S.card}>
        <h2 style={S.sectionTitle}>Batching</h2>
        <p style={S.sectionSub}>Controls how events are grouped before writing to Dremio</p>
        <div style={S.grid2}>
          <div>
            <label style={S.label}>Batch size (events)</label>
            <input style={S.input} type="number" value={s.batch_size ?? 500}
              onChange={e => set('batch_size', +e.target.value)} />
            <div style={S.hint}>Starting batch size. Ignored when adaptive batching is on.</div>
          </div>
          <div>
            <label style={S.label}>Batch timeout (seconds)</label>
            <input style={S.input} type="number" value={s.batch_timeout_seconds ?? 10}
              onChange={e => set('batch_timeout_seconds', +e.target.value)} />
            <div style={S.hint}>Flush even if batch_size not reached.</div>
          </div>
        </div>

        <label style={S.checkRow}>
          <input type="checkbox" checked={s.adaptive_batching ?? true}
            onChange={e => set('adaptive_batching', e.target.checked)} />
          <div>
            <div style={{ color: '#e2e8f0', fontWeight: 500 }}>Adaptive batching</div>
            <div style={S.hint}>Auto-tune batch size based on throughput and lag. High throughput → larger batches. Low lag → smaller batches.</div>
          </div>
        </label>

        {(s.adaptive_batching ?? true) && (
          <div style={S.grid2}>
            <div>
              <label style={S.label}>Min batch size</label>
              <input style={S.input} type="number" value={s.min_batch_size ?? 100}
                onChange={e => set('min_batch_size', +e.target.value)} />
            </div>
            <div>
              <label style={S.label}>Max batch size</label>
              <input style={S.input} type="number" value={s.max_batch_size ?? 5000}
                onChange={e => set('max_batch_size', +e.target.value)} />
            </div>
          </div>
        )}
      </div>

      <div style={S.card}>
        <h2 style={S.sectionTitle}>Snapshot</h2>
        <p style={S.sectionSub}>Full-table load on first run</p>
        <label style={S.checkRow}>
          <input type="checkbox" checked={s.snapshot_on_first_run ?? true}
            onChange={e => set('snapshot_on_first_run', e.target.checked)} />
          <div>
            <div style={{ color: '#e2e8f0', fontWeight: 500 }}>Snapshot on first run</div>
            <div style={S.hint}>When enabled, the daemon reads every row in each table before switching to streaming mode. Disable if the table is already pre-populated in Dremio.</div>
          </div>
        </label>

        <label style={S.checkRow}>
          <input type="checkbox" checked={s.incremental_snapshot ?? false}
            onChange={e => set('incremental_snapshot', e.target.checked)} />
          <div>
            <div style={{ color: '#e2e8f0', fontWeight: 500 }}>Incremental snapshot</div>
            <div style={S.hint}>Read the table in chunks using a cursor column (e.g. <code style={{ color: '#94a3b8' }}>id</code>) instead of a single full scan. Streaming starts sooner and restarts resume mid-table. Recommended for tables with millions of rows.</div>
          </div>
        </label>

        {(s.incremental_snapshot ?? false) && (
          <div style={S.grid2}>
            <div>
              <label style={S.label}>Chunk size (rows)</label>
              <input style={S.input} type="number" value={s.snapshot_chunk_size ?? 10000}
                onChange={e => set('snapshot_chunk_size', +e.target.value)} />
              <div style={S.hint}>Rows fetched per chunk. Progress is saved after each chunk.</div>
            </div>
            <div>
              <label style={S.label}>Cursor column (optional)</label>
              <input style={S.input} value={s.snapshot_cursor_column ?? ''}
                onChange={e => set('snapshot_cursor_column', e.target.value)}
                placeholder="Auto-detect from primary key" />
              <div style={S.hint}>Leave blank to auto-detect the primary key. Must be an ordered, indexed column.</div>
            </div>
          </div>
        )}
      </div>

      <div style={S.card}>
        <h2 style={S.sectionTitle}>Offset storage</h2>
        <p style={S.sectionSub}>Tracks replication positions — SQLite (single process) or PostgreSQL (multi-agent)</p>
        <div>
          <label style={S.label}>Offset DB path / DSN</label>
          <input style={S.input} value={s.offset_db_path ?? './cdc_offsets.db'}
            onChange={e => set('offset_db_path', e.target.value)} />
          <div style={S.hint}>
            SQLite: <code style={{ color: '#94a3b8' }}>./cdc_offsets.db</code>&nbsp;&nbsp;·&nbsp;&nbsp;
            PostgreSQL: <code style={{ color: '#94a3b8' }}>postgresql://user:pw@host/db</code>
            &nbsp;— PostgreSQL enables multiple engine processes to share offsets safely.
          </div>
        </div>
      </div>

      <div style={S.card}>
        <h2 style={S.sectionTitle}>Logging</h2>
        <div>
          <label style={S.label}>Log level</label>
          <select style={S.input} value={s.log_level ?? 'INFO'}
            onChange={e => set('log_level', e.target.value)}>
            <option value="DEBUG">DEBUG</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
          </select>
        </div>
      </div>
    </div>
  )
}

const S: Record<string, React.CSSProperties> = {
  page: { padding: 32, maxWidth: 700 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 },
  title: { fontSize: 22, fontWeight: 700, color: '#f1f5f9' },
  subtitle: { color: '#64748b', fontSize: 13, marginTop: 4 },
  card: { background: '#1e293b', border: '1px solid #334155', borderRadius: 10, padding: 24, marginBottom: 16, display: 'flex', flexDirection: 'column', gap: 16 },
  sectionTitle: { fontSize: 15, fontWeight: 700, color: '#f1f5f9' },
  sectionSub: { color: '#64748b', fontSize: 13, marginTop: -8 },
  grid2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 },
  label: { display: 'block', color: '#94a3b8', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 },
  input: { width: '100%', background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '8px 12px', color: '#e2e8f0', fontSize: 13, outline: 'none' },
  hint: { color: '#64748b', fontSize: 11, marginTop: 6, lineHeight: 1.5 },
  checkRow: { display: 'flex', gap: 12, cursor: 'pointer', alignItems: 'flex-start' },
  btnPrimary: { display: 'flex', alignItems: 'center', gap: 6, background: '#2563eb', color: '#fff', border: 'none', padding: '8px 18px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
}
