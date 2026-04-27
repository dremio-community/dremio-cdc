import { useEffect, useRef, useState } from 'react'
import { AlertCircle, ChevronDown, CheckCircle, Cloud, Database, Loader, Save, Zap } from 'lucide-react'
import { getTarget, saveTarget, testTarget, getNamespaces, getSources, TargetConfig, DremioConfig, IcebergConfig, NamespaceItem, TransformStudioConfig, Source } from '../api/client'
import SecretFieldInput from './SecretFieldInput'

export default function TargetPage() {
  const [cfg, setCfg] = useState<TargetConfig>({
    sink_mode: 'dremio',
    dremio: { host: 'localhost', port: 9047, ssl: false, user: 'admin', target_namespace: 'cdc' },
    iceberg: { type: 'rest', write_mode: 'merge', target_namespace: 'cdc' },
    transform_studio: { enabled: false },
  })
  const [sources, setSources] = useState<Source[]>([])
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ ok: boolean; error?: string } | null>(null)
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [saveErr, setSaveErr] = useState('')
  const [namespaces, setNamespaces] = useState<NamespaceItem[] | null>(null)
  const [nsOpen, setNsOpen] = useState(false)
  const [nsLoading, setNsLoading] = useState(false)
  const nsRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (nsRef.current && !nsRef.current.contains(e.target as Node)) setNsOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  useEffect(() => {
    getTarget().then(setCfg).catch(() => {})
    getSources().then(setSources).catch(() => {})
  }, [])

  const updateDremio = (k: keyof DremioConfig, v: unknown) =>
    setCfg(c => ({ ...c, dremio: { ...c.dremio, [k]: v } }))

  const updateIceberg = (k: string, v: unknown) =>
    setCfg(c => ({ ...c, iceberg: { ...c.iceberg, [k]: v } }))

  const updateTS = (k: keyof TransformStudioConfig, v: unknown) =>
    setCfg(c => ({ ...c, transform_studio: { ...c.transform_studio, [k]: v } }))

  const handleTest = async () => {
    setTesting(true)
    setTestResult(null)
    setNamespaces(null)
    try {
      const result = await testTarget(cfg.dremio)
      setTestResult(result)
      if (result.ok) {
        setNsLoading(true)
        getNamespaces().then(r => { if (r.ok) setNamespaces(r.namespaces ?? []) }).finally(() => setNsLoading(false))
      }
    }
    catch (e: any) { setTestResult({ ok: false, error: e.message }) }
    setTesting(false)
  }

  const handleBrowseNs = async () => {
    if (namespaces) { setNsOpen(o => !o); return }
    setNsLoading(true)
    try {
      const r = await getNamespaces()
      if (r.ok) { setNamespaces(r.namespaces ?? []); setNsOpen(true) }
    } catch {}
    setNsLoading(false)
  }

  const handleSave = async () => {
    setSaving(true)
    setSaveErr('')
    try {
      await saveTarget(cfg)
      setSaved(true)
      setTimeout(() => setSaved(false), 2000)
    } catch (e: any) {
      setSaveErr(e.message ?? 'Save failed')
    }
    setSaving(false)
  }

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Target</h1>
          <p style={S.subtitle}>Where CDC events are written</p>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 6 }}>
          <button style={S.btnPrimary} onClick={handleSave} disabled={saving}>
            {saving ? <Loader size={13} /> : <Save size={13} />}
            {saved ? 'Saved!' : 'Save'}
          </button>
          {saveErr && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: '#f87171', fontSize: 12, maxWidth: 340, textAlign: 'right' }}>
              <AlertCircle size={13} />
              {saveErr}
            </div>
          )}
        </div>
      </div>

      {/* Mode selector */}
      <div style={S.modeRow}>
        <ModeCard
          active={cfg.sink_mode === 'dremio'}
          icon={<Database size={20} color="#60a5fa" />}
          title="Mode A — Dremio SQL"
          description="MERGE INTO via Dremio REST API. Simple setup, works with any Dremio edition."
          onClick={() => setCfg(c => ({ ...c, sink_mode: 'dremio' }))}
        />
        <ModeCard
          active={cfg.sink_mode === 'iceberg'}
          icon={<Cloud size={20} color="#a78bfa" />}
          title="Mode B — Open Catalog"
          description="Direct Iceberg writes via PyIceberg. High throughput. Recommended for Dremio Cloud & Enterprise."
          onClick={() => setCfg(c => ({ ...c, sink_mode: 'iceberg' }))}
        />
      </div>

      <CompatWarnings sources={sources} sinkMode={cfg.sink_mode} />

      {/* Dremio connection (always shown — used for metadata refresh in Mode B too) */}
      <Section title="Dremio connection" subtitle={cfg.sink_mode === 'dremio' ? 'Target for MERGE INTO writes' : 'Used for metadata refresh (optional when using Dremio Open Catalog)'}>
        <div style={S.grid2}>
          <Field label="Host">
            <input style={S.input} value={cfg.dremio.host ?? ''} onChange={e => updateDremio('host', e.target.value)} placeholder="localhost" />
          </Field>
          <Field label="Port">
            <input style={S.input} type="number" value={cfg.dremio.port ?? 9047} onChange={e => updateDremio('port', +e.target.value)} />
          </Field>
        </div>
        <div style={S.grid2}>
          <Field label="User">
            <input style={S.input} value={cfg.dremio.user ?? ''} onChange={e => updateDremio('user', e.target.value)} placeholder="admin" />
          </Field>
          <Field label="Password">
            <SecretFieldInput value={cfg.dremio.password ?? ''} onChange={v => updateDremio('password', v)} isPassword />
          </Field>
        </div>
        <Field label="Personal Access Token (PAT) — overrides user/password">
          <SecretFieldInput value={cfg.dremio.pat ?? ''} onChange={v => updateDremio('pat', v)} placeholder="For Dremio Cloud or Enterprise" isPassword />
        </Field>
        <Field label="Project ID (Dremio Cloud only)">
          <input style={S.input} value={(cfg.dremio as any).project_id ?? ''} onChange={e => updateDremio('project_id' as any, e.target.value)} placeholder="e.g. 957704f5-4495-42ad-94de-671bf7790610" />
          <div style={S.hint}>Required for Mode A with Dremio Cloud. Find it in your Dremio Cloud project URL.</div>
        </Field>
        {cfg.sink_mode === 'dremio' && (
          <Field label="Target namespace">
            <div ref={nsRef} style={{ position: 'relative' }}>
              <div style={{ display: 'flex', gap: 8 }}>
                <input style={{ ...S.input, flex: 1 }}
                  value={cfg.dremio.target_namespace ?? ''}
                  onChange={e => updateDremio('target_namespace', e.target.value)}
                  placeholder="cdc" />
                <button style={S.btnSecondary} onClick={handleBrowseNs} disabled={nsLoading} title="Browse Dremio namespaces">
                  {nsLoading ? <Loader size={13} /> : <ChevronDown size={13} />} Browse
                </button>
              </div>
              {nsOpen && namespaces && (
                <div style={S.nsDropdown}>
                  {namespaces.length === 0 && <div style={{ color: '#64748b', padding: '8px 12px', fontSize: 13 }}>No sources or spaces found</div>}
                  {namespaces.map(ns => (
                    <div key={ns.name} style={S.nsOption} onClick={() => { updateDremio('target_namespace', ns.name); setNsOpen(false) }}>
                      <span style={{ ...S.nsBadge, ...(ns.type === 'source' ? S.nsBadgeSource : S.nsBadgeSpace) }}>{ns.type}</span>
                      <span style={{ fontFamily: 'monospace', fontSize: 13, color: '#e2e8f0' }}>{ns.name}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div style={S.hint}>Dremio source where CDC tables are created. Must be Iceberg-capable. Click Browse to pick from your Dremio instance.</div>
          </Field>
        )}
        <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginTop: 4 }}>
          <button style={S.btnSecondary} onClick={handleTest} disabled={testing}>
            {testing ? <Loader size={13} /> : null} Test connection
          </button>
          {testResult && (
            <span style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: testResult.ok ? '#4ade80' : '#f87171' }}>
              {testResult.ok ? <CheckCircle size={14} /> : <AlertCircle size={14} />}
              {testResult.ok ? `Connected` : testResult.error}
            </span>
          )}
        </div>
      </Section>

      {/* Transform Studio integration */}
      <Section
        title="Transform Studio"
        subtitle="Trigger a downstream pipeline after each CDC batch lands in Dremio"
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
            <input
              type="checkbox"
              checked={cfg.transform_studio?.enabled ?? false}
              onChange={e => updateTS('enabled', e.target.checked)}
            />
            <span style={{ color: '#94a3b8', fontSize: 13 }}>Enable Transform Studio integration</span>
          </label>
        </div>
        {cfg.transform_studio?.enabled && (
          <>
            <div style={S.infoBox}>
              <Zap size={13} style={{ display: 'inline', marginRight: 6 }} />
              After each successful flush, CDC will POST to
              <code style={S.code}> {'{url}'}/api/pipelines/{'{pipeline_id}'}/run</code> to trigger your pipeline.
            </div>
            <Field label="Transform Studio URL">
              <input
                style={S.input}
                value={cfg.transform_studio?.url ?? ''}
                onChange={e => updateTS('url', e.target.value)}
                placeholder="http://localhost:5000"
              />
            </Field>
            <Field label="Pipeline ID">
              <input
                style={S.input}
                value={cfg.transform_studio?.pipeline_id ?? ''}
                onChange={e => updateTS('pipeline_id', e.target.value)}
                placeholder="Pipeline ID to trigger"
              />
            </Field>
            <Field label="API token (optional)">
              <SecretFieldInput value={cfg.transform_studio?.token ?? ''} onChange={v => updateTS('token', v)} placeholder="Bearer token for authentication" isPassword />
              <div style={S.hint}>Leave blank if Transform Studio is running locally without auth.</div>
            </Field>
          </>
        )}
      </Section>

      {/* Iceberg / Open Catalog config */}
      {cfg.sink_mode === 'iceberg' && (
        <Section title="Iceberg catalog" subtitle="Dremio Open Catalog, standalone Polaris, Nessie, or Glue">
          <div style={S.infoBox}>
            <strong>Dremio Open Catalog (recommended)</strong><br />
            <span style={{ color: '#94a3b8' }}>Cloud: </span><code style={S.code}>https://catalog.dremio.cloud/api/iceberg</code><br />
            <span style={{ color: '#94a3b8' }}>Enterprise: </span><code style={S.code}>http://&lt;host&gt;:8181/api/catalog</code>
          </div>

          <div style={S.grid2}>
            <Field label="Catalog type">
              <select style={S.input} value={cfg.iceberg.type ?? 'rest'} onChange={e => updateIceberg('type', e.target.value)}>
                <option value="rest">REST (Polaris / Open Catalog)</option>
                <option value="nessie">Nessie</option>
                <option value="glue">AWS Glue</option>
                <option value="hive">Hive Metastore</option>
              </select>
            </Field>
            <Field label="Write mode">
              <select style={S.input} value={cfg.iceberg.write_mode ?? 'merge'} onChange={e => updateIceberg('write_mode', e.target.value)}>
                <option value="merge">Merge (upsert — current state)</option>
                <option value="append">Append (full event history)</option>
              </select>
            </Field>
          </div>

          <Field label="Catalog URI">
            <input style={S.input} value={(cfg.iceberg.uri as string) ?? ''} onChange={e => updateIceberg('uri', e.target.value)} placeholder="https://catalog.dremio.cloud/api/iceberg" />
          </Field>
          <Field label="Warehouse / project name">
            <input style={S.input} value={(cfg.iceberg.warehouse as string) ?? ''} onChange={e => updateIceberg('warehouse', e.target.value)} placeholder="my-project-name" />
          </Field>
          <Field label="Token (PAT — for Dremio Open Catalog)">
            <SecretFieldInput value={(cfg.iceberg.token as string) ?? ''} onChange={v => updateIceberg('token', v)} placeholder="Dremio Personal Access Token" isPassword />
            <div style={S.hint}>Use your Dremio PAT. Credential vending is handled automatically by Open Catalog — no S3 keys needed.</div>
          </Field>
          <Field label="Target namespace">
            <input style={S.input} value={(cfg.iceberg.target_namespace as string) ?? 'cdc'} onChange={e => updateIceberg('target_namespace', e.target.value)} />
          </Field>

          <details style={{ marginTop: 8 }}>
            <summary style={{ color: '#64748b', fontSize: 13, cursor: 'pointer' }}>
              Non-Dremio catalog (static S3 credentials)
            </summary>
            <div style={{ paddingTop: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div style={S.grid2}>
                <Field label="S3 endpoint">
                  <input style={S.input} value={(cfg.iceberg['s3.endpoint'] as string) ?? ''} onChange={e => updateIceberg('s3.endpoint', e.target.value)} placeholder="http://localhost:9000" />
                </Field>
                <Field label="S3 access key ID">
                  <input style={S.input} value={(cfg.iceberg['s3.access-key-id'] as string) ?? ''} onChange={e => updateIceberg('s3.access-key-id', e.target.value)} />
                </Field>
              </div>
              <Field label="S3 secret access key">
                <SecretFieldInput value={(cfg.iceberg['s3.secret-access-key'] as string) ?? ''} onChange={v => updateIceberg('s3.secret-access-key', v)} isPassword />
              </Field>
            </div>
          </details>
        </Section>
      )}
    </div>
  )
}

function ModeCard({ active, icon, title, description, onClick }: {
  active: boolean; icon: React.ReactNode; title: string; description: string; onClick: () => void
}) {
  return (
    <div style={{ ...S.modeCard, ...(active ? S.modeCardActive : {}) }} onClick={onClick}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
        {icon}
        <strong style={{ color: active ? '#f1f5f9' : '#94a3b8', fontSize: 14 }}>{title}</strong>
        {active && <span style={{ marginLeft: 'auto', fontSize: 11, background: '#1e3a5f', color: '#60a5fa', padding: '2px 8px', borderRadius: 99 }}>selected</span>}
      </div>
      <p style={{ color: '#64748b', fontSize: 13, lineHeight: 1.5 }}>{description}</p>
    </div>
  )
}

function Section({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <div style={S.section}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={S.sectionTitle}>{title}</h2>
        {subtitle && <p style={{ color: '#64748b', fontSize: 13, marginTop: 4 }}>{subtitle}</p>}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>{children}</div>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label style={S.label}>{label}</label>
      {children}
    </div>
  )
}

// Sources that require Mode B (Iceberg) — Mode A won't work correctly with these
const MODE_B_REQUIRED: Record<string, string> = {
  pubsub:     'Pub/Sub is a high-throughput streaming source — Mode A (SQL MERGE) is too slow and misses sort_by clustering and _cdc_ingest_ts metadata. Use Mode B (Open Catalog).',
  spanner:    'Spanner Change Streams produce continuous high-volume events. Mode B (Open Catalog) is strongly recommended for throughput and schema evolution.',
  datastream: 'Datastream GCS files are polled continuously — Mode B (Open Catalog / PyIceberg) is required for correct offset tracking and schema evolution.',
}

function CompatWarnings({ sources, sinkMode }: { sources: Source[]; sinkMode: string }) {
  if (sinkMode !== 'dremio') return null
  const warnings = sources
    .filter(s => MODE_B_REQUIRED[s.type])
    .map(s => ({ name: s.name, type: s.type, msg: MODE_B_REQUIRED[s.type] }))
  if (warnings.length === 0) return null
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 20 }}>
      {warnings.map(w => (
        <div key={w.name} style={{ display: 'flex', gap: 10, alignItems: 'flex-start', background: '#2d1a0a', border: '1px solid #92400e', borderRadius: 8, padding: '12px 16px' }}>
          <AlertCircle size={15} color="#fb923c" style={{ flexShrink: 0, marginTop: 1 }} />
          <div style={{ fontSize: 13, color: '#fdba74', lineHeight: 1.6 }}>
            <strong style={{ color: '#fb923c' }}>{w.name}</strong> ({w.type}) — {w.msg}
          </div>
        </div>
      ))}
    </div>
  )
}

const S: Record<string, React.CSSProperties> = {
  page: { padding: 32, maxWidth: 780 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 },
  title: { fontSize: 22, fontWeight: 700, color: '#f1f5f9' },
  subtitle: { color: '#64748b', fontSize: 13, marginTop: 4 },
  modeRow: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 28 },
  modeCard: { background: '#1e293b', border: '2px solid #334155', borderRadius: 10, padding: 18, cursor: 'pointer', transition: 'all 0.15s' },
  modeCardActive: { border: '2px solid #2563eb', background: '#0f1f3d' },
  section: { background: '#1e293b', border: '1px solid #334155', borderRadius: 10, padding: 24, marginBottom: 20 },
  sectionTitle: { fontSize: 15, fontWeight: 700, color: '#f1f5f9' },
  grid2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 },
  label: { display: 'block', color: '#94a3b8', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 },
  input: { width: '100%', background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '8px 12px', color: '#e2e8f0', fontSize: 13, outline: 'none' },
  hint: { color: '#64748b', fontSize: 11, marginTop: 6, lineHeight: 1.5 },
  infoBox: { background: '#0f172a', border: '1px solid #1e3a5f', borderRadius: 8, padding: '12px 16px', fontSize: 13, color: '#93c5fd', lineHeight: 1.8, marginBottom: 4 },
  code: { fontFamily: 'monospace', fontSize: 12, color: '#7dd3fc' },
  btnPrimary: { display: 'flex', alignItems: 'center', gap: 6, background: '#2563eb', color: '#fff', border: 'none', padding: '8px 18px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
  btnSecondary: { display: 'flex', alignItems: 'center', gap: 6, background: '#0f172a', color: '#94a3b8', border: '1px solid #334155', padding: '8px 14px', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
  nsDropdown: { position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 50, background: '#1e293b', border: '1px solid #334155', borderRadius: 8, marginTop: 4, boxShadow: '0 8px 24px rgba(0,0,0,0.4)', maxHeight: 240, overflowY: 'auto' },
  nsOption: { display: 'flex', alignItems: 'center', gap: 10, padding: '8px 12px', cursor: 'pointer', borderBottom: '1px solid #0f172a' },
  nsBadge: { fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 3, textTransform: 'uppercase' as const },
  nsBadgeSource: { background: '#1e3a5f', color: '#60a5fa' },
  nsBadgeSpace: { background: '#1a2e1a', color: '#86efac' },
}
