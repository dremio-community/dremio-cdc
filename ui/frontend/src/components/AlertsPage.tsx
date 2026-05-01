import { useEffect, useState } from 'react'
import { AlertTriangle, Bell, BellOff, Loader, Mail, Plus, Save, Trash2, Webhook, X } from 'lucide-react'
import { getAlerts, saveAlerts, AlertConfig, AlertChannel, AlertRecord } from '../api/client'
import SecretFieldInput from './SecretFieldInput'

const EMPTY_CONFIG: AlertConfig = {
  enabled: true,
  lag_threshold_seconds: 60,
  error_count_threshold: 5,
  cooldown_seconds: 300,
  check_interval_seconds: 30,
  channels: [],
}

export default function AlertsPage() {
  const [cfg, setCfg] = useState<AlertConfig>(EMPTY_CONFIG)
  const [recent, setRecent] = useState<AlertRecord[]>([])
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [error, setError] = useState('')
  const [addingType, setAddingType] = useState<AlertChannel['type'] | null>(null)

  const load = async () => {
    try {
      const d = await getAlerts()
      setCfg({ ...EMPTY_CONFIG, ...d.config, channels: d.config.channels ?? [] })
      setRecent(d.recent ?? [])
    } catch (e: any) {
      setError(e.message)
    }
  }

  useEffect(() => {
    load()
    const id = setInterval(() => getAlerts().then(d => setRecent(d.recent ?? [])).catch(() => {}), 10000)
    return () => clearInterval(id)
  }, [])

  const set = <K extends keyof AlertConfig>(k: K, v: AlertConfig[K]) =>
    setCfg(c => ({ ...c, [k]: v }))

  const handleSave = async () => {
    setSaving(true)
    try {
      await saveAlerts(cfg)
      setSaved(true)
      setTimeout(() => setSaved(false), 2000)
    } catch (e: any) {
      setError(e.message)
    }
    setSaving(false)
  }

  const removeChannel = (i: number) =>
    setCfg(c => ({ ...c, channels: (c.channels ?? []).filter((_, idx) => idx !== i) }))

  const addChannel = (ch: AlertChannel) => {
    setCfg(c => ({ ...c, channels: [...(c.channels ?? []), ch] }))
    setAddingType(null)
  }

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Alerts</h1>
          <p style={S.subtitle}>Notify when lag or errors exceed thresholds</p>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          {error && <span style={{ color: 'var(--status-error)', fontSize: 12 }}>{error}</span>}
          <button style={S.btnPrimary} onClick={handleSave} disabled={saving}>
            {saving ? <Loader size={13} /> : <Save size={13} />}
            {saved ? 'Saved!' : 'Save'}
          </button>
        </div>
      </div>

      {/* Enable / disable */}
      <div style={S.card}>
        <label style={S.checkRow}>
          <input type="checkbox" checked={cfg.enabled ?? true}
            onChange={e => set('enabled', e.target.checked)} />
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: 'var(--foreground)', fontWeight: 600 }}>
              {cfg.enabled ? <Bell size={14} color="var(--primary)" /> : <BellOff size={14} color="var(--muted-foreground)" />}
              Alerts enabled
            </div>
            <div style={S.hint}>When disabled, thresholds are still computed but no notifications are sent.</div>
          </div>
        </label>
      </div>

      {/* Thresholds */}
      <div style={S.card}>
        <h2 style={S.sectionTitle}>Thresholds</h2>
        <div style={S.grid3}>
          <div>
            <label style={S.label}>Lag threshold (seconds)</label>
            <input style={S.input} type="number" value={cfg.lag_threshold_seconds ?? 60}
              onChange={e => set('lag_threshold_seconds', +e.target.value)} />
            <div style={S.hint}>Fire if a worker's lag exceeds this.</div>
          </div>
          <div>
            <label style={S.label}>Error count threshold</label>
            <input style={S.input} type="number" value={cfg.error_count_threshold ?? 5}
              onChange={e => set('error_count_threshold', +e.target.value)} />
            <div style={S.hint}>Fire if a worker's flush error count reaches this.</div>
          </div>
          <div>
            <label style={S.label}>Cooldown (seconds)</label>
            <input style={S.input} type="number" value={cfg.cooldown_seconds ?? 300}
              onChange={e => set('cooldown_seconds', +e.target.value)} />
            <div style={S.hint}>Minimum time between repeat alerts for the same worker.</div>
          </div>
        </div>
        <div style={{ maxWidth: 260 }}>
          <label style={S.label}>Check interval (seconds)</label>
          <input style={S.input} type="number" value={cfg.check_interval_seconds ?? 30}
            onChange={e => set('check_interval_seconds', +e.target.value)} />
          <div style={S.hint}>How often the alert engine polls StatusStore.</div>
        </div>
      </div>

      {/* Channels */}
      <div style={S.card}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h2 style={S.sectionTitle}>Notification channels</h2>
            <p style={S.sectionSub}>Where to send alerts when thresholds fire</p>
          </div>
          {!addingType && (
            <div style={{ display: 'flex', gap: 8 }}>
              <button style={S.btnAdd} onClick={() => setAddingType('slack')}>+ Slack</button>
              <button style={S.btnAdd} onClick={() => setAddingType('webhook')}>+ Webhook</button>
              <button style={S.btnAdd} onClick={() => setAddingType('email')}>+ Email</button>
            </div>
          )}
        </div>

        {addingType && (
          <ChannelForm type={addingType} onAdd={addChannel} onCancel={() => setAddingType(null)} />
        )}

        {(cfg.channels ?? []).length === 0 && !addingType ? (
          <div style={S.empty}>No channels configured — add one above to receive notifications.</div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {(cfg.channels ?? []).map((ch, i) => (
              <ChannelRow key={i} ch={ch} onRemove={() => removeChannel(i)} />
            ))}
          </div>
        )}
      </div>

      {/* Recent alerts */}
      <div style={S.card}>
        <h2 style={S.sectionTitle}>Recent alerts</h2>
        {recent.length === 0 ? (
          <div style={S.empty}>No alerts fired recently.</div>
        ) : (
          <table style={S.table}>
            <thead>
              <tr>
                {['Time', 'Type', 'Source / Table', 'Message'].map(h => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...recent].reverse().map((r, i) => (
                <tr key={i} style={i % 2 === 0 ? S.trEven : {}}>
                  <td style={S.td}>{new Date(r.time * 1000).toLocaleTimeString()}</td>
                  <td style={S.td}><AlertTypeBadge type={r.type} /></td>
                  <td style={{ ...S.td, fontFamily: 'monospace' }}>{r.source}/{r.table}</td>
                  <td style={S.td}>{r.message}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

// ── Channel form ──────────────────────────────────────────────────────────────

function ChannelForm({ type, onAdd, onCancel }: {
  type: AlertChannel['type']
  onAdd: (ch: AlertChannel) => void
  onCancel: () => void
}) {
  const [f, setF] = useState<Record<string, string | boolean | number>>({ type })
  const set = (k: string, v: string | boolean | number) => setF(c => ({ ...c, [k]: v }))

  const submit = () => onAdd(f as unknown as AlertChannel)

  return (
    <div style={S.formBox}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
        <span style={{ color: 'var(--foreground)', fontWeight: 600, fontSize: 13 }}>
          New {type} channel
        </span>
        <button style={S.btnIcon} onClick={onCancel}><X size={14} /></button>
      </div>

      {type === 'slack' && (
        <Field label="Webhook URL" hint="https://hooks.slack.com/services/...">
          <SecretFieldInput
            value={(f.webhook_url as string) ?? ''}
            onChange={v => set('webhook_url', v)}
            placeholder="https://hooks.slack.com/services/..."
            isPassword={false}
          />
        </Field>
      )}

      {type === 'webhook' && (
        <div style={S.grid2}>
          <Field label="URL">
            <SecretFieldInput
              value={(f.url as string) ?? ''}
              onChange={v => set('url', v)}
              placeholder="https://my-endpoint.example.com/hook"
              isPassword={false}
            />
          </Field>
          <Field label="HTTP method">
            <select style={S.input} value={(f.method as string) ?? 'post'}
              onChange={e => set('method', e.target.value)}>
              <option value="post">POST</option>
              <option value="put">PUT</option>
            </select>
          </Field>
        </div>
      )}

      {type === 'email' && (
        <>
          <div style={S.grid2}>
            <Field label="From"><input style={S.input} value={(f.from as string) ?? ''}
              onChange={e => set('from', e.target.value)} placeholder="alerts@company.com" /></Field>
            <Field label="To"><input style={S.input} value={(f.to as string) ?? ''}
              onChange={e => set('to', e.target.value)} placeholder="oncall@company.com" /></Field>
          </div>
          <div style={S.grid3}>
            <Field label="SMTP host"><input style={S.input} value={(f.smtp_host as string) ?? ''}
              onChange={e => set('smtp_host', e.target.value)} placeholder="smtp.gmail.com" /></Field>
            <Field label="SMTP port"><input style={S.input} type="number" value={(f.smtp_port as number) ?? 587}
              onChange={e => set('smtp_port', +e.target.value)} /></Field>
            <Field label="SMTP user"><input style={S.input} value={(f.smtp_user as string) ?? ''}
              onChange={e => set('smtp_user', e.target.value)} placeholder="me@company.com" /></Field>
          </div>
          <div style={S.grid2}>
            <Field label="SMTP password">
              <SecretFieldInput value={(f.smtp_password as string) ?? ''} onChange={v => set('smtp_password', v)} isPassword />
            </Field>
            <Field label="">
              <label style={S.checkRow}>
                <input type="checkbox" checked={!!f.smtp_tls}
                  onChange={e => set('smtp_tls', e.target.checked)} />
                <span style={{ color: 'var(--foreground)' }}>Enable STARTTLS</span>
              </label>
            </Field>
          </div>
        </>
      )}

      <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
        <button style={S.btnPrimary} onClick={submit}><Plus size={13} /> Add channel</button>
        <button style={S.btnSecondary} onClick={onCancel}>Cancel</button>
      </div>
    </div>
  )
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div>
      {label && <label style={S.label}>{label}</label>}
      {children}
      {hint && <div style={S.hint}>{hint}</div>}
    </div>
  )
}

function ChannelRow({ ch, onRemove }: { ch: AlertChannel; onRemove: () => void }) {
  const icon = ch.type === 'slack' ? <Bell size={13} color="var(--primary)" />
    : ch.type === 'email' ? <Mail size={13} color="var(--accent)" />
    : <Webhook size={13} color="var(--status-success)" />

  const summary = ch.type === 'slack' ? (ch.webhook_url ?? '').slice(0, 50) + '…'
    : ch.type === 'webhook' ? `${(ch.method ?? 'POST').toUpperCase()} ${ch.url ?? ''}`
    : `${ch.from} → ${ch.to}`

  return (
    <div style={S.channelRow}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        {icon}
        <span style={{ color: 'var(--secondary-foreground)', fontSize: 12, fontWeight: 600, textTransform: 'uppercase' }}>{ch.type}</span>
        <span style={{ color: 'var(--muted-foreground)', fontSize: 12, fontFamily: 'monospace' }}>{summary}</span>
      </div>
      <button style={S.btnIcon} onClick={onRemove}><Trash2 size={13} color="var(--destructive)" /></button>
    </div>
  )
}

function AlertTypeBadge({ type }: { type: string }) {
  const colors: Record<string, { color: string; bg: string }> = {
    lag:          { color: 'var(--status-warning)', bg: 'var(--status-warning-bg)' },
    errors:       { color: 'var(--status-error)', bg: 'var(--status-error-bg)' },
    worker_error: { color: 'var(--status-error)', bg: 'var(--status-error-bg)' },
  }
  const s = colors[type] ?? { color: 'var(--secondary-foreground)', bg: 'var(--muted)' }
  return (
    <span style={{ fontSize: 11, fontWeight: 600, padding: '2px 7px', borderRadius: 99, color: s.color, background: s.bg }}>
      {type}
    </span>
  )
}

const S: Record<string, React.CSSProperties> = {
  page: { padding: 32, maxWidth: 860 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 },
  title: { fontSize: 22, fontWeight: 700, color: 'var(--foreground)' },
  subtitle: { color: 'var(--secondary-foreground)', fontSize: 13, marginTop: 4 },
  card: { background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 10, padding: 24, marginBottom: 16, display: 'flex', flexDirection: 'column', gap: 16 },
  sectionTitle: { fontSize: 15, fontWeight: 700, color: 'var(--foreground)' },
  sectionSub: { color: 'var(--secondary-foreground)', fontSize: 13, marginTop: -8 },
  grid2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 },
  grid3: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 },
  label: { display: 'block', color: 'var(--secondary-foreground)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 },
  input: { width: '100%', background: '#fff', border: '1px solid var(--border)', borderRadius: 4, padding: '7px 10px', color: 'var(--foreground)', fontSize: 13, outline: 'none' },
  hint: { color: 'var(--secondary-foreground)', fontSize: 11, marginTop: 6, lineHeight: 1.5 },
  checkRow: { display: 'flex', gap: 12, cursor: 'pointer', alignItems: 'flex-start' },
  empty: { color: 'var(--secondary-foreground)', fontSize: 13, padding: '12px 0' },
  formBox: { background: 'var(--muted)', border: '1px solid var(--border)', borderRadius: 8, padding: 18, display: 'flex', flexDirection: 'column', gap: 14 },
  channelRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: 'var(--muted)', border: '1px solid var(--border)', borderRadius: 6, padding: '10px 14px' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left' as const, color: 'var(--muted-foreground)', fontWeight: 600, padding: '6px 10px', borderBottom: '1px solid var(--border)', fontSize: 11, textTransform: 'uppercase' as const, letterSpacing: '0.04em', background: 'var(--muted)' },
  td: { padding: '7px 10px', color: 'var(--secondary-foreground)', verticalAlign: 'top' as const },
  trEven: { background: 'var(--muted)' },
  btnPrimary: { display: 'flex', alignItems: 'center', gap: 6, background: 'var(--primary)', color: '#fff', border: 'none', padding: '8px 18px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
  btnSecondary: { display: 'flex', alignItems: 'center', gap: 6, background: 'transparent', color: 'var(--secondary-foreground)', border: '1px solid var(--border)', padding: '8px 14px', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
  btnAdd: { background: 'transparent', color: 'var(--secondary-foreground)', border: '1px solid var(--border)', padding: '6px 12px', borderRadius: 6, cursor: 'pointer', fontSize: 12 },
  btnIcon: { background: 'none', border: 'none', cursor: 'pointer', color: 'var(--muted-foreground)', display: 'flex', alignItems: 'center', padding: 4 },
}
