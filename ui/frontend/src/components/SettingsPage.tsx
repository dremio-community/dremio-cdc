import { useEffect, useState } from 'react'
import { CheckCircle, Loader, Save, XCircle } from 'lucide-react'
import { getSettings, saveSettings, Settings, getSecrets, saveSecrets, testVault, SecretsConfig, VaultConfig } from '../api/client'

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

  // Secrets state
  const [secretsProvider, setSecretsProvider] = useState<'none' | 'env' | 'vault'>('none')
  const [vault, setVault] = useState<VaultConfig>({ auth_method: 'token', mount: 'secret' })
  const [vaultTesting, setVaultTesting] = useState(false)
  const [vaultTestResult, setVaultTestResult] = useState<{ ok: boolean; error?: string } | null>(null)
  const [secretsSaving, setSecretsSaving] = useState(false)
  const [secretsSaved, setSecretsSaved] = useState(false)

  useEffect(() => { getSettings().then(d => setS(prev => ({ ...prev, ...d }))).catch(() => {}) }, [])
  useEffect(() => {
    getSecrets().then(d => {
      if (d?.vault) { setSecretsProvider('vault'); setVault(prev => ({ ...prev, ...d.vault })) }
    }).catch(() => {})
  }, [])

  const set = (k: keyof Settings, v: unknown) => setS(c => ({ ...c, [k]: v }))
  const setV = (k: keyof VaultConfig, v: unknown) => {
    setVault(c => ({ ...c, [k]: v }))
    setVaultTestResult(null)
  }

  const handleSave = async () => {
    setSaving(true)
    try { await saveSettings(s); setSaved(true); setTimeout(() => setSaved(false), 2000) }
    catch {}
    setSaving(false)
  }

  const handleSecretsSave = async () => {
    setSecretsSaving(true)
    try {
      const payload: SecretsConfig = secretsProvider === 'vault' ? { vault } : {}
      await saveSecrets(payload)
      setSecretsSaved(true)
      setTimeout(() => setSecretsSaved(false), 2000)
    } catch {}
    setSecretsSaving(false)
  }

  const handleVaultTest = async () => {
    setVaultTesting(true)
    setVaultTestResult(null)
    try {
      const result = await testVault(vault)
      setVaultTestResult(result)
    } catch (e: unknown) {
      setVaultTestResult({ ok: false, error: e instanceof Error ? e.message : 'Connection failed' })
    }
    setVaultTesting(false)
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
            <div style={{ color: 'var(--foreground)', fontWeight: 500 }}>Adaptive batching</div>
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
            <div style={{ color: 'var(--foreground)', fontWeight: 500 }}>Snapshot on first run</div>
            <div style={S.hint}>When enabled, the daemon reads every row in each table before switching to streaming mode. Disable if the table is already pre-populated in Dremio.</div>
          </div>
        </label>

        <label style={S.checkRow}>
          <input type="checkbox" checked={s.incremental_snapshot ?? false}
            onChange={e => set('incremental_snapshot', e.target.checked)} />
          <div>
            <div style={{ color: 'var(--foreground)', fontWeight: 500 }}>Incremental snapshot</div>
            <div style={S.hint}>Read the table in chunks using a cursor column (e.g. <code style={{ color: 'var(--accent)' }}>id</code>) instead of a single full scan. Streaming starts sooner and restarts resume mid-table. Recommended for tables with millions of rows.</div>
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
            SQLite: <code style={{ color: 'var(--accent)' }}>./cdc_offsets.db</code>&nbsp;&nbsp;·&nbsp;&nbsp;
            PostgreSQL: <code style={{ color: 'var(--accent)' }}>postgresql://user:pw@host/db</code>
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

      {/* ── Secrets management ── */}
      <div style={S.card}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <h2 style={S.sectionTitle}>Secrets management</h2>
            <p style={S.sectionSub}>Store credentials outside of config.yml using environment variables or HashiCorp Vault</p>
          </div>
          <button style={S.btnPrimary} onClick={handleSecretsSave} disabled={secretsSaving}>
            {secretsSaving ? <Loader size={13} /> : <Save size={13} />}
            {secretsSaved ? 'Saved!' : 'Save'}
          </button>
        </div>

        <div>
          <label style={S.label}>Secrets provider</label>
          <select style={S.input} value={secretsProvider}
            onChange={e => { setSecretsProvider(e.target.value as 'none' | 'env' | 'vault'); setVaultTestResult(null) }}>
            <option value="none">None — credentials stored in config.yml</option>
            <option value="env">Environment variables only (no extra config needed)</option>
            <option value="vault">HashiCorp Vault</option>
          </select>
          {secretsProvider === 'env' && (
            <div style={S.hint}>
              Use <code style={{ color: 'var(--accent)' }}>${'{'}ENV_VAR{'}'}</code> anywhere in config.yml — including inline, e.g.&nbsp;
              <code style={{ color: 'var(--accent)' }}>jdbc://${'{'}DB_HOST{'}'}/mydb</code>. No additional configuration required.
            </div>
          )}
        </div>

        {secretsProvider === 'vault' && (<>
          <div style={S.grid2}>
            <div>
              <label style={S.label}>Vault URL</label>
              <input style={S.input} value={vault.url ?? ''} placeholder="https://vault.example.com"
                onChange={e => setV('url', e.target.value)} />
              <div style={S.hint}>Or set <code style={{ color: 'var(--accent)' }}>VAULT_ADDR</code> env var.</div>
            </div>
            <div>
              <label style={S.label}>KV mount point</label>
              <input style={S.input} value={vault.mount ?? 'secret'} placeholder="secret"
                onChange={e => setV('mount', e.target.value)} />
              <div style={S.hint}>KV v2 mount — default is <code style={{ color: 'var(--accent)' }}>secret</code>.</div>
            </div>
          </div>

          <div>
            <label style={S.label}>Authentication method</label>
            <select style={S.input} value={vault.auth_method ?? 'token'}
              onChange={e => setV('auth_method', e.target.value as 'token' | 'approle')}>
              <option value="token">Token</option>
              <option value="approle">AppRole (recommended for production)</option>
            </select>
          </div>

          {(vault.auth_method ?? 'token') === 'token' ? (
            <div>
              <label style={S.label}>Vault token</label>
              <input style={{ ...S.input, fontFamily: 'monospace' }} type="password"
                value={vault.token ?? ''} placeholder="hvs.xxxxxx  (or use ${VAULT_TOKEN})"
                onChange={e => setV('token', e.target.value)} />
              <div style={S.hint}>Tip: use <code style={{ color: 'var(--accent)' }}>${'{'}VAULT_TOKEN{'}'}</code> to read from an env var instead of hardcoding.</div>
            </div>
          ) : (<>
            <div style={S.grid2}>
              <div>
                <label style={S.label}>Role ID</label>
                <input style={{ ...S.input, fontFamily: 'monospace' }}
                  value={vault.role_id ?? ''} placeholder="${VAULT_ROLE_ID}"
                  onChange={e => setV('role_id', e.target.value)} />
              </div>
              <div>
                <label style={S.label}>Secret ID</label>
                <input style={{ ...S.input, fontFamily: 'monospace' }} type="password"
                  value={vault.secret_id ?? ''} placeholder="${VAULT_SECRET_ID}"
                  onChange={e => setV('secret_id', e.target.value)} />
              </div>
            </div>
            <div style={S.hint}>AppRole credentials can themselves reference env vars, e.g. <code style={{ color: 'var(--accent)' }}>${'{'}VAULT_SECRET_ID{'}'}</code>.</div>
          </>)}

          <div>
            <label style={S.label}>Vault namespace <span style={{ color: 'var(--muted-foreground)', fontWeight: 400 }}>(Enterprise only)</span></label>
            <input style={S.input} value={vault.namespace ?? ''} placeholder="Optional — leave blank for open-source Vault"
              onChange={e => setV('namespace', e.target.value)} />
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <button style={S.btnSecondary} onClick={handleVaultTest} disabled={vaultTesting}>
              {vaultTesting ? <Loader size={13} /> : null}
              {vaultTesting ? 'Testing…' : 'Test connection'}
            </button>
            {vaultTestResult && (
              <span style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13,
                color: vaultTestResult.ok ? 'var(--status-success)' : 'var(--status-error)' }}>
                {vaultTestResult.ok
                  ? <><CheckCircle size={14} /> Connected successfully</>
                  : <><XCircle size={14} /> {vaultTestResult.error}</>}
              </span>
            )}
          </div>

          <div style={{ ...S.hint, borderTop: '1px solid var(--border)', paddingTop: 12 }}>
            Once configured, reference secrets in config.yml as&nbsp;
            <code style={{ color: 'var(--accent)' }}>vault:secret/path#field</code>, e.g.&nbsp;
            <code style={{ color: 'var(--accent)' }}>vault:prod/postgres#password</code>
          </div>
        </>)}
      </div>
    </div>
  )
}

const S: Record<string, React.CSSProperties> = {
  page: { padding: 32, maxWidth: 700 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 },
  title: { fontSize: 22, fontWeight: 700, color: 'var(--foreground)' },
  subtitle: { color: 'var(--secondary-foreground)', fontSize: 13, marginTop: 4 },
  card: { background: 'var(--card)', border: '1px solid var(--border)', borderRadius: 10, padding: 24, marginBottom: 16, display: 'flex', flexDirection: 'column', gap: 16 },
  sectionTitle: { fontSize: 15, fontWeight: 700, color: 'var(--foreground)' },
  sectionSub: { color: 'var(--secondary-foreground)', fontSize: 13, marginTop: -8 },
  grid2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 },
  label: { display: 'block', color: 'var(--secondary-foreground)', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6 },
  input: { width: '100%', background: '#fff', border: '1px solid var(--border)', borderRadius: 4, padding: '7px 10px', color: 'var(--foreground)', fontSize: 13, outline: 'none' },
  hint: { color: 'var(--secondary-foreground)', fontSize: 11, marginTop: 6, lineHeight: 1.5 },
  checkRow: { display: 'flex', gap: 12, cursor: 'pointer', alignItems: 'flex-start' },
  btnPrimary: { display: 'flex', alignItems: 'center', gap: 6, background: 'var(--primary)', color: '#fff', border: 'none', padding: '8px 18px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
  btnSecondary: { display: 'flex', alignItems: 'center', gap: 6, background: 'transparent', color: 'var(--secondary-foreground)', border: '1px solid var(--border)', padding: '7px 16px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
}
