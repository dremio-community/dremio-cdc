import { useEffect, useState } from 'react'
import { AlertCircle, CheckCircle, ChevronDown, ChevronRight, Columns, Database, Loader, Plus, Table, Trash2, X } from 'lucide-react'
import { getSources, addSource, updateSource, deleteSource, testSource, createSourceTables, Source, CreateTableResult } from '../api/client'
import SecretFieldInput from './SecretFieldInput'

const MASK_FNS = [
  '', 'redact', 'hash_sha256', 'hash_md5', 'mask', 'nullify', 'tokenize',
  'mask_email', 'mask_phone', 'mask_ssn', 'mask_card', 'mask_ip', 'mask_name',
] as const
const MASK_LABELS: Record<string, string> = {
  '':          '— none —',
  redact:      'Redact',
  hash_sha256: 'SHA-256 hash',
  hash_md5:    'MD5 hash',
  mask:        'Mask (***)',
  nullify:     'Nullify (NULL)',
  tokenize:    'Tokenize',
  mask_email:  'Email  (a***@domain)',
  mask_phone:  'Phone  (***-***-1234)',
  mask_ssn:    'SSN    (***-**-6789)',
  mask_card:   'Card   (****-1234)',
  mask_ip:     'IP     (1.2.*.*)',
  mask_name:   'Name   (J***)',
}

const SOURCE_TYPES = ['postgres', 'mysql', 'mongodb', 'dynamodb', 'sqlserver', 'snowflake', 'cockroachdb', 'oracle', 'db2', 'debezium'] as const

export default function SourcesPage() {
  const [sources, setSources] = useState<Source[]>([])
  const [showAdd, setShowAdd] = useState(false)
  const [editSource, setEditSource] = useState<Source | null>(null)
  const [msg, setMsg] = useState('')

  const load = async () => setSources(await getSources())
  useEffect(() => { load() }, [])

  const handleDelete = async (name: string) => {
    if (!confirm(`Delete source "${name}"?`)) return
    await deleteSource(name)
    setMsg(`Deleted ${name}`)
    load()
    setTimeout(() => setMsg(''), 3000)
  }

  return (
    <div style={S.page}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Sources</h1>
          <p style={S.subtitle}>Database connections to replicate from</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          {msg && <span style={{ color: '#94a3b8', fontSize: 13 }}>{msg}</span>}
          <button style={S.btnPrimary} onClick={() => { setEditSource(null); setShowAdd(true) }}>
            <Plus size={15} /> Add source
          </button>
        </div>
      </div>

      {sources.length === 0 ? (
        <div style={S.empty}>
          <Database size={36} color="#334155" />
          <p>No sources configured yet.</p>
          <button style={S.btnPrimary} onClick={() => setShowAdd(true)}><Plus size={14} /> Add your first source</button>
        </div>
      ) : (
        <div style={S.list}>
          {sources.map(src => (
            <SourceCard
              key={src.name} source={src}
              onEdit={() => { setEditSource(src); setShowAdd(true) }}
              onDelete={() => handleDelete(src.name)}
            />
          ))}
        </div>
      )}

      {showAdd && (
        <SourceModal
          initial={editSource}
          onClose={() => { setShowAdd(false); setEditSource(null) }}
          onSaved={() => { setShowAdd(false); setEditSource(null); load() }}
        />
      )}
    </div>
  )
}

function SourceCard({ source, onEdit, onDelete }: { source: Source; onEdit: () => void; onDelete: () => void }) {
  const [expanded, setExpanded] = useState(false)
  const [expandedTable, setExpandedTable] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)

  return (
    <div style={S.card}>
      {showCreate && (
        <CreateTablesModal source={source} onClose={() => setShowCreate(false)} />
      )}
      <div style={S.cardRow}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ ...S.typeBadge, ...typeColor(source.type) }}>{source.type}</span>
          <span style={S.cardName}>{source.name}</span>
          {source.type === 'debezium' && source.listen_port && (
            <span style={{ ...S.tableCount, color: '#fdba74' }}>port {source.listen_port}</span>
          )}
          <span style={S.tableCount}>{source.tables?.length ?? 0} tables</span>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button style={S.btnSmall} onClick={() => setExpanded(!expanded)}>
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </button>
          <button style={{ ...S.btnSmall, color: '#93c5fd' }} onClick={() => setShowCreate(true)} title="Create tables in Dremio">
            <Table size={13} /> Create in Dremio
          </button>
          <button style={S.btnSmall} onClick={onEdit}>Edit</button>
          <button style={{ ...S.btnSmall, color: '#f87171' }} onClick={onDelete}><Trash2 size={13} /></button>
        </div>
      </div>
      {expanded && (
        <div style={{ marginTop: 12 }}>
          {source.tables?.map(t => {
            const cols = source.columns?.[t]
            const isExpanded = expandedTable === t
            return (
              <div key={t} style={{ marginBottom: 4 }}>
                <div
                  style={S.treeTableRow}
                  onClick={() => setExpandedTable(isExpanded ? null : t)}
                >
                  {cols?.length ? <ChevronRight size={12} style={{ transform: isExpanded ? 'rotate(90deg)' : 'none', transition: '0.15s' }} /> : <span style={{ width: 12 }} />}
                  <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#94a3b8' }}>{t}</span>
                  {cols?.length ? (
                    <span style={S.colBadge}><Columns size={10} /> {cols.length} cols</span>
                  ) : (
                    <span style={{ ...S.colBadge, color: '#475569' }}>all cols</span>
                  )}
                </div>
                {isExpanded && cols?.length && (
                  <div style={S.colPills}>
                    {cols.map(c => {
                      const fn = source.masking?.[t]?.[c]
                      return (
                        <span key={c} style={{ ...S.colPill, ...(fn ? { borderColor: '#854d0e', color: '#fde68a' } : {}) }}>
                          {c}{fn ? ` [${fn}]` : ''}
                        </span>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

function SourceModal({ initial, onClose, onSaved }: {
  initial: Source | null
  onClose: () => void
  onSaved: () => void
}) {
  const isEdit = !!initial
  const [step, setStep] = useState<'config' | 'tables'>('config')
  const [type, setType] = useState<string>(initial?.type ?? 'postgres')
  const [name, setName] = useState(initial?.name ?? '')
  const initConn = (t: string, existing?: Record<string, unknown>) =>
    ({ ...defaultConn(t), ...Object.fromEntries(Object.entries(existing ?? {}).map(([k, v]) => [k, String(v)])) })
  const isDebeziumLike = (t: string) => t === 'debezium' || t === 'oracle' || t === 'db2'
  const [conn, setConn] = useState<Record<string, string>>(() =>
    isDebeziumLike(initial?.type ?? '')
      ? { listen_port: String(initial?.listen_port ?? defaultDebeziumPort(initial?.type ?? 'debezium')) }
      : initConn(initial?.type ?? 'postgres', initial?.connection)
  )
  const [tables, setTables] = useState<string[]>(initial?.tables ?? [])
  const [columns, setColumns] = useState<Record<string, string[]>>(initial?.columns ?? {})
  const [masking, setMasking] = useState<Record<string, Record<string, string>>>(initial?.masking ?? {})
  const [available, setAvailable] = useState<string[]>([])
  const [schema, setSchema] = useState<Record<string, string[]>>({})
  const [expandedTable, setExpandedTable] = useState<string | null>(null)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ ok: boolean; error?: string } | null>(null)
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState('')

  const fields = connFields(type)

  const handleTest = async () => {
    setTesting(true)
    setTestResult(null)
    try {
      const testPayload = isDebeziumLike(type)
        ? { name, type: type as any, listen_port: parseInt(conn.listen_port ?? '8765', 10), tables: [] }
        : { name, type: type as any, connection: conn, tables: [] }
      const result = await testSource(testPayload)
      setTestResult(result)
      if (result.ok && result.tables) {
        setAvailable(result.tables)
        setSchema(result.schema ?? {})
        setStep('tables')
      }
    } catch (e: any) {
      setTestResult({ ok: false, error: e.message })
    }
    setTesting(false)
  }

  const handleSave = async () => {
    if (!name) { setErr('Name is required'); return }
    if (tables.length === 0) { setErr('Select at least one table'); return }
    setSaving(true)
    try {
      const src: Source = isDebeziumLike(type)
        ? { name, type: type as any, listen_port: parseInt(conn.listen_port ?? '8765', 10), tables, columns, masking }
        : { name, type: type as any, connection: conn, tables, columns, masking }
      if (isEdit) await updateSource(initial!.name, src)
      else await addSource(src)
      onSaved()
    } catch (e: any) {
      setErr(e.message)
    }
    setSaving(false)
  }

  const toggleTable = (t: string) =>
    setTables(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t])

  const toggleColumn = (table: string, col: string) => {
    setColumns(prev => {
      const allCols = schema[table] ?? []
      const current = prev[table] ?? allCols
      const next = current.includes(col) ? current.filter(c => c !== col) : [...current, col]
      // If all selected, store as empty (= all)
      return { ...prev, [table]: next.length === allCols.length ? [] : next }
    })
  }

  const setColumnMasking = (table: string, col: string, fn: string) => {
    setMasking(prev => {
      const tbl = { ...(prev[table] ?? {}) }
      if (fn) tbl[col] = fn
      else delete tbl[col]
      if (Object.keys(tbl).length === 0) {
        const next = { ...prev }
        delete next[table]
        return next
      }
      return { ...prev, [table]: tbl }
    })
  }

  const selectedCols = (table: string): string[] =>
    columns[table]?.length ? columns[table] : (schema[table] ?? [])

  const colLabel = (table: string): string => {
    const all = schema[table]?.length ?? 0
    const sel = columns[table]?.length ?? 0
    if (!all) return ''
    return sel === 0 || sel === all ? `all ${all} cols` : `${sel}/${all} cols`
  }

  return (
    <div style={S.overlay}>
      <div style={S.modal}>
        <div style={S.modalHeader}>
          <h2 style={S.modalTitle}>{isEdit ? 'Edit source' : 'Add source'}</h2>
          <button style={S.btnIcon} onClick={onClose}><X size={16} /></button>
        </div>

        {/* Step indicator */}
        <div style={S.steps}>
          <span style={{ ...S.step, ...(step === 'config' ? S.stepActive : S.stepDone) }}>1 Connection</span>
          <span style={S.stepDivider}>›</span>
          <span style={{ ...S.step, ...(step === 'tables' ? S.stepActive : {}) }}>2 Tables & columns</span>
        </div>

        {step === 'config' ? (
          <>
            <div style={S.field}>
              <label style={S.label}>Source type</label>
              <div style={S.typeGrid}>
                {SOURCE_TYPES.map(t => (
                  <button key={t}
                    style={{ ...S.typeBtn, ...(type === t ? S.typeBtnActive : {}) }}
                    onClick={() => { setType(t); setConn(isDebeziumLike(t) ? { listen_port: String(defaultDebeziumPort(t)) } : initConn(t)) }}
                  >{t}</button>
                ))}
              </div>
            </div>

            <div style={S.field}>
              <label style={S.label}>Connection name</label>
              <input style={S.input} value={name} onChange={e => setName(e.target.value)}
                placeholder="e.g. prod_postgres" />
            </div>

            {isDebeziumLike(type) && (
              <div style={S.setupHint}>
                <div style={{ fontWeight: 600, marginBottom: 6, color: '#e2e8f0' }}>
                  {type === 'oracle' ? '🔌 Requires Debezium Server (Oracle LogMiner)' :
                   type === 'db2'    ? '🔌 Requires Debezium Server (DB2 ASN Capture)' :
                                       '🔌 Requires Debezium Server'}
                </div>
                <div style={{ fontSize: 12, lineHeight: 1.6 }}>
                  {type === 'oracle' && <>Start with: <code style={S.code}>cp debezium/oracle.properties debezium/application.properties</code><br />then: <code style={S.code}>docker run -p {conn.listen_port ?? 8765}:{conn.listen_port ?? 8765} debezium/server:2.7</code></>}
                  {type === 'db2'    && <>Start with: <code style={S.code}>cp debezium/db2.properties debezium/application.properties</code><br />then: <code style={S.code}>docker run -p {conn.listen_port ?? 8767}:{conn.listen_port ?? 8767} debezium/server:2.7</code></>}
                  {type === 'debezium' && <>Point <code style={S.code}>debezium.sink.http.url</code> at <code style={S.code}>http://&lt;host&gt;:{conn.listen_port ?? 8765}/events</code></>}
                </div>
              </div>
            )}

            {fields.map(f => (
              <div style={S.field} key={f.key}>
                <label style={S.label}>{f.label}</label>
                {f.secret ? (
                  <SecretFieldInput
                    value={conn[f.key] ?? ''}
                    onChange={v => setConn(c => ({ ...c, [f.key]: v }))}
                    placeholder={f.placeholder ?? ''}
                    isPassword
                    inputStyle={{ boxSizing: 'border-box' }}
                  />
                ) : (
                  <input style={S.input}
                    type="text"
                    value={conn[f.key] ?? ''}
                    onChange={e => setConn(c => ({ ...c, [f.key]: e.target.value }))}
                    placeholder={f.placeholder ?? ''}
                  />
                )}
              </div>
            ))}

            {testResult && (
              <div style={{ ...S.testBanner, background: testResult.ok ? '#052e16' : '#2d0a0a', borderColor: testResult.ok ? '#166534' : '#7f1d1d' }}>
                {testResult.ok
                  ? isDebeziumLike(type)
                    ? <><CheckCircle size={14} color="#4ade80" /> Listener ready on port {conn.listen_port ?? 8765} — Debezium Server will push events here</>
                    : <><CheckCircle size={14} color="#4ade80" /> Connected — found {available.length} tables</>
                  : <><AlertCircle size={14} color="#f87171" /> {testResult.error}</>}
              </div>
            )}

            {err && <div style={S.errMsg}>{err}</div>}

            <div style={S.modalFooter}>
              <button style={S.btnSecondary} onClick={onClose}>Cancel</button>
              <button style={S.btnSecondary} onClick={handleTest} disabled={testing}>
                {testing ? <Loader size={13} /> : null} Test connection
              </button>
              {(testResult?.ok || isDebeziumLike(type)) && (
                <button style={S.btnPrimary} onClick={() => setStep('tables')}>
                  {isDebeziumLike(type) ? 'Enter tables →' : 'Choose tables →'}
                </button>
              )}
            </div>
          </>
        ) : (
          <>
            <p style={{ color: '#94a3b8', fontSize: 13, marginBottom: 12 }}>
              Check the tables to replicate, then expand each to filter columns.
            </p>
            <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
              <button style={S.btnSmall} onClick={() => setTables(available)}>Select all tables</button>
              <button style={S.btnSmall} onClick={() => setTables([])}>Clear</button>
            </div>
            <div style={S.tableScroll}>
              {available.map(t => {
                const checked = tables.includes(t)
                const allCols = schema[t] ?? []
                const isExpanded = expandedTable === t
                const selCols = selectedCols(t)

                return (
                  <div key={t}>
                    {/* Table row */}
                    <div style={{ ...S.tableRow, opacity: checked ? 1 : 0.45 }}>
                      <input type="checkbox" checked={checked} onChange={() => toggleTable(t)} />
                      <span
                        style={{ fontFamily: 'monospace', fontSize: 13, flex: 1, cursor: allCols.length ? 'pointer' : 'default' }}
                        onClick={() => allCols.length && checked && setExpandedTable(isExpanded ? null : t)}
                      >
                        {t}
                      </span>
                      {checked && allCols.length > 0 && (
                        <span style={S.colCountBadge}>
                          <Columns size={10} /> {colLabel(t)}
                        </span>
                      )}
                      {checked && allCols.length > 0 && (
                        <button style={S.btnIconSm} onClick={() => setExpandedTable(isExpanded ? null : t)}>
                          <ChevronRight size={13} style={{ transform: isExpanded ? 'rotate(90deg)' : 'none', transition: '0.15s' }} />
                        </button>
                      )}
                    </div>

                    {/* Column selector */}
                    {isExpanded && checked && allCols.length > 0 && (
                      <div style={S.colSection}>
                        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
                          <button style={S.btnTiny} onClick={() => setColumns(c => ({ ...c, [t]: [] }))}>
                            All columns
                          </button>
                          <button style={S.btnTiny} onClick={() => setColumns(c => ({ ...c, [t]: [] }))}>
                            Reset
                          </button>
                        </div>
                        <div style={S.colGrid}>
                          {allCols.map(col => {
                            const isSel = selCols.includes(col)
                            const maskFn = masking[t]?.[col] ?? ''
                            return (
                              <div key={col} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '2px 0' }}>
                                <label style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', flex: 1, minWidth: 0 }}>
                                  <input
                                    type="checkbox"
                                    checked={isSel}
                                    onChange={() => toggleColumn(t, col)}
                                  />
                                  <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cbd5e1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{col}</span>
                                </label>
                                {isSel && (
                                  <select
                                    style={S.maskSelect}
                                    value={maskFn}
                                    onChange={e => setColumnMasking(t, col, e.target.value)}
                                    title="Masking function"
                                  >
                                    {MASK_FNS.map(fn => (
                                      <option key={fn} value={fn}>{MASK_LABELS[fn]}</option>
                                    ))}
                                  </select>
                                )}
                              </div>
                            )
                          })}
                        </div>
                      </div>
                    )}
                  </div>
                )
              })}

              {available.length === 0 && (
                <div style={{ color: '#64748b', padding: 16 }}>
                  {isDebeziumLike(type)
                    ? <>Enter table names exactly as Debezium sends them ({debeziumTableHint(type)}):</>
                    : <>No tables found — type table names manually:</>}
                  <textarea style={{ ...S.input, marginTop: 8, height: 80, resize: 'vertical' }}
                    placeholder={debeziumTablePlaceholder(type)}
                    value={tables.join('\n')}
                    onChange={e => setTables(e.target.value.split('\n').filter(Boolean))}
                  />
                </div>
              )}
            </div>

            {err && <div style={S.errMsg}>{err}</div>}
            <div style={S.modalFooter}>
              <button style={S.btnSecondary} onClick={() => setStep('config')}>← Back</button>
              <span style={{ color: '#64748b', fontSize: 13 }}>{tables.length} table{tables.length !== 1 ? 's' : ''} selected</span>
              <button style={S.btnPrimary} onClick={handleSave} disabled={saving}>
                {saving ? <Loader size={13} /> : null} {isEdit ? 'Save changes' : 'Add source'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

function CreateTablesModal({ source, onClose }: { source: Source; onClose: () => void }) {
  const [results, setResults] = useState<CreateTableResult[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [executing, setExecuting] = useState(false)
  const [expandedDdl, setExpandedDdl] = useState<string | null>(null)
  const [err, setErr] = useState('')

  useEffect(() => {
    createSourceTables(source.name, { dry_run: true })
      .then(r => setResults(r.results))
      .catch(e => setErr(e.message))
      .finally(() => setLoading(false))
  }, [source.name])

  const handleCreate = async () => {
    setExecuting(true)
    try {
      const r = await createSourceTables(source.name, { dry_run: false })
      setResults(r.results)
    } catch (e: any) {
      setErr(e.message)
    }
    setExecuting(false)
  }

  const allDone = results?.every(r => r.status === 'created' || r.status === 'exists')
  const hasErrors = results?.some(r => r.status === 'error')

  return (
    <div style={S.overlay}>
      <div style={{ ...S.modal, width: 620 }}>
        <div style={S.modalHeader}>
          <div>
            <h2 style={S.modalTitle}>Create tables in Dremio</h2>
            <p style={{ color: '#64748b', fontSize: 13, marginTop: 4 }}>
              Source: <span style={{ color: '#93c5fd', fontFamily: 'monospace' }}>{source.name}</span>
            </p>
          </div>
          <button style={S.btnIcon} onClick={onClose}><X size={16} /></button>
        </div>

        {loading && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, color: '#64748b', padding: '24px 0' }}>
            <Loader size={16} /> Introspecting schema…
          </div>
        )}

        {err && <div style={S.errMsg}>{err}</div>}

        {results && (
          <>
            <p style={{ color: '#94a3b8', fontSize: 13, marginBottom: 12 }}>
              {results.length} table{results.length !== 1 ? 's' : ''} will be created with CDC metadata columns
              (<span style={{ fontFamily: 'monospace', fontSize: 12 }}>_cdc_op, _cdc_source, _cdc_ts</span>).
            </p>

            <div style={{ border: '1px solid #334155', borderRadius: 8, overflow: 'hidden', marginBottom: 16 }}>
              {results.map(r => (
                <div key={r.table} style={{ borderBottom: '1px solid #1e293b' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px' }}>
                    <StatusIcon status={r.status} />
                    <div style={{ flex: 1 }}>
                      <span style={{ fontFamily: 'monospace', fontSize: 13, color: '#e2e8f0' }}>{r.table}</span>
                      <span style={{ color: '#475569', fontSize: 12, marginLeft: 8 }}>→ {r.target}</span>
                    </div>
                    {r.error && <span style={{ color: '#f87171', fontSize: 12 }}>{r.error}</span>}
                    <button style={S.btnIconSm} onClick={() => setExpandedDdl(expandedDdl === r.table ? null : r.table)}>
                      <ChevronRight size={13} style={{ transform: expandedDdl === r.table ? 'rotate(90deg)' : 'none', transition: '0.15s' }} />
                    </button>
                  </div>
                  {expandedDdl === r.table && (
                    <pre style={{ margin: 0, padding: '10px 14px', background: '#0a1628', fontSize: 12, color: '#7dd3fc', overflowX: 'auto', borderTop: '1px solid #1e293b' }}>
                      {r.ddl}
                    </pre>
                  )}
                </div>
              ))}
            </div>
          </>
        )}

        <div style={S.modalFooter}>
          <button style={S.btnSecondary} onClick={onClose}>
            {allDone ? 'Close' : 'Cancel'}
          </button>
          {results && !allDone && (
            <button style={S.btnPrimary} onClick={handleCreate} disabled={executing || loading}>
              {executing ? <Loader size={13} /> : <Table size={13} />}
              {executing ? 'Creating…' : `Create ${results.length} table${results.length !== 1 ? 's' : ''}`}
            </button>
          )}
          {allDone && !hasErrors && (
            <span style={{ display: 'flex', alignItems: 'center', gap: 6, color: '#4ade80', fontSize: 13 }}>
              <CheckCircle size={14} /> All tables ready
            </span>
          )}
        </div>
      </div>
    </div>
  )
}

function StatusIcon({ status }: { status: CreateTableResult['status'] }) {
  if (status === 'created') return <CheckCircle size={14} color="#4ade80" />
  if (status === 'exists')  return <CheckCircle size={14} color="#60a5fa" />
  if (status === 'error')   return <AlertCircle size={14} color="#f87171" />
  return <span style={{ width: 14, height: 14, borderRadius: '50%', border: '1px solid #475569', display: 'inline-block' }} />
}

type FieldDef = { key: string; label: string; placeholder?: string; default?: string; secret?: boolean }

function connFields(type: string): FieldDef[] {
  switch (type) {
    case 'postgres': return [
      { key: 'host',             label: 'Host',             default: 'localhost' },
      { key: 'port',             label: 'Port',             default: '5432' },
      { key: 'database',         label: 'Database',         placeholder: 'mydb' },
      { key: 'user',             label: 'User',             placeholder: 'cdc_user' },
      { key: 'password',         label: 'Password',         secret: true },
      { key: 'replication_slot', label: 'Replication slot', default: 'dremio_cdc' },
      { key: 'publication',      label: 'Publication',      default: 'dremio_cdc' },
    ]
    case 'mysql': return [
      { key: 'host',      label: 'Host',      default: 'localhost' },
      { key: 'port',      label: 'Port',      default: '3306' },
      { key: 'database',  label: 'Database',  placeholder: 'mydb' },
      { key: 'user',      label: 'User',      placeholder: 'cdc_user' },
      { key: 'password',  label: 'Password',  secret: true },
      { key: 'server_id', label: 'Server ID', default: '1001' },
    ]
    case 'mongodb': return [
      { key: 'uri', label: 'Connection URI', placeholder: 'mongodb://user:pass@localhost:27017' },
    ]
    case 'dynamodb': return [
      { key: 'region',               label: 'AWS Region',              default: 'us-east-1' },
      { key: 'aws_access_key_id',    label: 'Access key ID' },
      { key: 'aws_secret_access_key',label: 'Secret access key',       secret: true },
      { key: 'endpoint_url',         label: 'Endpoint URL (optional)', placeholder: 'http://localhost:8000' },
    ]
    case 'sqlserver': return [
      { key: 'host',     label: 'Host',     default: 'localhost' },
      { key: 'port',     label: 'Port',     default: '1433' },
      { key: 'database', label: 'Database', placeholder: 'mydb' },
      { key: 'user',     label: 'User',     placeholder: 'sa' },
      { key: 'password', label: 'Password', secret: true },
      { key: 'driver',   label: 'ODBC Driver', default: 'ODBC Driver 17 for SQL Server' },
    ]
    case 'snowflake': return [
      { key: 'account',   label: 'Account',   placeholder: 'xy12345.us-east-1' },
      { key: 'user',      label: 'User',      placeholder: 'cdc_user' },
      { key: 'password',  label: 'Password',  secret: true },
      { key: 'database',  label: 'Database',  placeholder: 'MYDB' },
      { key: 'schema',    label: 'Schema',    default: 'PUBLIC' },
      { key: 'warehouse', label: 'Warehouse', placeholder: 'COMPUTE_WH' },
      { key: 'role',      label: 'Role (optional)', placeholder: 'CDC_ROLE' },
    ]
    case 'cockroachdb': return [
      { key: 'host',     label: 'Host',     default: 'localhost' },
      { key: 'port',     label: 'Port',     default: '26257' },
      { key: 'database', label: 'Database', placeholder: 'defaultdb' },
      { key: 'user',     label: 'User',     default: 'root' },
      { key: 'password', label: 'Password', secret: true },
      { key: 'sslmode',  label: 'SSL mode', default: 'disable' },
    ]
    case 'oracle': return [
      { key: 'listen_port', label: 'Listen port', default: '8765',
        placeholder: 'Must match debezium.sink.http.url port in oracle.properties' },
    ]
    case 'db2': return [
      { key: 'listen_port', label: 'Listen port', default: '8767',
        placeholder: 'Must match debezium.sink.http.url port in db2.properties' },
    ]
    case 'debezium': return [
      { key: 'listen_port', label: 'Listen port', default: '8765' },
    ]
    default: return []
  }
}

function defaultDebeziumPort(type: string): number {
  if (type === 'db2') return 8767
  return 8765
}

function debeziumTableHint(type: string): React.ReactNode {
  if (type === 'oracle')
    return <>Oracle uses uppercase <code style={{ color: '#fdba74' }}>SCHEMA.TABLE</code> (e.g. <code style={{ color: '#fdba74' }}>HR.EMPLOYEES</code>)</>
  if (type === 'db2')
    return <>DB2 uses uppercase <code style={{ color: '#a78bfa' }}>SCHEMA.TABLE</code> (e.g. <code style={{ color: '#a78bfa' }}>HR.EMPLOYEE</code>)</>
  return <>match <code style={{ color: '#fdba74' }}>table.include.list</code> in your Debezium properties</>
}

function debeziumTablePlaceholder(type: string): string {
  if (type === 'oracle')  return 'HR.EMPLOYEES\nHR.DEPARTMENTS\nFINANCE.ACCOUNTS'
  if (type === 'db2')     return 'HR.EMPLOYEE\nHR.DEPARTMENT\nFINANCE.LEDGER'
  return 'dbo.customers\ndbo.orders'
}

function defaultConn(type: string): Record<string, string> {
  return Object.fromEntries(connFields(type).filter(f => f.default).map(f => [f.key, f.default!]))
}

function typeColor(type: string): React.CSSProperties {
  const map: Record<string, React.CSSProperties> = {
    postgres:    { background: '#1e3a5f', color: '#93c5fd' },
    mysql:       { background: '#1a2e1a', color: '#86efac' },
    mongodb:     { background: '#1f2937', color: '#6ee7b7' },
    dynamodb:    { background: '#2d1f3d', color: '#c4b5fd' },
    sqlserver:   { background: '#1f1a2e', color: '#a78bfa' },
    snowflake:   { background: '#0f2537', color: '#67e8f9' },
    cockroachdb: { background: '#1a1f2e', color: '#fca5a5' },
    oracle:      { background: '#2d1a0a', color: '#fb923c' },
    db2:         { background: '#1a1a2e', color: '#c084fc' },
    debezium:    { background: '#2d1a0a', color: '#fdba74' },
  }
  return map[type] ?? {}
}

const S: Record<string, React.CSSProperties> = {
  page: { padding: 32, maxWidth: 900 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 28 },
  title: { fontSize: 22, fontWeight: 700, color: '#f1f5f9' },
  subtitle: { color: '#64748b', fontSize: 13, marginTop: 4 },
  list: { display: 'flex', flexDirection: 'column', gap: 12 },
  card: { background: '#1e293b', border: '1px solid #334155', borderRadius: 10, padding: '14px 18px' },
  cardRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  cardName: { fontWeight: 600, color: '#e2e8f0' },
  tableCount: { color: '#64748b', fontSize: 12 },
  treeTableRow: { display: 'flex', alignItems: 'center', gap: 6, padding: '4px 8px', borderRadius: 4, cursor: 'pointer', background: '#0f172a', marginBottom: 2 },
  colBadge: { marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: '#60a5fa', background: '#1e3a5f', padding: '1px 6px', borderRadius: 3 },
  colPills: { display: 'flex', flexWrap: 'wrap', gap: 4, paddingLeft: 20, paddingBottom: 6 },
  colPill: { fontFamily: 'monospace', fontSize: 11, color: '#94a3b8', background: '#0f172a', border: '1px solid #1e293b', padding: '1px 6px', borderRadius: 3 },
  typeBadge: { fontSize: 11, fontWeight: 600, padding: '2px 8px', borderRadius: 4 },
  empty: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16, padding: '80px 0', color: '#475569' },
  overlay: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 100 },
  modal: { background: '#1e293b', border: '1px solid #334155', borderRadius: 12, padding: 28, width: 560, maxHeight: '90vh', overflowY: 'auto' },
  modalHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 },
  modalTitle: { fontSize: 17, fontWeight: 700, color: '#f1f5f9' },
  steps: { display: 'flex', alignItems: 'center', gap: 8, marginBottom: 24 },
  step: { fontSize: 12, fontWeight: 600, color: '#475569' },
  stepActive: { color: '#93c5fd' },
  stepDone: { color: '#4ade80' },
  stepDivider: { color: '#334155', fontSize: 14 },
  field: { marginBottom: 16 },
  label: { display: 'block', color: '#94a3b8', fontSize: 12, fontWeight: 600, marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.05em' },
  input: { width: '100%', background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '8px 12px', color: '#e2e8f0', fontSize: 13, outline: 'none', boxSizing: 'border-box' },
  typeGrid: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  typeBtn: { background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '6px 14px', color: '#64748b', cursor: 'pointer', fontSize: 13, fontWeight: 500 },
  typeBtnActive: { background: '#1e3a5f', border: '1px solid #2563eb', color: '#93c5fd' },
  testBanner: { display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderRadius: 8, border: '1px solid', fontSize: 13, marginBottom: 16 },
  errMsg: { color: '#f87171', fontSize: 13, marginBottom: 12 },
  modalFooter: { display: 'flex', gap: 10, justifyContent: 'flex-end', marginTop: 24, paddingTop: 16, borderTop: '1px solid #334155', alignItems: 'center' },
  tableScroll: { maxHeight: 360, overflowY: 'auto', border: '1px solid #334155', borderRadius: 8 },
  tableRow: { display: 'flex', alignItems: 'center', gap: 10, padding: '8px 12px', cursor: 'default', borderBottom: '1px solid #1e293b' },
  colCountBadge: { display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: '#60a5fa', background: '#1e3a5f', padding: '2px 7px', borderRadius: 3, whiteSpace: 'nowrap' },
  colSection: { background: '#0a1628', borderBottom: '1px solid #1e293b', padding: '10px 12px 10px 32px' },
  colGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: '4px 16px' },
  colCheckRow: { display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', padding: '2px 0' },
  maskSelect: { background: '#0f172a', border: '1px solid #1e293b', borderRadius: 4, color: '#94a3b8', fontSize: 10, padding: '1px 4px', cursor: 'pointer', maxWidth: 90 },
  setupHint: { background: '#1a1f0a', border: '1px solid #365314', borderRadius: 8, padding: '12px 14px', marginBottom: 16, color: '#a3e635', fontSize: 13 },
  code: { fontFamily: 'monospace', fontSize: 11, background: '#0f172a', padding: '1px 5px', borderRadius: 3, color: '#fde68a' },
  btnPrimary: { display: 'flex', alignItems: 'center', gap: 6, background: '#2563eb', color: '#fff', border: 'none', padding: '8px 16px', borderRadius: 6, cursor: 'pointer', fontWeight: 600, fontSize: 13 },
  btnSecondary: { display: 'flex', alignItems: 'center', gap: 6, background: '#0f172a', color: '#94a3b8', border: '1px solid #334155', padding: '8px 14px', borderRadius: 6, cursor: 'pointer', fontSize: 13 },
  btnSmall: { background: '#0f172a', color: '#94a3b8', border: '1px solid #334155', padding: '4px 10px', borderRadius: 4, cursor: 'pointer', fontSize: 12 },
  btnTiny: { background: '#0f172a', color: '#64748b', border: '1px solid #1e293b', padding: '3px 8px', borderRadius: 4, cursor: 'pointer', fontSize: 11 },
  btnIcon: { background: 'none', border: 'none', color: '#64748b', cursor: 'pointer', padding: 4, display: 'flex' },
  btnIconSm: { background: 'none', border: 'none', color: '#475569', cursor: 'pointer', padding: '0 2px', display: 'flex', alignItems: 'center' },
}
