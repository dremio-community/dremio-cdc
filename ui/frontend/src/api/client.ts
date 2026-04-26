const BASE = '/api'

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || res.statusText)
  }
  return res.json()
}

// Engine
export const startEngine  = () => req('POST', '/engine/start')
export const stopEngine   = () => req('POST', '/engine/stop')
export const restartEngine = () => req('POST', '/engine/restart')
export const getStatus    = () => req<EngineStatus>('GET', '/status')

// Sources
export const getSources   = () => req<Source[]>('GET', '/sources')
export const addSource    = (s: Source) => req<Source>('POST', '/sources', s)
export const updateSource = (name: string, s: Source) => req<Source>('PUT', `/sources/${name}`, s)
export const deleteSource = (name: string) => req('DELETE', `/sources/${name}`)
export const testSource   = (s: Partial<Source>) => req<TestResult>('POST', '/sources/test', s)

// Target
export const getTarget  = () => req<TargetConfig>('GET', '/target')
export const saveTarget = (t: TargetConfig) => req('PUT', '/target', t)
export const testTarget = (dremio: DremioConfig) => req<TestResult>('POST', '/target/test', { dremio })

// Settings
export const getSettings  = () => req<Settings>('GET', '/settings')
export const saveSettings = (s: Settings) => req('PUT', '/settings', s)

// Offsets
export const resetOffset = (source: string, table: string) =>
  req('DELETE', `/offsets/${source}/${table}`)

// Create tables in Dremio
export const createSourceTables = (name: string, opts: { tables?: string[]; dry_run?: boolean }) =>
  req<CreateTablesResult>('POST', `/sources/${name}/create-tables`, opts)

// Namespace browser
export const getNamespaces = () => req<{ ok: boolean; namespaces?: NamespaceItem[]; error?: string }>('GET', '/target/namespaces')

// Mappings
export const getMappings = () => req<MappingsResult>('GET', '/mappings')

// Alerts
export const getAlerts    = () => req<AlertsResponse>('GET', '/alerts')
export const saveAlerts   = (cfg: AlertConfig) => req('PUT', '/alerts', cfg)

// Dead Letter Queue
export const getDLQ          = () => req<DLQResponse>('GET', '/dlq')
export const retryDLQEntry   = (id: number) => req('POST', `/dlq/${id}/retry`)
export const retryAllDLQ     = () => req('POST', '/dlq/retry-all')
export const discardDLQEntry = (id: number) => req('DELETE', `/dlq/${id}`)
export const discardAllDLQ   = () => req('DELETE', '/dlq')

// ── Types ──────────────────────────────────────────────────────────────────

export interface Source {
  name: string
  type: 'postgres' | 'mysql' | 'mongodb' | 'dynamodb' | 'debezium' | 'oracle' | 'db2'
  connection?: Record<string, unknown>
  listen_port?: number        // debezium only — top-level, not under connection
  tables: string[]
  columns?: Record<string, string[]>           // table -> selected columns; absent/empty = all columns
  masking?: Record<string, Record<string, string>> // table -> column -> function name
}

export interface DremioConfig {
  host?: string
  port?: number
  ssl?: boolean
  user?: string
  password?: string
  pat?: string
  target_namespace?: string
}

export interface IcebergConfig {
  type?: string
  uri?: string
  token?: string
  credential?: string
  warehouse?: string
  target_namespace?: string
  write_mode?: 'merge' | 'append'
  [key: string]: unknown
}

export interface TransformStudioConfig {
  enabled?: boolean
  url?: string
  pipeline_id?: string
  token?: string
}

export interface TargetConfig {
  sink_mode: 'dremio' | 'iceberg'
  dremio: DremioConfig
  iceberg: IcebergConfig
  transform_studio?: TransformStudioConfig
}

export interface Settings {
  batch_size?: number
  batch_timeout_seconds?: number
  snapshot_on_first_run?: boolean
  incremental_snapshot?: boolean
  snapshot_chunk_size?: number
  snapshot_cursor_column?: string
  offset_db_path?: string
  log_level?: string
  sink_mode?: string
  adaptive_batching?: boolean
  min_batch_size?: number
  max_batch_size?: number
}

export interface WorkerStatus {
  source: string
  table: string
  state: 'idle' | 'snapshotting' | 'streaming' | 'paused' | 'error'
  events_written: number
  events_per_minute: number
  lag_seconds: number | null
  pipeline_lag_seconds: number | null
  last_flush_duration_ms: number
  error_count: number
  current_batch_size: number
  schema_drift: string | null
  rate_history: [number, number][]   // [unix_ts, epm] tuples
  last_source_ts: number | null
  last_flush_ts: number | null
  last_offset: string | null
  error: string | null
  started_at: number | null
}

export interface EngineStatusSummary {
  total_events: number
  total_errors: number
  active_workers: number
  total_workers: number
}

export interface EngineStatus {
  engine_state: 'stopped' | 'starting' | 'running' | 'error'
  engine_started_at: number | null
  workers: WorkerStatus[]
  config_path: string
  summary?: EngineStatusSummary
}

export interface TestResult {
  ok: boolean
  error?: string
  tables?: string[]
  schema?: Record<string, string[]>
  version?: string
}

export interface CreateTableResult {
  table: string
  target: string
  ddl: string
  status: 'pending' | 'created' | 'exists' | 'error'
  error?: string
}

export interface CreateTablesResult {
  results: CreateTableResult[]
  dry_run: boolean
}

export interface NamespaceItem {
  name: string
  type: 'source' | 'space'
}

export interface Mapping {
  source_name: string
  source_type: string
  source_table: string
  target_path: string
  columns: string[]
  all_columns: boolean
}

export interface MappingsResult {
  mappings: Mapping[]
  namespace: string
  sink_mode: string
}

export interface AlertChannel {
  type: 'slack' | 'webhook' | 'email'
  // slack
  webhook_url?: string
  // webhook
  url?: string
  method?: string
  // email
  smtp_host?: string
  smtp_port?: number
  smtp_tls?: boolean
  smtp_user?: string
  smtp_password?: string
  from?: string
  to?: string
}

export interface AlertConfig {
  enabled?: boolean
  lag_threshold_seconds?: number
  error_count_threshold?: number
  cooldown_seconds?: number
  check_interval_seconds?: number
  channels?: AlertChannel[]
}

export interface AlertRecord {
  time: number
  type: string
  source: string
  table: string
  message: string
}

export interface AlertsResponse {
  config: AlertConfig
  recent: AlertRecord[]
}

export interface DLQEntry {
  id: number
  source: string
  table: string
  event_count: number
  error: string | null
  retry_count: number
  max_retries: number
  status: 'pending' | 'replayed' | 'exhausted' | 'discarded'
  created_at: string
}

export interface DLQStats {
  pending:   { entries: number; events: number }
  replayed:  { entries: number; events: number }
  exhausted: { entries: number; events: number }
  discarded: { entries: number; events: number }
}

export interface DLQResponse {
  entries: DLQEntry[]
  stats: DLQStats
}
