# Dremio CDC

[![Docker Hub](https://img.shields.io/docker/v/mshainman/dremio-cdc?label=Docker%20Hub&logo=docker)](https://hub.docker.com/r/mshainman/dremio-cdc)
[![GitHub](https://img.shields.io/badge/GitHub-dremio--community%2Fdremio--cdc-blue?logo=github)](https://github.com/dremio-community/dremio-cdc)

**Stream database changes directly into Dremio — no Kafka, no Spark, no infrastructure overhead.**

Dremio CDC is a lightweight Change Data Capture daemon that reads from Postgres, MySQL, MongoDB, DynamoDB, Oracle, SQL Server, and DB2, then writes changes as native Dremio tables via `MERGE INTO` (Mode A) or direct Iceberg writes to Dremio Open Catalog (Mode B). A built-in web UI lets you configure sources, monitor lag, and manage the pipeline without touching YAML.

---

## How it works

```
PostgreSQL WAL   ──┐
MySQL binlog     ──┤                          ┌── Mode A: Dremio REST API
MongoDB Streams  ──┼──► CDC daemon ──────────►│         MERGE INTO / DELETE
DynamoDB Streams ──┤       │                  └── Mode B: PyIceberg → Dremio Open Catalog
Debezium HTTP    ──┘  offset store                       (Polaris / Iceberg REST)
                       (SQLite)
```

### Mode A — Dremio SQL (default)

Connects to Dremio's REST API and issues `MERGE INTO` / `DELETE` statements. Changes land directly as Dremio tables, immediately queryable. Best for moderate-throughput pipelines and Dremio on-prem.

### Mode B — Dremio Open Catalog (high throughput)

Writes Iceberg data files directly via PyIceberg, targeting **Dremio Open Catalog** (Apache Polaris). Because writes go into the same catalog Dremio reads from, tables appear instantly — no separate metadata sync step. Best for high-throughput pipelines and Dremio Cloud.

| | Mode A | Mode B |
|---|---|---|
| **Target** | Any Dremio (on-prem or Cloud) | Dremio Open Catalog (Polaris) |
| **Write path** | Dremio SQL `MERGE INTO` | PyIceberg → Iceberg REST |
| **Throughput** | Moderate (SQL round-trips) | High (direct file writes) |
| **Setup** | Dremio connection only | Iceberg REST catalog URI + PAT |
| **Metadata sync** | Automatic | Automatic (via Open Catalog) |

---

## Supported sources

| Source | Mechanism | Notes |
|--------|-----------|-------|
| **PostgreSQL** | Logical replication (pgoutput) | `wal_level = logical` required |
| **MySQL** | Binary log (`binlog_format = ROW`) | MySQL 8.0+ recommended |
| **MongoDB** | Change Streams | Replica set required |
| **Amazon DynamoDB** | DynamoDB Streams | `NEW_AND_OLD_IMAGES` stream type |
| **Oracle** | Debezium Server → HTTP adapter | LogMiner; see [Oracle setup](#oracle) |
| **SQL Server** | Debezium Server → HTTP adapter | SQL Server Agent + CDC enabled |
| **DB2** | Debezium Server → HTTP adapter | ASN Capture required |

---

## Quick start

### Docker (recommended)

```bash
# 1. Pull the image
docker pull mshainman/dremio-cdc:latest

# 2. Copy and edit the example config
curl -O https://raw.githubusercontent.com/dremio-community/dremio-cdc/main/config.example.yml
cp config.example.yml config.yml
# Edit config.yml with your source + Dremio connection details

# 3. Run the daemon
docker run -d \
  --name dremio-cdc \
  -p 7070:7070 \
  -v $(pwd)/config.yml:/app/config.yml \
  -v $(pwd)/cdc_data:/app/data \
  mshainman/dremio-cdc:latest

# 4. Open the web UI
open http://localhost:7070
```

### From source

```bash
git clone https://github.com/dremio-community/dremio-cdc.git
cd dremio-cdc
pip install -r requirements.txt

cp config.example.yml config.yml
# Edit config.yml

python main.py --config config.yml
```

### Web UI mode

```bash
python main.py --ui --config config.yml
# Opens http://localhost:7070 in your browser
```

---

## Configuration

Copy `config.example.yml` and edit. All values support environment variable expansion: `${MY_VAR}`.

### Minimal — Mode A (Dremio SQL)

```yaml
dremio:
  host:             localhost
  port:             9047
  user:             admin
  password:         ${DREMIO_PASSWORD}
  target_namespace: cdc           # Dremio source or space where tables are created

sources:
  - name: my_postgres
    type: postgres
    connection:
      host: db.example.com
      database: production
      user: cdc_user
      password: ${PG_PASSWORD}
    tables:
      - public.orders
      - public.customers
```

### Minimal — Mode B (Dremio Open Catalog)

```yaml
sources:
  - name: my_postgres
    type: postgres
    connection:
      host: db.example.com
      database: production
      user: cdc_user
      password: ${PG_PASSWORD}
    tables:
      - public.orders

options:
  sink_mode: iceberg

iceberg:
  type:      rest
  uri:       https://catalog.dremio.cloud/api/iceberg   # Dremio Cloud
  token:     ${DREMIO_PAT}
  warehouse: my-project-name     # Dremio Cloud project name (not UUID)
  target_namespace: cdc
  write_mode: merge              # "merge" (upsert) or "append" (event log)
```

### Dremio Cloud — Mode A with SQL API

```yaml
dremio:
  host:       api.dremio.cloud
  port:       443
  ssl:        true
  pat:        ${DREMIO_PAT}
  project_id: 957704f5-4495-42ad-94de-671bf7790610   # From your Cloud project URL

sources:
  - name: my_postgres
    type: postgres
    ...
```

### Full options reference

```yaml
options:
  sink_mode:                  dremio    # "dremio" | "iceberg"
  batch_size:                 500       # Events per batch
  batch_timeout_seconds:      10        # Max wait before flushing a partial batch
  snapshot_on_first_run:      true      # Full table snapshot before streaming
  incremental_snapshot:       false     # Chunk-based snapshot (safer for large tables)
  snapshot_chunk_size:        5000
  snapshot_cursor_column:     id
  adaptive_batching:          true      # Auto-tune batch size based on throughput
  min_batch_size:             100
  max_batch_size:             5000
  offset_db_path:             ./cdc_offsets.db   # SQLite; or postgres:// for multi-process
  schema_drift_action:        alert     # "alert" | "auto_migrate" | "pause"
  dlq_db_path:                ./cdc_dlq.db
  dlq_max_retries:            3
```

---

## Source setup

### PostgreSQL

```sql
-- Verify logical replication is enabled (default on most managed Postgres)
SHOW wal_level;   -- must be 'logical'

-- Grant replication permission
ALTER ROLE cdc_user REPLICATION LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

The daemon creates the publication and replication slot automatically on first connect.

```yaml
sources:
  - name: pg_prod
    type: postgres
    connection:
      host:              db.example.com
      port:              5432
      database:          production
      user:              cdc_user
      password:          secret
      replication_slot:  dremio_cdc    # auto-created if absent
      publication:       dremio_cdc    # auto-created if absent
    tables:
      - public.orders
      - public.customers
```

### MySQL

```sql
-- Verify row-based binlog (required)
SHOW VARIABLES LIKE 'binlog_format';   -- must be ROW

-- Grant CDC permissions
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON production.* TO 'cdc_user'@'%';
```

```yaml
sources:
  - name: mysql_prod
    type: mysql
    connection:
      host:       db.example.com
      port:       3306
      database:   production
      user:       cdc_user
      password:   secret
      server_id:  1
    tables:
      - production.orders
```

### MongoDB

```bash
# Single-node replica set (if not already configured)
mongod --replSet rs0
mongosh --eval "rs.initiate()"
```

```yaml
sources:
  - name: mongo_prod
    type: mongodb
    connection:
      uri:      mongodb://cdc_user:secret@mongo.example.com:27017/?directConnection=true
      database: production
    tables:
      - customers
      - orders
```

### DynamoDB

```bash
# Enable streams on each table
aws dynamodb update-table \
  --table-name Orders \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

```yaml
sources:
  - name: dynamo_prod
    type: dynamodb
    connection:
      region:              us-east-1
      aws_access_key_id:   ${AWS_ACCESS_KEY_ID}
      aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    tables:
      - Orders
      - Customers
```

### Oracle

<a name="oracle"></a>

Oracle CDC uses **Debezium Server** as an adapter. Debezium connects to Oracle's LogMiner and POSTs change events to the CDC daemon's HTTP endpoint.

**1. Enable Oracle prerequisites:**

```sql
-- Enable ARCHIVELOG mode (requires DBA)
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- Enable supplemental logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Create a Debezium LogMiner user (CDB-level)
CREATE USER c##dbzuser IDENTIFIED BY dbzpass
    DEFAULT TABLESPACE users QUOTA UNLIMITED ON users
    CONTAINER=ALL;
GRANT CREATE SESSION, LOGMINING TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
-- (see debezium/oracle.properties for the full privilege list)
```

**2. Run Debezium Server:**

```bash
# Copy and edit the pre-built config
cp debezium/oracle.properties debezium/application.properties
# Edit: database.hostname, database.user, database.password, database.dbname

docker run -d --name debezium-oracle \
  -v $(pwd)/debezium/application.properties:/debezium/conf/application.properties \
  --add-host host.docker.internal:host-gateway \
  debezium/server:2.7.3.Final
```

**3. Configure the CDC daemon:**

```yaml
sources:
  - name: oracle_prod
    type: oracle
    listen_port: 8765          # Debezium posts events here
    tables:
      - HR.EMPLOYEES
      - HR.DEPARTMENTS
```

Pre-built configs are included for Oracle, SQL Server, and DB2:
- `debezium/oracle.properties` — production template
- `debezium/sqlserver.properties`
- `debezium/db2.properties`

### SQL Server

```sql
-- Enable CDC on the database and tables (requires sysadmin)
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='Orders',
     @role_name=NULL, @supports_net_changes=1;
```

```yaml
sources:
  - name: sqlserver_prod
    type: debezium
    listen_port: 8766
    tables:
      - dbo.Orders
```

---

## Web UI

Start with `python main.py --ui --config config.yml` or `docker run -p 7070:7070 ...`

| Page | What it does |
|------|-------------|
| **Status** | Live dashboard: lag gauge per worker, events/sec sparkline, error count, batch size |
| **Sources** | Add and configure source connectors; test connections; browse tables and columns |
| **Target** | Configure Dremio connection (Mode A) or Iceberg catalog (Mode B); test connectivity |
| **Mappings** | Select tables, filter columns, configure per-column PII masking |
| **Alerts** | Set lag and error thresholds; configure Slack, webhook, or email notifications |
| **DLQ** | Browse failed events; retry individually or in bulk |
| **Settings** | Batch size, snapshot mode, adaptive batching, offset store path |

---

## PII masking

Apply column-level masking before events reach Dremio:

```yaml
sources:
  - name: pg_prod
    type: postgres
    masking:
      public.users:
        email:  hash_sha256    # deterministic pseudonymisation
        ssn:    redact         # replace with [REDACTED]
        name:   mask           # replace with ***
        phone:  nullify        # replace with NULL
        card:   tokenize       # replace with tok_<hex16>
```

Available functions: `redact`, `hash_sha256`, `hash_md5`, `mask`, `nullify`, `tokenize`

---

## Alerting

```yaml
alerts:
  enabled: true
  lag_threshold_seconds:  60
  error_count_threshold:  5
  cooldown_seconds:       300
  channels:
    - type: slack
      webhook_url: https://hooks.slack.com/services/...
    - type: email
      smtp_host:     smtp.gmail.com
      smtp_port:     587
      smtp_tls:      true
      smtp_user:     me@gmail.com
      smtp_password: ${SMTP_PASSWORD}
      from:          me@gmail.com
      to:            oncall@company.com
    - type: webhook
      url:    https://my-ops-system.example.com/events
      method: POST
```

---

## Docker deployment

### Single container

```bash
docker run -d \
  --name dremio-cdc \
  -p 7070:7070 \
  -v $(pwd)/config.yml:/app/config.yml \
  -v $(pwd)/cdc_data:/app/data \
  -e DREMIO_PASSWORD=secret \
  -e PG_PASSWORD=secret \
  mshainman/dremio-cdc:latest
```

### Docker Compose (with source databases for dev/test)

```bash
# Starts Postgres, MySQL, MongoDB, DynamoDB (LocalStack), SQL Server, Oracle,
# Debezium Server, and a local Iceberg REST catalog
docker compose up -d

# Then run the CDC daemon against the test environment
python main.py --config config.test.yml
```

### Environment variables

All config values support `${VAR}` substitution. Pass secrets via environment:

```bash
docker run ... \
  -e DREMIO_PAT=eyJ... \
  -e PG_PASSWORD=secret \
  -e AWS_ACCESS_KEY_ID=AKIA... \
  mshainman/dremio-cdc:latest
```

---

## Advanced features

### Adaptive batching

Automatically tunes batch size based on observed throughput and lag. When lag is low, uses smaller batches for lower latency; under load, grows batch size to maximize throughput.

```yaml
options:
  adaptive_batching: true
  min_batch_size:    100
  max_batch_size:    5000
```

### Schema drift detection

Automatically detects when source tables add, remove, or change column types:

```yaml
options:
  schema_drift_action: alert         # "alert" | "auto_migrate" | "pause"
  schema_drift_check_every_n_batches: 10
```

`auto_migrate` issues `ALTER TABLE ADD COLUMN` on the Dremio target automatically.

### Incremental snapshot

Safer than a full-table snapshot for large tables — reads in PK-ordered chunks:

```yaml
options:
  incremental_snapshot:    true
  snapshot_chunk_size:     5000
  snapshot_cursor_column:  id
```

### Dead letter queue

Failed events are parked to SQLite instead of dropped; a background worker retries automatically:

```yaml
options:
  dlq_db_path:     ./cdc_dlq.db
  dlq_max_retries: 3
```

Browse and replay from the `/dlq` page in the web UI.

### Multi-process offset store

Switch from SQLite to PostgreSQL for the offset store to support running multiple daemon instances:

```yaml
options:
  offset_db_path: postgresql://cdc_user:pass@postgres.example.com/cdc_offsets
```

### Prometheus metrics

Expose metrics for Prometheus scraping:

```
GET http://localhost:7070/metrics
```

Each worker emits: `cdc_lag_seconds`, `cdc_events_total`, `cdc_errors_total`, `cdc_flush_duration_seconds`, `cdc_batch_size`.

---

## Development

### Local test environment

```bash
# Start all test databases (Postgres, MySQL, MongoDB, DynamoDB, SQL Server, Oracle)
docker compose up -d

# Run tests (unit + integration; excludes cloud and oracle live tests)
python3 -m pytest tests/test_e2e.py -m "not cloud and not ui and not oracle" -v

# Run Oracle live tests (requires docker compose up -d oracle debezium-oracle)
python3 -m pytest tests/test_e2e.py -m oracle -v

# Run against Dremio Cloud
python3 -m pytest tests/test_e2e.py -m cloud -v
```

### Project structure

```
dremio-cdc/
├── main.py                     # CLI entry point (daemon or UI)
├── config.example.yml          # Annotated config template
├── requirements.txt
│
├── core/
│   ├── engine.py               # CDCEngine: one TableWorker thread per (source, table)
│   ├── event.py                # ChangeEvent dataclass + Operation enum
│   ├── dremio_sink.py          # Mode A: Dremio REST API + MERGE INTO
│   ├── iceberg_sink.py         # Mode B: PyIceberg → Dremio Open Catalog
│   ├── offset_store.py         # SQLite / Postgres offset persistence
│   ├── schema_store.py         # Schema drift detection
│   ├── status_store.py         # Live status for web UI
│   ├── alert_manager.py        # Lag + error threshold alerting
│   ├── dlq.py                  # Dead letter queue
│   ├── masking.py              # Column-level PII masking
│   └── ts_trigger.py           # Transform Studio pipeline trigger
│
├── sources/
│   ├── base.py                 # CDCSource abstract base class
│   ├── postgres.py             # pgoutput logical replication
│   ├── mysql.py                # MySQL binlog via python-mysql-replication
│   ├── mongodb.py              # MongoDB Change Streams
│   ├── dynamodb.py             # DynamoDB Streams via boto3
│   ├── debezium.py             # HTTP adapter for Debezium Server (Oracle, SQL Server, DB2)
│   ├── sqlserver.py            # SQL Server CDC via Debezium
│   ├── snowflake_src.py        # Snowflake polling
│   └── cockroachdb.py          # CockroachDB CHANGEFEED (experimental)
│
├── ui/
│   ├── backend/app.py          # Flask REST API + SPA server
│   └── frontend/               # React 18 + Vite (TypeScript)
│
├── debezium/                   # Pre-built Debezium Server configs
│   ├── oracle.properties       # Oracle production template
│   ├── oracle-test.properties  # Oracle Docker test config
│   ├── sqlserver.properties
│   └── db2.properties
│
└── tests/
    ├── test_e2e.py             # Integration test suite (110+ tests)
    └── fixtures/               # SQL seed files, Docker init scripts
```

### Adding a new source

Subclass `CDCSource` in `sources/`:

```python
from sources.base import CDCSource

class MySource(CDCSource):
    def connect(self): ...
    def get_schema(self, table) -> list[ColumnSchema]: ...
    def snapshot(self, table) -> Generator[ChangeEvent, None, None]: ...
    def stream(self, table, offset) -> Generator[ChangeEvent, None, None]: ...
    def close(self): ...
```

Register in `core/engine.py` in the `_SOURCE_MAP` dict.

---

## License

Apache 2.0

## Author

Mark Shainman
