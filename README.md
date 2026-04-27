# Dremio CDC

[![Docker Hub](https://img.shields.io/docker/v/mshainman/dremio-cdc?label=Docker%20Hub&logo=docker)](https://hub.docker.com/r/mshainman/dremio-cdc)
[![GitHub](https://img.shields.io/badge/GitHub-dremio--community%2Fdremio--cdc-blue?logo=github)](https://github.com/dremio-community/dremio-cdc)

📖 **[Visual User Guide](https://htmlpreview.github.io/?https://github.com/dremio-community/dremio-cdc/blob/main/docs/VISUAL_GUIDE.html)** — step-by-step walkthrough with real screenshots

**Stream database changes directly into Dremio — no Kafka, no Spark, no infrastructure overhead.**

Dremio CDC is a lightweight Change Data Capture daemon that reads from Postgres, MySQL, MariaDB, MongoDB, DynamoDB, Snowflake, Oracle, SQL Server (native or via Debezium), and DB2, then writes changes as native Dremio tables via `MERGE INTO` (Mode A) or direct Iceberg writes to Dremio Open Catalog (Mode B). A built-in web UI lets you configure sources, monitor lag, and manage the pipeline without touching YAML.

---

## How it works

```
PostgreSQL WAL   ──┐
MySQL binlog     ──┤
MariaDB binlog   ──┤                          ┌── Mode A: Dremio REST API
MongoDB Streams  ──┼──► CDC daemon ──────────►│         MERGE INTO / DELETE
DynamoDB Streams ──┤       │                  └── Mode B: PyIceberg → Dremio Open Catalog
Snowflake STREAM ──┤  offset store                       (Polaris / Iceberg REST)
Debezium HTTP    ──┘   (SQLite)
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
| **MariaDB** | Binary log (`binlog_format = ROW`) | MariaDB 10.2+; same protocol as MySQL |
| **MongoDB** | Change Streams | Replica set required |
| **Amazon DynamoDB** | DynamoDB Streams | `NEW_AND_OLD_IMAGES` stream type |
| **Snowflake** | Snowflake STREAM objects | Native CDC; no Debezium required |
| **Oracle** | Debezium Server → HTTP adapter | LogMiner; see [Oracle setup](#oracle) |
| **SQL Server** | Native CDC (LSN-based) | `sp_cdc_enable_db` + `sp_cdc_enable_table` required |
| **SQL Server** | Debezium Server → HTTP adapter | Alternative; SQL Server Agent + CDC enabled |
| **DB2** | Debezium Server → HTTP adapter | ASN Capture required |

---

## Quick start

### Docker (recommended)

```bash
# 1. Pull and run
docker pull mshainman/dremio-cdc:latest
docker run -d \
  --name dremio-cdc \
  -p 7070:7070 \
  -v $(pwd)/cdc_data:/app/data \
  mshainman/dremio-cdc:latest

# 2. Open the web UI
open http://localhost:7070
```

The UI walks you through connecting a source, configuring the Dremio target, selecting tables, and starting the pipeline.

### From source

```bash
git clone https://github.com/dremio-community/dremio-cdc.git
cd dremio-cdc
pip install -r requirements.txt
python main.py --ui
# Opens http://localhost:7070
```

---

## Web UI

Open `http://localhost:7070` after starting the daemon. Everything is configurable from the UI — no config file required.

| Page | What it does |
|------|-------------|
| **Status** | Live dashboard: lag gauge per worker, events/sec sparkline, error count, batch size |
| **Sources** | Add and configure source connectors; test connections; browse tables and columns |
| **Target** | Configure Dremio connection (Mode A) or Iceberg catalog (Mode B); test connectivity |
| **Mappings** | Select tables, filter columns, configure per-column PII masking |
| **Alerts** | Set lag and error thresholds; configure Slack, webhook, or email notifications |
| **DLQ** | Browse failed events; retry individually or in bulk |
| **Settings** | Batch size, snapshot mode, adaptive batching, schema drift, offset store path |

---

## Source setup

Each source requires a one-time database-side configuration (granting permissions, enabling CDC features). Once the database is ready, add the source in the UI under **Sources → Add Source**.

The SQL commands below are the database prerequisites — required regardless of whether you configure the daemon via the UI or a YAML file.

### PostgreSQL

```sql
-- Verify logical replication is enabled (default on most managed Postgres)
SHOW wal_level;   -- must be 'logical'

-- Grant replication permission
ALTER ROLE cdc_user REPLICATION LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

The daemon creates the publication and replication slot automatically on first connect.

**In the UI:** Sources → Add Source → PostgreSQL. Enter host, port, database, user, and password. The replication slot and publication names default to `dremio_cdc` and are created automatically.

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: pg_prod
    type: postgres
    connection:
      host:              db.example.com
      port:              5432
      database:          production
      user:              cdc_user
      password:          ${PG_PASSWORD}
      replication_slot:  dremio_cdc    # auto-created if absent
      publication:       dremio_cdc    # auto-created if absent
    tables:
      - public.orders
      - public.customers
```

</details>

### MySQL

```sql
-- Verify row-based binlog (required)
SHOW VARIABLES LIKE 'binlog_format';   -- must be ROW

-- Grant CDC permissions
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON production.* TO 'cdc_user'@'%';
```

**In the UI:** Sources → Add Source → MySQL. Enter host, port, database, user, password, and server ID (any unique integer, e.g. `1`).

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: mysql_prod
    type: mysql
    connection:
      host:       db.example.com
      port:       3306
      database:   production
      user:       cdc_user
      password:   ${MYSQL_PASSWORD}
      server_id:  1
    tables:
      - production.orders
```

</details>

### MariaDB

MariaDB uses the same binlog replication protocol as MySQL. Setup is identical.

```sql
-- Verify row-based binlog (required)
SHOW VARIABLES LIKE 'binlog_format';   -- must be ROW

-- Grant CDC permissions
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON production.* TO 'cdc_user'@'%';
```

**In the UI:** Sources → Add Source → MariaDB. Enter host, port, database, user, password, and server ID.

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: mariadb_prod
    type: mariadb
    connection:
      host:       db.example.com
      port:       3306
      database:   production
      user:       cdc_user
      password:   ${MARIADB_PASSWORD}
      server_id:  1
    tables:
      - production.orders
```

</details>

### Snowflake

Snowflake CDC uses native **STREAM** objects — no Debezium or external tooling required. The daemon creates a stream on each watched table automatically.

```sql
-- Grant required privileges to the CDC role
GRANT USAGE  ON WAREHOUSE <wh>     TO ROLE <role>;
GRANT USAGE  ON DATABASE  <db>     TO ROLE <role>;
GRANT USAGE  ON SCHEMA    <schema> TO ROLE <role>;
GRANT SELECT, REFERENCES ON TABLE <table> TO ROLE <role>;
GRANT CREATE STREAM ON SCHEMA <schema> TO ROLE <role>;
```

**In the UI:** Sources → Add Source → Snowflake. Enter account identifier, user, password, database, schema, warehouse, and role.

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: snowflake_prod
    type: snowflake
    connection:
      account:       xy12345.us-east-1
      user:          cdc_user
      password:      ${SNOWFLAKE_PASSWORD}
      database:      PRODUCTION
      schema:        PUBLIC
      warehouse:     COMPUTE_WH
      role:          CDC_ROLE        # optional
      poll_interval: 30              # seconds between stream checks
    tables:
      - PUBLIC.ORDERS
      - PUBLIC.CUSTOMERS
```

</details>

### MongoDB

```bash
# Single-node replica set (if not already configured)
mongod --replSet rs0
mongosh --eval "rs.initiate()"
```

**In the UI:** Sources → Add Source → MongoDB. Enter the connection URI and database name.

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: mongo_prod
    type: mongodb
    connection:
      uri:      mongodb://cdc_user:${MONGO_PASSWORD}@mongo.example.com:27017/?directConnection=true
      database: production
    tables:
      - customers
      - orders
```

</details>

### DynamoDB

```bash
# Enable streams on each table
aws dynamodb update-table \
  --table-name Orders \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

**In the UI:** Sources → Add Source → DynamoDB. Enter region and AWS credentials (or leave credentials blank to use the instance role / environment).

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: dynamo_prod
    type: dynamodb
    connection:
      region:                us-east-1
      aws_access_key_id:     ${AWS_ACCESS_KEY_ID}
      aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    tables:
      - Orders
      - Customers
```

</details>

### Oracle

<a name="oracle"></a>

Oracle CDC uses **Debezium Server** as an adapter. Debezium connects to Oracle's LogMiner and POSTs change events to the CDC daemon's HTTP endpoint.

**1. Enable Oracle prerequisites (run once as DBA):**

```sql
-- Enable ARCHIVELOG mode
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
cp debezium/oracle.properties debezium/application.properties
# Edit: database.hostname, database.user, database.password, database.dbname

docker run -d --name debezium-oracle \
  -v $(pwd)/debezium/application.properties:/debezium/conf/application.properties \
  --add-host host.docker.internal:host-gateway \
  debezium/server:2.7.3.Final
```

**3. In the UI:** Sources → Add Source → Oracle (Debezium). Enter the listen port that Debezium is posting to (default `8765`) and the tables to watch.

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: oracle_prod
    type: oracle
    listen_port: 8765
    tables:
      - HR.EMPLOYEES
      - HR.DEPARTMENTS
```

</details>

Pre-built Debezium configs are included:
- `debezium/oracle.properties` — Oracle production template
- `debezium/sqlserver.properties`
- `debezium/db2.properties`

### SQL Server

```sql
-- Enable CDC on the database and tables (requires sysadmin)
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='Orders',
     @role_name=NULL, @supports_net_changes=1;
```

**In the UI:** Sources → Add Source → SQL Server. Enter host, port, database, user, and password.

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: sqlserver_prod
    type: sqlserver
    connection:
      host:     db.example.com
      port:     1433
      database: production
      user:     cdc_user
      password: ${MSSQL_PASSWORD}
    tables:
      - dbo.Orders
```

</details>

---

## PII masking

Configure column-level masking in the UI under **Mappings** — select a table, click a column, and choose the masking function. Masking is applied before any event reaches Dremio.

Available functions: `redact` (→ `[REDACTED]`), `hash_sha256`, `hash_md5`, `mask` (→ `***`), `nullify` (→ `NULL`), `tokenize` (→ `tok_<hex16>`)

<details>
<summary>Headless YAML</summary>

```yaml
sources:
  - name: pg_prod
    type: postgres
    masking:
      public.users:
        email:  hash_sha256
        ssn:    redact
        name:   mask
        phone:  nullify
        card:   tokenize
```

</details>

---

## Alerting

Configure alert thresholds and notification channels in the UI under **Alerts**. Supported channels: Slack, email (SMTP), and generic webhook.

<details>
<summary>Headless YAML</summary>

```yaml
alerts:
  enabled: true
  lag_threshold_seconds:  60
  error_count_threshold:  5
  cooldown_seconds:       300
  channels:
    - type: slack
      webhook_url: ${SLACK_WEBHOOK_URL}
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

</details>

---

## Advanced features

### Adaptive batching

Automatically tunes batch size based on throughput and lag — smaller batches for low latency, larger for high throughput.

**In the UI:** Settings → Batching → enable Adaptive Batching; set min/max batch size.

### Schema drift detection

Detects when source tables add, remove, or change column types. `auto_migrate` issues `ALTER TABLE ADD COLUMN` on the Dremio target automatically.

**In the UI:** Settings → Schema drift action (`Alert` / `Auto-migrate` / `Pause`).

### Incremental snapshot

Reads large tables in PK-ordered chunks instead of a single full scan.

**In the UI:** Settings → Snapshot mode → Incremental; set chunk size and cursor column.

### Dead letter queue

Failed events park to SQLite and are retried automatically. Browse and replay from the **DLQ** page in the UI.

### PostgreSQL offset store (multi-instance)

By default offsets are stored in a local SQLite file. For multi-instance or Kubernetes deployments, switch to a shared PostgreSQL store.

**In the UI:** Settings → Offset store → enter a `postgresql://` connection string.

```sql
-- Run once
CREATE USER cdc_user WITH PASSWORD 'cdc_pass';
CREATE DATABASE cdc_offsets;
GRANT ALL PRIVILEGES ON DATABASE cdc_offsets TO cdc_user;
```

The `cdc_offsets` table is created automatically on first run.

> **Note:** The Dead Letter Queue always uses a local SQLite file — DLQ entries are per-process by design.

### Secrets management

#### Environment variables

Use `${VAR}` anywhere in the config or UI credential fields. Set secrets at runtime:

```bash
docker run ... \
  -e DREMIO_PAT=eyJ... \
  -e PG_PASSWORD=secret \
  -e AWS_ACCESS_KEY_ID=AKIA... \
  mshainman/dremio-cdc:latest
```

#### HashiCorp Vault (KV v2)

Use `vault:path#field` references in any credential field. Configure the Vault connection under Settings → Secrets.

<details>
<summary>Headless YAML</summary>

```yaml
secrets:
  vault:
    url:         https://vault.example.com
    auth_method: token                   # token (default) or approle
    token:       ${VAULT_TOKEN}
    mount:       secret
    namespace:   ""                      # Vault Enterprise only

sources:
  - name: prod_pg
    type: postgres
    connection:
      password: vault:secret/prod/postgres#password
```

</details>

Install the Vault client: `pip install hvac`

### Prometheus metrics

```
GET http://localhost:7070/metrics
```

Each worker emits: `cdc_lag_seconds`, `cdc_events_total`, `cdc_errors_total`, `cdc_flush_duration_seconds`, `cdc_batch_size`.

---

## Docker deployment

### Without a config file (UI-configured)

```bash
docker run -d \
  --name dremio-cdc \
  -p 7070:7070 \
  -v $(pwd)/cdc_data:/app/data \
  mshainman/dremio-cdc:latest
```

Open `http://localhost:7070` and configure everything in the UI. The UI saves config to the mounted data volume.

### With a config file (headless)

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

### Docker Compose (dev/test environment)

```bash
# Starts Postgres, MySQL, MongoDB, DynamoDB (LocalStack), SQL Server, Oracle,
# Debezium Server, and a local Iceberg REST catalog
docker compose up -d

python main.py --ui
```

---

## Headless YAML reference

> The sections below document the full YAML format for deployments that run without the UI (CI pipelines, Kubernetes, infrastructure-as-code). All values support `${ENV_VAR}` expansion.

### Minimal — Mode A (Dremio SQL)

```yaml
dremio:
  host:             localhost
  port:             9047
  user:             admin
  password:         ${DREMIO_PASSWORD}
  target_namespace: cdc

sources:
  - name: my_postgres
    type: postgres
    connection:
      host:     db.example.com
      database: production
      user:     cdc_user
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
      host:     db.example.com
      database: production
      user:     cdc_user
      password: ${PG_PASSWORD}
    tables:
      - public.orders

options:
  sink_mode: iceberg

iceberg:
  type:             rest
  uri:              https://catalog.dremio.cloud/api/iceberg
  token:            ${DREMIO_PAT}
  warehouse:        my-project-name
  target_namespace: cdc
  write_mode:       merge    # "merge" (upsert) or "append" (event log)
```

### Dremio Cloud — Mode A with SQL API

```yaml
dremio:
  host:       api.dremio.cloud
  port:       443
  ssl:        true
  pat:        ${DREMIO_PAT}
  project_id: 957704f5-4495-42ad-94de-671bf7790610

sources:
  - name: my_postgres
    type: postgres
    ...
```

### Full options reference

```yaml
options:
  sink_mode:                  dremio    # "dremio" | "iceberg"
  batch_size:                 500
  batch_timeout_seconds:      10
  snapshot_on_first_run:      true
  incremental_snapshot:       false
  snapshot_chunk_size:        5000
  snapshot_cursor_column:     id
  adaptive_batching:          true
  min_batch_size:             100
  max_batch_size:             5000
  offset_db_path:             ./cdc_offsets.db   # SQLite; or postgres:// for multi-process
  schema_drift_action:        alert              # "alert" | "auto_migrate" | "pause"
  dlq_db_path:                ./cdc_dlq.db
  dlq_max_retries:            3
```

---

## Development

### Local test environment

```bash
# Start all test databases
docker compose up -d

# Run tests (unit + integration; excludes cloud and oracle live tests)
python3 -m pytest tests/test_e2e.py -m "not cloud and not ui and not oracle" -v

# Run Oracle live tests
python3 -m pytest tests/test_e2e.py -m oracle -v

# Run DB2 live tests
python3 -m pytest tests/test_e2e.py -m db2 -v
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
│   ├── mariadb.py              # MariaDB binlog (subclass of MySQLSource)
│   ├── mongodb.py              # MongoDB Change Streams
│   ├── dynamodb.py             # DynamoDB Streams via boto3
│   ├── debezium.py             # HTTP adapter for Debezium Server (Oracle, SQL Server, DB2)
│   ├── sqlserver.py            # SQL Server CDC via pyodbc
│   ├── snowflake_src.py        # Snowflake native STREAM objects
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
    ├── test_e2e.py             # Integration test suite (135+ tests)
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

Register in `core/engine.py` inside `_load_sources()`:

```python
from sources.my_source import MySource
register_source("mysource", MySource)
```

---

## License

Apache 2.0

## Author

Mark Shainman
