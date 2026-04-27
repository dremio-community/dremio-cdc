"""
Oracle CDC source — uses Oracle LogMiner (V$LOGMNR_CONTENTS) for change data capture.

Setup (run once as SYSDBA):
    -- 1. Enable supplemental logging
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
    -- Or per-table (preferred for large databases):
    ALTER TABLE HR.EMPLOYEES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

    -- 2. Create CDC user
    CREATE USER cdcuser IDENTIFIED BY cdcpass;
    GRANT CREATE SESSION TO cdcuser;
    GRANT LOGMINING TO cdcuser;                        -- 12c+
    GRANT EXECUTE ON DBMS_LOGMNR TO cdcuser;
    GRANT SELECT ON V_$LOGMNR_CONTENTS TO cdcuser;
    GRANT SELECT ON V_$DATABASE TO cdcuser;
    GRANT SELECT ANY DICTIONARY TO cdcuser;
    GRANT SELECT ON HR.EMPLOYEES TO cdcuser;           -- per table for snapshot

Driver:
    pip install python-oracledb    # thin mode — no Oracle Instant Client required
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import CDCSource

logger = logging.getLogger(__name__)

_OP_MAP = {
    1: Operation.INSERT,
    2: Operation.DELETE,
    3: Operation.UPDATE,
}

_TYPE_MAP = {
    "NUMBER":                           "numeric",
    "FLOAT":                            "double",
    "BINARY_FLOAT":                     "float",
    "BINARY_DOUBLE":                    "double",
    "VARCHAR2":                         "varchar",
    "NVARCHAR2":                        "varchar",
    "CHAR":                             "varchar",
    "NCHAR":                            "varchar",
    "CLOB":                             "text",
    "NCLOB":                            "text",
    "LONG":                             "text",
    "DATE":                             "timestamp",
    "TIMESTAMP":                        "timestamp",
    "TIMESTAMP WITH TIME ZONE":         "timestamp",
    "TIMESTAMP WITH LOCAL TIME ZONE":   "timestamp",
    "INTERVAL YEAR TO MONTH":           "varchar",
    "INTERVAL DAY TO SECOND":           "varchar",
    "RAW":                              "bytea",
    "LONG RAW":                         "bytea",
    "BLOB":                             "bytea",
    "XMLTYPE":                          "varchar",
    "SDO_GEOMETRY":                     "varchar",
}

# LogMiner OPTIONS bitmask: use online catalog + committed data only + no ROWID in statements
_LOGMNR_OPTIONS = (
    1        # DICT_FROM_ONLINE_CATALOG
    + 4      # COMMITTED_DATA_ONLY
    + 4096   # NO_ROWID_IN_STMT (11.2.0.3+)
)


def _connect(cfg: Dict) -> Any:
    try:
        import oracledb as cx
    except ImportError:
        try:
            import cx_Oracle as cx
        except ImportError:
            raise SystemExit("Oracle driver required: pip install python-oracledb")

    host    = cfg.get("host", "localhost")
    port    = int(cfg.get("port", 1521))
    svc     = cfg.get("service_name") or cfg.get("database") or "ORCL"
    user    = cfg["user"]
    pw      = cfg.get("password", "")
    dsn     = cfg.get("dsn") or f"{host}:{port}/{svc}"
    return cx.connect(user=user, password=pw, dsn=dsn)


def _split(table: str) -> Tuple[str, str]:
    parts = table.upper().split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("HR", parts[0])


def _quote(name: str) -> str:
    return f'"{name}"'


# ── SQL_REDO / SQL_UNDO parsers ────────────────────────────────────────────────

def _csv_split(s: str) -> List[str]:
    """Split comma-separated Oracle SQL values, respecting single-quoted strings."""
    tokens: List[str] = []
    buf: List[str] = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c == "'":
            buf.append(c); i += 1
            while i < n:
                c2 = s[i]; buf.append(c2); i += 1
                if c2 == "'":
                    if i < n and s[i] == "'":
                        buf.append(s[i]); i += 1  # escaped ''
                    else:
                        break
        elif c == ',':
            tokens.append(''.join(buf).strip()); buf = []; i += 1
        else:
            buf.append(c); i += 1
    if buf:
        tokens.append(''.join(buf).strip())
    return tokens


def _and_split(s: str) -> List[str]:
    """Split AND-separated Oracle WHERE conditions, respecting quoted strings."""
    tokens: List[str] = []
    buf: List[str] = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c == "'":
            buf.append(c); i += 1
            while i < n:
                c2 = s[i]; buf.append(c2); i += 1
                if c2 == "'":
                    if i < n and s[i] == "'":
                        buf.append(s[i]); i += 1
                    else:
                        break
        elif s[i:i+5].upper() == ' AND ':
            tokens.append(''.join(buf).strip()); buf = []; i += 5
        else:
            buf.append(c); i += 1
    if buf:
        tokens.append(''.join(buf).strip())
    return tokens


def _find_kw(sql: str, kw: str) -> int:
    """Find keyword in SQL at word boundary, ignoring quoted strings."""
    kn = len(kw)
    i, n = 0, len(sql)
    while i <= n - kn:
        if sql[i] == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    i += 1
                    if i < n and sql[i] == "'":
                        i += 1
                    else:
                        break
                else:
                    i += 1
        elif sql[i:i+kn].upper() == kw.upper():
            pre  = i == 0 or not (sql[i-1].isalnum() or sql[i-1] == '_')
            post = i+kn >= n or not (sql[i+kn].isalnum() or sql[i+kn] == '_')
            if pre and post:
                return i
            else:
                i += 1
        else:
            i += 1
    return -1


def _parse_val(s: str) -> Any:
    s = s.strip()
    if not s or s.upper() == 'NULL':
        return None
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("''", "'")
    if s.upper().startswith("HEXTORAW("):
        return s
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def _parse_col_list(s: str) -> List[str]:
    s = s.strip().strip('()')
    return [c.strip().strip('"') for c in s.split(',')]


def _parse_insert(sql: str) -> Dict[str, Any]:
    """insert into "S"."T"("C1","C2") values ('v1', 'v2');"""
    m = re.match(
        r'insert\s+into\s+"[^"]+"\."[^"]+"\s*(\([^)]+\))\s+values\s*\((.*)\)\s*;?\s*$',
        sql.strip(), re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return {}
    cols = _parse_col_list(m.group(1))
    vals = [_parse_val(v) for v in _csv_split(m.group(2))]
    return dict(zip(cols, vals))


def _parse_set(sql: str) -> Dict[str, Any]:
    """update "S"."T" set "C1"='v1', "C2"=2 where ..."""
    set_pos = _find_kw(sql, ' SET ')
    where_pos = _find_kw(sql, ' WHERE ')
    if set_pos < 0:
        return {}
    end = where_pos if where_pos > set_pos else len(sql)
    set_part = sql[set_pos + 5:end].strip().rstrip(';')
    result: Dict[str, Any] = {}
    for item in _csv_split(set_part):
        eq = item.find('=')
        if eq < 0:
            continue
        col = item[:eq].strip().strip('"')
        val = _parse_val(item[eq+1:].strip())
        if col.upper() != 'ROWID':
            result[col] = val
    return result


def _parse_where(sql: str) -> Dict[str, Any]:
    """Extract column=value pairs from WHERE clause."""
    where_pos = _find_kw(sql, ' WHERE ')
    if where_pos < 0:
        return {}
    where_part = sql[where_pos + 7:].strip().rstrip(';')
    result: Dict[str, Any] = {}
    for cond in _and_split(where_part):
        cond = cond.strip()
        if _find_kw(cond, ' IS NULL') >= 0:
            col = cond[:_find_kw(cond, ' IS NULL')].strip().strip('"')
            if col.upper() != 'ROWID':
                result[col] = None
            continue
        eq = cond.find('=')
        if eq < 0:
            continue
        col = cond[:eq].strip().strip('"')
        val = _parse_val(cond[eq+1:].strip())
        if col.upper() != 'ROWID':
            result[col] = val
    return result


def _parse_logminer_row(
    op_code: int, sql_redo: Optional[str], sql_undo: Optional[str]
) -> Tuple[Optional[Dict], Optional[Dict]]:
    try:
        if op_code == 1:   # INSERT
            return _parse_insert(sql_redo or ""), None
        if op_code == 2:   # DELETE — undo is the corresponding INSERT
            return None, _parse_insert(sql_undo or "")
        if op_code == 3:   # UPDATE
            after  = _parse_set(sql_redo or "")
            before = _parse_set(sql_undo or "")
            return after or None, before or None
    except Exception as exc:
        logger.debug("SQL parse error op=%d: %s | %s", op_code, exc,
                     (sql_redo or "")[:120])
    return None, None


class OracleSource(CDCSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._conn_cfg: Dict = {}

    def connect(self):
        conn_cfg = self.cfg.get("connection", self.cfg)
        missing = [k for k in ("host", "user") if not conn_cfg.get(k)]
        if missing:
            raise ValueError(f"Missing Oracle connection fields: {', '.join(missing)}")
        if not conn_cfg.get("service_name") and not conn_cfg.get("database") and not conn_cfg.get("dsn"):
            raise ValueError("Oracle connection requires service_name (or database/dsn)")
        self._conn_cfg = conn_cfg
        conn = _connect(conn_cfg)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM DUAL")
            cur.close()
        finally:
            conn.close()
        logger.info("Connected to Oracle %s", conn_cfg.get("host"))

    def _new_conn(self):
        return _connect(self._conn_cfg)

    def get_schema(self, table: str) -> List[ColumnSchema]:
        schema_name, table_name = _split(table)
        conn = self._new_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT c.COLUMN_NAME, c.DATA_TYPE, "
                "  CASE WHEN p.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PK "
                "FROM ALL_TAB_COLUMNS c "
                "LEFT JOIN ("
                "  SELECT cc.COLUMN_NAME FROM ALL_CONSTRAINTS cn "
                "  JOIN ALL_CONS_COLUMNS cc ON cn.CONSTRAINT_NAME = cc.CONSTRAINT_NAME "
                "    AND cn.OWNER = cc.OWNER "
                "  WHERE cn.CONSTRAINT_TYPE = 'P' AND cn.OWNER = :1 AND cn.TABLE_NAME = :2"
                ") p ON c.COLUMN_NAME = p.COLUMN_NAME "
                "WHERE c.OWNER = :3 AND c.TABLE_NAME = :4 "
                "ORDER BY c.COLUMN_ID",
                [schema_name, table_name, schema_name, table_name],
            )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        return [
            ColumnSchema(
                name=row[0],
                data_type=_TYPE_MAP.get(row[1].upper().split("(")[0], "varchar"),
                primary_key=bool(row[2]),
            )
            for row in rows
        ]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        schema_name, table_name = _split(table)
        conn = self._new_conn()
        try:
            cur = conn.cursor()
            cur.arraysize = 2000
            cur.execute(
                f'SELECT {", ".join(_quote(c) for c in col_names)} '
                f'FROM {_quote(schema_name)}.{_quote(table_name)}'
            )
            while True:
                rows = cur.fetchmany(2000)
                if not rows:
                    break
                for row in rows:
                    yield ChangeEvent(
                        op=Operation.SNAPSHOT,
                        source_name=self.name,
                        source_table=table,
                        schema=schema,
                        before=None,
                        after=dict(zip(col_names, row)),
                        timestamp=datetime.now(timezone.utc),
                        offset=None,
                    )
            cur.close()
        finally:
            conn.close()

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        col_names = [c.name for c in schema]
        schema_name, table_name = _split(table)
        conn = self._new_conn()
        try:
            cur = conn.cursor()
            if start_after is None:
                cur.execute(
                    f'SELECT {", ".join(_quote(c) for c in col_names)} '
                    f'FROM {_quote(schema_name)}.{_quote(table_name)} '
                    f'ORDER BY {_quote(cursor_col)} '
                    f'FETCH FIRST :1 ROWS ONLY',
                    [chunk_size],
                )
            else:
                cur.execute(
                    f'SELECT {", ".join(_quote(c) for c in col_names)} '
                    f'FROM {_quote(schema_name)}.{_quote(table_name)} '
                    f'WHERE {_quote(cursor_col)} > :1 '
                    f'ORDER BY {_quote(cursor_col)} '
                    f'FETCH FIRST :2 ROWS ONLY',
                    [start_after, chunk_size],
                )
            for row in cur.fetchall():
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    schema=schema,
                    before=None,
                    after=dict(zip(col_names, row)),
                    timestamp=datetime.now(timezone.utc),
                    offset=None,
                )
            cur.close()
        finally:
            conn.close()

    def stream(self, table: str, offset: Optional[Any]) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        schema_name, table_name = _split(table)
        poll_interval = int(self._conn_cfg.get("poll_interval", 5))

        # Initialise SCN from saved offset
        current_scn: Optional[int] = None
        raw = offset if (offset and not str(offset).startswith("snap:")) else None
        if raw is not None:
            try:
                current_scn = int(raw)
            except (ValueError, TypeError):
                current_scn = None

        if current_scn is None:
            conn = self._new_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                row = cur.fetchone()
                current_scn = row[0] if row else 0
                cur.close()
            finally:
                conn.close()

        while True:
            conn = self._new_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
                row = cur.fetchone()
                max_scn = row[0] if row else current_scn
                cur.close()

                if max_scn and current_scn and max_scn > current_scn:
                    logmnr_started = False
                    try:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            BEGIN
                                DBMS_LOGMNR.START_LOGMNR(
                                    STARTSCN => :1,
                                    ENDSCN   => :2,
                                    OPTIONS  => :3
                                );
                            END;
                            """,
                            [current_scn, max_scn, _LOGMNR_OPTIONS],
                        )
                        logmnr_started = True
                        cur.execute(
                            """
                            SELECT SCN, OPERATION_CODE, SQL_REDO, SQL_UNDO
                            FROM   V$LOGMNR_CONTENTS
                            WHERE  OPERATION_CODE IN (1, 2, 3)
                              AND  SEG_OWNER     = :1
                              AND  TABLE_NAME    = :2
                              AND  (CSF = 0 OR CSF IS NULL)
                            ORDER BY SCN, RS_ID, SSN
                            """,
                            [schema_name, table_name],
                        )
                        for row in cur.fetchall():
                            scn, op_code, sql_redo, sql_undo = row
                            op = _OP_MAP.get(op_code)
                            if op is None:
                                continue
                            after, before = _parse_logminer_row(op_code, sql_redo, sql_undo)
                            yield ChangeEvent(
                                op=op,
                                source_name=self.name,
                                source_table=table,
                                schema=schema,
                                before=before,
                                after=after,
                                timestamp=datetime.now(timezone.utc),
                                offset=str(scn),
                            )
                        cur.close()
                        current_scn = max_scn
                    except Exception as exc:
                        logger.error("LogMiner query error for %s: %s", table, exc)
                    finally:
                        if logmnr_started:
                            try:
                                end_cur = conn.cursor()
                                end_cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;")
                                end_cur.close()
                            except Exception:
                                pass
            except Exception as exc:
                logger.warning("[%s] Oracle poll error: %s", table, exc)
            finally:
                conn.close()

            time.sleep(poll_interval)

    def close(self):
        pass  # No persistent connection to close
