#!/bin/bash
# Oracle Free 23c init script — runs inside the container after DB creation.
# Enables ARCHIVELOG, supplemental logging, creates CDC user + test schema.
set -e

echo ">>> Oracle CDC setup: enabling ARCHIVELOG (if not already enabled)..."
ARCHIVELOG_STATUS=$(sqlplus -s / as sysdba <<-EOSQL
    SET HEADING OFF FEEDBACK OFF
    SELECT log_mode FROM v\$database;
    EXIT;
EOSQL
)
if echo "$ARCHIVELOG_STATUS" | grep -qE "^\s*ARCHIVELOG\s*$"; then
    echo ">>> ARCHIVELOG already enabled, skipping."
else
    sqlplus / as sysdba <<-EOSQL
        SHUTDOWN IMMEDIATE;
        STARTUP MOUNT;
        ALTER DATABASE ARCHIVELOG;
        ALTER DATABASE OPEN;
        ALTER PLUGGABLE DATABASE ALL OPEN READ WRITE;
EOSQL
fi

echo ">>> Enabling supplemental logging..."
sqlplus / as sysdba <<-EOSQL
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
EOSQL

echo ">>> Creating LogMiner user c##dbzuser..."
sqlplus / as sysdba <<-EOSQL
    CREATE USER c##dbzuser IDENTIFIED BY dbzpass
        DEFAULT TABLESPACE users QUOTA UNLIMITED ON users
        CONTAINER=ALL;
    GRANT CREATE SESSION                  TO c##dbzuser CONTAINER=ALL;
    GRANT LOGMINING                       TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$DATABASE          TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOG               TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGFILE           TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGMNR_CONTENTS   TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$ARCHIVED_LOG      TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON V_\$TRANSACTION       TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT_CATALOG_ROLE             TO c##dbzuser CONTAINER=ALL;
    GRANT EXECUTE ON DBMS_LOGMNR          TO c##dbzuser CONTAINER=ALL;
    GRANT EXECUTE ON DBMS_LOGMNR_D        TO c##dbzuser CONTAINER=ALL;
    -- Snapshot privileges
    GRANT SELECT ANY TABLE                TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ANY TRANSACTION          TO c##dbzuser CONTAINER=ALL;
    GRANT FLASHBACK ANY TABLE             TO c##dbzuser CONTAINER=ALL;
    GRANT CREATE TABLE                    TO c##dbzuser CONTAINER=ALL;
    GRANT LOCK ANY TABLE                  TO c##dbzuser CONTAINER=ALL;
    GRANT SELECT ON DBA_TABLESPACES       TO c##dbzuser CONTAINER=ALL;
    GRANT SET CONTAINER                   TO c##dbzuser CONTAINER=ALL;
EOSQL

echo ">>> Creating test schema in FREEPDB1..."
sqlplus sys/oracle@//localhost/FREEPDB1 as sysdba <<-EOSQL
    CREATE USER cdc_test IDENTIFIED BY cdc_test
        DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
    GRANT CREATE SESSION TO cdc_test;
    GRANT CREATE TABLE   TO cdc_test;

    CREATE TABLE cdc_test.employees (
        id     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        name   VARCHAR2(100) NOT NULL,
        email  VARCHAR2(100),
        salary NUMBER(10,2)
    );

    INSERT INTO cdc_test.employees (name, email, salary) VALUES ('Alice',   'alice@example.com',   75000);
    INSERT INTO cdc_test.employees (name, email, salary) VALUES ('Bob',     'bob@example.com',     85000);
    INSERT INTO cdc_test.employees (name, email, salary) VALUES ('Charlie', 'charlie@example.com', 95000);
    COMMIT;

    GRANT SELECT ON cdc_test.employees TO c##dbzuser;
EOSQL

echo ">>> Oracle CDC setup complete."
