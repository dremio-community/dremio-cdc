#!/bin/bash
# IBM DB2 CDC setup script — run by ibmcom/db2 via /var/custom/ hook after DB creation.
# Enables archive logging, takes online backup (baseline), creates test table, seeds data,
# sets up ASN CDC (ASNCDC schema + EMPLOYEES registration), and starts asncap.
set -e

echo ">>> DB2 CDC setup: enabling archive logging..."
su - db2inst1 -c "db2 connect to TESTDB && db2 'UPDATE DB CFG FOR TESTDB USING LOGARCHMETH1 LOGRETAIN' && db2 terminate"

echo ">>> Taking online backup (baseline required after enabling LOGRETAIN)..."
mkdir -p /tmp/db2backup
chown db2inst1:db2iadm1 /tmp/db2backup
su - db2inst1 -c "db2 'BACKUP DATABASE TESTDB ONLINE TO /tmp/db2backup'"

echo ">>> Creating test schema..."
su - db2inst1 -c "db2 connect to TESTDB && db2 \"CREATE TABLE DB2INST1.EMPLOYEES (ID INTEGER NOT NULL, NAME VARCHAR(100), EMAIL VARCHAR(100), SALARY DECIMAL(10,2), PRIMARY KEY (ID))\" && db2 commit && db2 terminate"

echo ">>> Seeding test data..."
su - db2inst1 -c "db2 connect to TESTDB && db2 \"INSERT INTO DB2INST1.EMPLOYEES VALUES (1, 'Alice', 'alice@example.com', 75000.00)\" && db2 commit && db2 terminate"
su - db2inst1 -c "db2 connect to TESTDB && db2 \"INSERT INTO DB2INST1.EMPLOYEES VALUES (2, 'Bob', 'bob@example.com', 85000.00)\" && db2 commit && db2 terminate"
su - db2inst1 -c "db2 connect to TESTDB && db2 \"INSERT INTO DB2INST1.EMPLOYEES VALUES (3, 'Charlie', 'charlie@example.com', 95000.00)\" && db2 commit && db2 terminate"

echo ">>> Creating ASNCDC schema and control tables for Debezium CDC..."
su - db2inst1 -c "db2 connect to TESTDB && db2 'CREATE SCHEMA ASNCDC' && db2 terminate" || true

# Create ASN control tables in ASNCDC schema (modified from asnctlw.sql)
sed 's/ASN\./ASNCDC./g; s/TSASN/TSASNCDC/g' /opt/ibm/db2/V11.5/samples/repl/sql/asnctlw.sql > /tmp/asnctlw_asncdc.sql
su - db2inst1 -c "db2 connect to TESTDB && db2 -tvf /tmp/asnctlw_asncdc.sql > /tmp/asncdc_setup.log 2>&1; db2 terminate" || true

echo ">>> Configuring CAPPARMS..."
su - db2inst1 -c "db2 connect to TESTDB && db2 'DELETE FROM ASNCDC.IBMSNAP_CAPPARMS' && db2 \"INSERT INTO ASNCDC.IBMSNAP_CAPPARMS (ARCH_LEVEL, COMPATIBILITY, AUTOPRUNE, TERM, AUTOSTOP, LOGREUSE, LOGSTDOUT, SLEEP_INTERVAL) VALUES ('1021', '1021', 'Y', 'N', 'N', 'N', 'N', 5)\" && db2 commit && db2 terminate"

echo ">>> Enabling DATA CAPTURE CHANGES on EMPLOYEES..."
su - db2inst1 -c "db2 connect to TESTDB && db2 'ALTER TABLE DB2INST1.EMPLOYEES DATA CAPTURE CHANGES' && db2 commit && db2 terminate"

echo ">>> Registering EMPLOYEES with ASN Capture (creates CD table via asnclp)..."
cat > /tmp/register_employees.asnclp << 'ASNEOF'
ASNCLP SESSION SET TO SQL REPLICATION;
SET SERVER CAPTURE TO DB TESTDB;
SET CAPTURE SCHEMA SOURCE ASNCDC;
SET RUN SCRIPT NOW STOP ON SQL ERROR ON;
CREATE REGISTRATION (DB2INST1.EMPLOYEES FULL DIFFERENTIAL REFRESH;);
ASNEOF
su - db2inst1 -c "asnclp -f /tmp/register_employees.asnclp 2>&1" || true

echo ">>> Activating EMPLOYEES registration..."
su - db2inst1 -c "db2 connect to TESTDB && db2 \"UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE='A' WHERE SOURCE_OWNER='DB2INST1' AND SOURCE_TABLE='EMPLOYEES'\" && db2 commit && db2 terminate"

echo ">>> Creating ASNCDC view for CD table (Debezium expects ASNCDC schema)..."
# IBM DB2 LUW ASNCAP stores UPDATEs as 'U' (after-image only) and before-image UPDATEs
# as 'X'. Debezium 3.0 LUW connector only handles 'D'+'I' pairs; 'U'/'X' rows produce
# NULL OPCODE and crash the connector. The view maps 'U'/'X' -> 'I' so update rows are
# delivered as INSERT events without crashing. Tests that need real UPDATE detection use
# DELETE+INSERT in the same commit to generate the D+I pair Debezium expects.
su - db2inst1 -c "db2 connect to TESTDB && db2 \"CREATE VIEW ASNCDC.CDEMPLOYEES AS SELECT IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ, CASE WHEN IBMSNAP_OPERATION IN (CHAR(CHR(85)), CHAR(CHR(88))) THEN CHAR(CHR(73)) ELSE IBMSNAP_OPERATION END AS IBMSNAP_OPERATION, ID, NAME, EMAIL, SALARY FROM DB2INST1.CDEMPLOYEES\" && db2 commit && db2 terminate" || true

echo ">>> Starting ASN Capture daemon..."
nohup su - db2inst1 -c "asncap capture_server=TESTDB capture_schema=ASNCDC capture_path=/tmp AUTOSTOP=N" > /tmp/db2inst1.TESTDB.ASNCDC.CAP.log 2>&1 &
sleep 5

echo ">>> DB2 CDC setup complete."
