# Debezium Server with IBM Db2 JDBC driver
# The DB2 connector plugin is included in debezium/server 3.x but the
# proprietary IBM JDBC driver must be added separately (available on Maven Central).
FROM --platform=linux/amd64 debezium/server:3.0.0.Final
USER root
RUN curl -sL \
    "https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar" \
    -o /debezium/lib/db2jcc4.jar
USER jboss
