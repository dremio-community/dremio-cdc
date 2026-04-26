"""
MariaDB CDC source — uses the MySQL binlog protocol via python-mysql-replication.
MariaDB 10.2+ is fully compatible with the MySQL binlog replication protocol,
so this class inherits MySQLSource unchanged.

Setup (run once in MariaDB):
    SET GLOBAL binlog_format = 'ROW';
    SET GLOBAL binlog_row_image = 'FULL';
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
"""
from __future__ import annotations

from sources.mysql import MySQLSource


class MariaDBSource(MySQLSource):
    """CDC source for MariaDB — identical protocol to MySQL binlog replication."""
