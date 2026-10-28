"""Connections to the source (SQL Server) and the warehouse (DuckDB)."""

import json
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal

import duckdb
import pymssql

DATABASE = "fundo"
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "data/warehouse.duckdb")
_PASSWORD_ENV = {"sa": "MSSQL_SA_PASSWORD", "etl_reader": "ETL_READER_PASSWORD"}


def mssql(user: str, database: str = DATABASE, autocommit: bool = False) -> pymssql.Connection:
    """Open a SQL Server connection. TDS 7.4 returns DATE and DATETIME2 as Python date/datetime."""
    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "sqlserver"),
        user=user,
        password=os.environ[_PASSWORD_ENV[user]],
        database=database,
        autocommit=autocommit,
        tds_version="7.4",
        login_timeout=5,
    )


def sql_param(value):
    """Send dates and decimals to SQL Server as text so no precision is lost on the way."""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, Decimal)):
        return str(value)
    return value


def warehouse(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """check and the tests open it read-only, so they can never modify the warehouse."""
    if not read_only:
        os.makedirs(os.path.dirname(WAREHOUSE_PATH), exist_ok=True)
    return duckdb.connect(WAREHOUSE_PATH, read_only=read_only)


def insert_rows(wh: duckdb.DuckDBPyConnection, table: str, columns: dict[str, str], rows: list[tuple]) -> None:
    """Bulk-insert tuples (in `columns` order) through a JSON-lines file; keeps NULL and '' distinct."""
    if not rows:
        return
    names = list(columns)
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(dict(zip(names, row, strict=True)), default=str) + "\n")
    try:
        wh.execute(
            f"INSERT INTO {table} ({', '.join(names)}) "
            f"SELECT {', '.join(names)} FROM read_json(?, format = 'newline_delimited', columns = ?)",
            [f.name, columns],
        )
    finally:
        os.remove(f.name)
