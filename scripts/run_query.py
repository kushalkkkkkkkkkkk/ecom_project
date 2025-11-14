from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "db" / "ecom.db"
SQL_PATH = ROOT_DIR / "sql" / "join_query.sql"


def read_query() -> str:
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_PATH}")
    return SQL_PATH.read_text(encoding="utf-8")


def run_query(query: str) -> pd.DataFrame:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return pd.read_sql_query(query, conn)


def main() -> None:
    query = read_query()
    df = run_query(query)
    if df.empty:
        print("Query returned no rows.")
        return

    print(df.to_string(index=False))


if __name__ == "__main__":
    main()

