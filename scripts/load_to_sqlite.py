from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DB_DIR = ROOT_DIR / "db"
DB_PATH = DB_DIR / "ecom.db"


DDL_SCRIPTS = {
    "customers": """
        DROP TABLE IF EXISTS customers;
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            city TEXT,
            created_at TEXT
        );
    """,
    "products": """
        DROP TABLE IF EXISTS products;
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT,
            price REAL NOT NULL
        );
    """,
    "orders": """
        DROP TABLE IF EXISTS orders;
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL DEFAULT 0,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
    """,
    "order_items": """
        DROP TABLE IF EXISTS order_items;
        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            item_price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
    """,
    "payments": """
        DROP TABLE IF EXISTS payments;
        CREATE TABLE payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            payment_method TEXT NOT NULL,
            payment_status TEXT NOT NULL,
            payment_date TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );
    """,
}

CSV_MAPPING = {
    "customers": "customers.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv",
}


def create_tables(conn: sqlite3.Connection) -> None:
    with conn:
        for script in DDL_SCRIPTS.values():
            conn.executescript(script)


def load_csv(conn: sqlite3.Connection, table: str, filename: str) -> None:
    csv_path = DATA_DIR / filename
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV file: {csv_path}")

    df = pd.read_csv(csv_path)
    df.to_sql(table, conn, if_exists="append", index=False)


def main() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        create_tables(conn)
        for table, filename in CSV_MAPPING.items():
            load_csv(conn, table, filename)

    print("Data loaded successfully")


if __name__ == "__main__":
    main()

