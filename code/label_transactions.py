import polars as pl
import json
import sqlite3
from pathlib import Path
from utils import load_transactions_db, apply_label_rules

if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent.parent
    DB_PATH = BASE_DIR / "data" / "transactions.db"
    RULES_PATH = BASE_DIR / "code" / "rules.json"

    # Load transactions and apply labeling
    df = load_transactions_db(DB_PATH)
    df = apply_label_rules(df, RULES_PATH)

    # Write manually row by row into SQLite (overwriting old table)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS transactions")
    cursor.execute("""
    CREATE TABLE transactions (
        transactionId TEXT PRIMARY KEY,
        bookingDate TEXT,
        valueDate TEXT,
        amount REAL,
        currency TEXT,
        remittance TEXT,
        internalId TEXT,
        category TEXT
    )
    """)

    for row in df.iter_rows(named=True):
        cursor.execute("""
            INSERT INTO transactions (
                transactionId, bookingDate, valueDate,
                amount, currency, remittance, internalId, category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["transactionId"],
            str(row["bookingDate"]),
            str(row["valueDate"]),
            row["amount"],
            row["currency"],
            row["remittance"],
            row["internalId"],
            row["category"]
        ))


    conn.commit()
    conn.close()

    print("✅ Transactions labeled and written to 'transactions' table.")
