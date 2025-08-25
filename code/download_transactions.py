from pathlib import Path
import os
import sqlite3
import requests
from dotenv import load_dotenv
from utils import get_access_token, fetch_transactions

# Load secrets from .env
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)

SECRET_ID = os.getenv("GC_SECRET_ID")
SECRET_KEY = os.getenv("GC_SECRET_KEY")
ACCOUNT_ID = os.getenv("GC_ACCOUNT_ID")

# Setup paths
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "transactions.db"

# Get access token from API
access_token = get_access_token(SECRET_ID, SECRET_KEY)

# Fetch transactions
transactions = fetch_transactions(ACCOUNT_ID, access_token)

# Save to SQLite (if new)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    transactionId TEXT PRIMARY KEY,
    bookingDate TEXT,
    valueDate TEXT,
    amount REAL,
    currency TEXT,
    remittance TEXT,
    internalId TEXT
)
""")

new_entries = 0
for tx in transactions:
    tx_id = tx.get("transactionId")
    booking = tx.get("bookingDate")
    value = tx.get("valueDate")
    amount = float(tx.get("transactionAmount", {}).get("amount", 0))
    currency = tx.get("transactionAmount", {}).get("currency", "EUR")
    remittance = tx.get("remittanceInformationStructured", "")
    internal = tx.get("internalTransactionId", "")

    try:
        cursor.execute("""
        INSERT INTO transactions (transactionId, bookingDate, valueDate, amount, currency, remittance, internalId)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (tx_id, booking, value, amount, currency, remittance, internal))
        new_entries += 1
    except sqlite3.IntegrityError:
        continue  # Duplicate entry, skip

conn.commit()
conn.close()

print(f"✅ Inserted {new_entries} new transactions.")
