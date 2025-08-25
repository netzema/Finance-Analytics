import sqlite3
import polars as pl
from pathlib import Path
import json
import requests
from classification_utils import classify_transaction

def get_access_token(secret_id: str, secret_key: str) -> str:
    url = "https://bankaccountdata.gocardless.com/api/v2/token/new/"
    response = requests.post(url, json={
        "secret_id": secret_id,
        "secret_key": secret_key
    })
    response.raise_for_status()
    return response.json()["access"]


def fetch_transactions(account_id: str, token: str) -> list:
    url = f"https://bankaccountdata.gocardless.com/api/v2/accounts/{account_id}/transactions/"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("transactions", {}).get("booked", [])


def apply_label_rules(df: pl.DataFrame, rules_path: Path) -> pl.DataFrame:
    """
    Apply labeling rules from a JSON file to a Polars DataFrame.

    Supported rule types:
    - String containment (literal or regex) on any string field
    - Numeric comparisons (>, >=, <, <=, ==, !=) on numeric fields like "amount"

    Parameters:
    df (pl.DataFrame): Transaction data
    rules_path (Path): Path to rules.json

    Returns:
    pl.DataFrame: Updated DataFrame with labeled 'category' column
    """
    import re

    with open(rules_path, "r") as f:
        rules = json.load(f)

    df = df.with_columns(pl.lit(None).alias("category"))

    for rule in rules:
        field = rule["field"]
        pattern = rule["match"]
        category = rule["category"]
        use_regex = rule.get("regex", False)

        # Handle numeric comparisons (for float fields like "amount")
        if field == "amount" and re.match(r"^(>=|<=|==|!=|>|<)\s*\d+(\.\d+)?$", pattern.strip()):
            op_match = re.match(r"^(>=|<=|==|!=|>|<)", pattern.strip())
            op = op_match.group(1)
            value = float(pattern.strip()[len(op):].strip())
            col = pl.col(field)

            condition = {
                ">": col > value,
                "<": col < value,
                ">=": col >= value,
                "<=": col <= value,
                "==": col == value,
                "!=": col != value
            }[op]

        # String matching (regex or literal)
        else:
            col = pl.col(field).cast(pl.Utf8).str.to_lowercase()
            pattern = pattern.lower()

            if use_regex:
                condition = col.str.contains(pattern, literal=False)
            else:
                condition = col.str.contains(pattern, literal=True)

        # Apply condition to update category
        df = df.with_columns(
            pl.when(condition)
            .then(pl.lit(category))
            .otherwise(pl.col("category"))
            .alias("category")
        )

    return df


def load_transactions_db(db_path: Path) -> pl.DataFrame:
    """
    Load the transactions database and convert date columns to pl.Date.

    Parameters:
    db_path (Path): Path to the SQLite database.

    Returns:
    pl.DataFrame: Transactions with parsed date columns.
    """
    conn = sqlite3.connect(db_path)
    df = pl.read_database("SELECT * FROM transactions", conn)
    conn.close()

    # Convert date columns to proper Date type
    df = df.with_columns(
        pl.col(["bookingDate", "valueDate"]).str.strptime(pl.Date, "%Y-%m-%d")
    )

    return df

# Other accounts (not main account)
def preprocess_transactions(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean and format the raw transaction dataframe.
    Converts dates and amounts, selects necessary columns.
    """
    df_cleaned = (
        df
        .select([
            pl.col("bookingDate"), #.str.strptime(pl.Date, "%d.%m.%Y"),
            pl.col("partner").str.strip_chars(),
            pl.col("amount").str.replace_all("\\.", "").str.replace(",", ".").cast(pl.Float64),
            pl.col("remittance").fill_null("")
        ])
        .sort("bookingDate")
    )
    return df_cleaned
    
def add_category_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add a 'category' column to the dataframe using classification rules.
    """
    return df.with_columns([
        pl.struct(["partner", "remittance"]).map_elements(classify_transaction, return_dtype=pl.String).alias("category")
    ])
    

def load_data(account: str) -> pl.DataFrame:
    '''
    Loads the respective transactions data into a dataframe.

    Params:
        account (str): Any of "Silvia", "Hansi"
    '''
    df = pl.read_csv(f"../data/{account}_konto.csv", try_parse_dates=True)

    df = df.rename({
        "Buchungsdatum": "bookingDate",
        "Partnername": "partner",
        "Betrag": "amount", 
        "Buchungs-Details": "remittance"
    })
    df = preprocess_transactions(df)
    df = add_category_column(df)
    return df.select([
        "bookingDate",
        "category",
        # "partner",
        "amount", 
        "remittance",
    ])

def load_account_data(accounts):
    dfs = []
    if "Daniel" in accounts:
        BASE_DIR = Path(__file__).resolve().parent.parent
        DB_PATH = BASE_DIR / "data" / "transactions.db"
        df_conn = sqlite3.connect(DB_PATH)
        df_daniel = pl.read_database("SELECT * FROM transactions", df_conn)
        df_conn.close()
        df_daniel = df_daniel.with_columns(
            pl.col("bookingDate").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d"),
        ).select([
            "bookingDate",
            "category",
            "amount", 
            "remittance"
        ])
        dfs.append(df_daniel)
    for acc in [a for a in accounts if a != "Daniel"]:
        dfs.append(load_data(acc.lower()))
    if dfs:
        return pl.concat(dfs, how="vertical")
    else:
        return pl.DataFrame([])