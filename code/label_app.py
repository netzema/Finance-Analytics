import dash
from dash import html, dcc, Input, Output, State, ctx
import polars as pl
import sqlite3
import json
from pathlib import Path
import subprocess
from flask import request
import os

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "transactions.db"
RULES_PATH = BASE_DIR / "code" / "rules.json"
LABEL_SCRIPT = BASE_DIR / "code" / "label_transactions.py"

# Dash app
app = dash.Dash(__name__)
app.title = "Transaction Labeler"

# Helper functions
def _shutdown_server():
    func = request.environ.get("werkzeug.server.shutdown")
    if func:
        func()
    else:
        # Fallback if not running with the Werkzeug dev server
        os._exit(0)
        
def load_next_unlabeled_transaction():
    conn = sqlite3.connect(DB_PATH)
    df = pl.read_database("SELECT * FROM transactions WHERE category IS NULL LIMIT 1", conn)
    conn.close()
    return df

def load_categories():
    conn = sqlite3.connect(DB_PATH)
    cats = conn.execute("SELECT DISTINCT category FROM transactions WHERE category IS NOT NULL").fetchall()
    conn.close()
    return sorted(list({c[0] for c in cats if c[0]}))

def write_label_to_db(tx_id: str, category: str):
    print(f"💾 Writing to DB: {tx_id} -> {category}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE transactions SET category = ? WHERE transactionId = ?",
        (category, tx_id)
    )
    print("🔍 Rows affected:", cursor.rowcount)
    conn.commit()
    conn.close()

def add_rule_to_json(keyword: str, category: str, field: str):
    if not keyword.strip():
        return
    keyword = keyword.strip()

    # Simple heuristic: if it contains regex meta characters, mark as regex
    is_regex = bool(any(c in keyword for c in "*+?[]()^$|"))

    if RULES_PATH.exists():
        with open(RULES_PATH, "r") as f:
            rules = json.load(f)
    else:
        rules = []

    existing = {r['match'].lower() for r in rules}
    if keyword.lower() not in existing:
        rules.append({
            "match": keyword,
            "field": field,
            "category": category,
            "regex": is_regex
        })
        with open(RULES_PATH, "w") as f:
            json.dump(rules, f, indent=2)#
    else:
        print("Rule for this pattern already exists.")

def run_label_script():
    subprocess.run(["python", str(LABEL_SCRIPT)], check=True)

# App Layout
app.layout = html.Div([
    html.H2("🧾 Transaction Labeler"),

    html.Div(id="transaction-display"),

    html.Label("Choose category:", className="label"),
    dcc.Dropdown(id="category-dropdown", options=[], value=None, placeholder="Select existing category"),

    html.Label("...or create new category:", className="label"),
    dcc.Input(id="new-category", type="text", placeholder="e.g. Food"),

    html.Label("Optional rule pattern:", className="label"),
    dcc.Input(id="new-pattern", type="text", placeholder="Enter remittance pattern"),

    html.Button("Unique", id="unique-btn", style={"marginLeft": "10px"}),

    html.Button("Assign Label", id="assign-btn"),
    html.Div(id="confirm-msg"),

    html.Button("Finish & Continue", id="finish-btn", style={"marginLeft": "10px"}),

    dcc.Interval(id="init-trigger", interval=1, max_intervals=1)
], className="container")

# Callbacks
@app.callback(
    # Output("confirm-msg", "children"),
    Input("finish-btn", "n_clicks"),
    prevent_initial_call=True
)
def finish_flow(_):
    _shutdown_server()
    # return "Shutting down…"

@app.callback(
    Output("transaction-display", "children"),
    Output("category-dropdown", "options"),
    Output("category-dropdown", "value"),
    Output("new-category", "value"),
    Output("new-pattern", "value"),
    Output("confirm-msg", "children"),
    Input("assign-btn", "n_clicks"),
    Input("init-trigger", "n_intervals"), 
    Input("unique-btn", "n_clicks"),
    State("category-dropdown", "value"),
    State("new-category", "value"),
    State("new-pattern", "value"),
    prevent_initial_call=True
)
def unified_labeling_callback(n_clicks, _intervals, unique_n_clicks, dropdown_val, new_cat_val, pattern_val):
    print("🔁 Callback triggered by:", ctx.triggered_id)
    confirm = ""

    if ctx.triggered_id == "assign-btn":
        print("🔨 Assigning label...")
        df = load_next_unlabeled_transaction()
        if df.is_empty():
            print("✅ Nothing to label.")
            return "✅ All transactions labeled!", [], None, None, None, ""

        tx = df.row(0, named=True)
        tx_id = tx["transactionId"]
        label = new_cat_val if new_cat_val else dropdown_val

        if label:
            print(f"📝 Writing label '{label}' for transaction {tx_id}")
            write_label_to_db(tx_id, label)
            add_rule_to_json(pattern_val or "", label, "remittance")
            run_label_script()
            confirm = f"✅ Labeled {tx_id} as '{label}'"
        else:
            confirm = "⚠️ No category selected."

    if ctx.triggered_id == "unique-btn":
        print("🔨 Assigning unique label...")
        df = load_next_unlabeled_transaction()
        if df.is_empty():
            return "✅ All transactions labeled!", [], None, None, None, ""

        tx = df.row(0, named=True)
        tx_id = tx["transactionId"]
        label = new_cat_val if new_cat_val else dropdown_val

        if label:
            write_label_to_db(tx_id, label)
            add_rule_to_json(tx_id, label, "transactionId")
            run_label_script()
            confirm = f"✅ Labeled {tx_id} as '{label}' (unique rule)"
        else:
            confirm = "⚠️ No category selected."


    # Always fetch the next transaction
    df = load_next_unlabeled_transaction()
    if df.is_empty():
        print("✅ No more unlabeled transactions.")
        tx_display = "✅ All transactions labeled!"
    else:
        tx = df.row(0, named=True)
        print("🧾 Showing transaction:", tx["transactionId"])
        tx_display = html.Div([
            html.B("Transaction ID: "), tx['transactionId'], html.Br(),
            html.B("Remittance: "), tx['remittance'], html.Br(),
            html.B("Amount: "), f"{tx['amount']} {tx['currency']}", html.Br(),
            html.B("Date: "), tx['bookingDate']
        ])

    return (
        tx_display,
        [{'label': c, 'value': c} for c in load_categories()],
        None,
        None,
        None,
        confirm
    )

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)