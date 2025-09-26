from pathlib import Path
import polars as pl
import pandas as pd
import dash
from dash import html, dcc, Input, Output, State, ctx
import dash_bootstrap_components as dbc
from dash import dash_table
import re
import os
import tempfile

# Paths 
SAVINGS_CSV_PATH = (Path(__file__).resolve().parent / "../data/savings_account.csv").resolve()

# Final canonical column order
FINAL_COLS = ["bookingDate", "partner", "partnerIBAN", "remittance", "purpose", "amount"]


# Helpers
def _normalize_amount_str(val: str) -> float:
    """
    Accepts EU '1.234,56', plain '1234.56', or '-500'.
    Returns Python float (positive for inflow, negative for outflow will be applied later).
    """
    if val is None:
        raise ValueError("Amount is required.")
    s = str(val).strip()
    if not s:
        raise ValueError("Amount is required.")
    s = re.sub(r"\s", "", s)
    s = s.replace(".", "").replace(",", ".")
    if s in {".", ""}:
        raise ValueError("Invalid amount.")
    return float(s)


def _normalize_iban(iban: str) -> str:
    """
    Uppercase, strip spaces. Does not hard-fail on format to keep UX smooth.
    """
    if iban is None:
        return ""
    return re.sub(r"\s", "", str(iban)).upper()


def _normalize_text(x) -> str:
    return "" if x is None else str(x).strip()


def _normalize_date_iso(datestr: str) -> str:
    """
    Dash DatePicker gives ISO by default (YYYY-MM-DD).
    Also support DD.MM.YYYY and DD/MM/YYYY just in case.
    """
    if not datestr:
        raise ValueError("Date is required.")
    try:
        # Try fast paths first
        if re.match(r"^\d{4}-\d{2}-\d{2}$", datestr):
            return datestr
        for fmt in ("%d.%m.%Y", "%d/%m/%Y"):
            try:
                return pd.to_datetime(datestr, format=fmt).date().isoformat()
            except Exception:
                pass
        # Fallback: general parser
        return pd.to_datetime(datestr).date().isoformat()
    except Exception as e:
        raise ValueError(f"Invalid date: {datestr}") from e


def _read_savings_csv(path: Path) -> pl.DataFrame:
    """
    Read CSV if exists; otherwise return empty DF with schema.
    Keep columns as strings except amount (Float64).
    """
    if path.exists() and path.stat().st_size > 0:
        df = pl.read_csv(path, infer_schema_length=0)
        # Ensure all required columns exist
        for c in FINAL_COLS:
            if c not in df.columns:
                df = df.with_columns(pl.lit(None).alias(c))
        # Cast types
        df = df.with_columns([
            pl.col("bookingDate").cast(pl.Utf8),
            pl.col("partner").cast(pl.Utf8),
            pl.col("partnerIBAN").cast(pl.Utf8),
            pl.col("remittance").cast(pl.Utf8),
            pl.col("purpose").cast(pl.Utf8),
            pl.col("amount").cast(pl.Float64),
        ]).select(FINAL_COLS)
        return df
    else:
        return pl.DataFrame(schema={
            "bookingDate": pl.Utf8,
            "partner": pl.Utf8,
            "partnerIBAN": pl.Utf8,
            "remittance": pl.Utf8,
            "purpose": pl.Utf8,
            "amount": pl.Float64,
        })


def _atomic_write_csv(df: pl.DataFrame, path: Path) -> None:
    """
    Write to a temp file then replace atomically to avoid partial writes.
    """
    tmpdir = Path(tempfile.mkdtemp())
    tmpfile = tmpdir / (path.name + ".tmp")
    df.write_csv(tmpfile)
    os.replace(tmpfile, path)


def _append_row(path: Path, row: dict) -> pl.DataFrame:
    """
    Append one row, enforce order, sort by date asc, and de-duplicate on a stable key.
    Returns the updated DataFrame.
    """
    df_old = _read_savings_csv(path)
    df_new = pl.DataFrame([row]).select(FINAL_COLS)
    # Combine
    df = pl.concat([df_old, df_new], how="vertical_relaxed")

    # Deduplicate (keep first)
    df = df.with_columns(
        (pl.col("bookingDate") + "|" +
         pl.col("partner") + "|" +
         pl.col("remittance") + "|" +
         pl.col("amount").cast(pl.Utf8)).alias("_key")
    ).unique(subset=["_key"], keep="first").drop("_key")

    # Sort
    df = df.with_columns(
        pl.col("bookingDate").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    ).sort("bookingDate").with_columns(
        pl.col("bookingDate").cast(pl.Utf8)
    )

    _atomic_write_csv(df.select(FINAL_COLS), path)
    return df


def _options_from_series(values: list[str]) -> list[dict]:
    uniq = sorted({v for v in values if v})
    return [{"label": v, "value": v} for v in uniq]


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SLATE])
app.title = "Savings: Add Transaction"

def build_layout() -> html.Div:
    df = _read_savings_csv(SAVINGS_CSV_PATH)
    partner_opts = _options_from_series(df.get_column("partner").to_list()) if df.height > 0 else []
    iban_opts = _options_from_series(df.get_column("partnerIBAN").to_list()) if df.height > 0 else []

    last10 = df.sort("bookingDate", descending=True).head(10).to_pandas() if df.height else pd.DataFrame(columns=FINAL_COLS)

    return html.Div([
        dbc.Container([
            html.H3("🏦 Add Savings Transaction", className="my-3 text-center"),

            # Entry card
            dbc.Card([
                dbc.CardHeader("New Entry"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Date"),
                            dcc.DatePickerSingle(id="sav-date")
                        ], md=3),
                        dbc.Col([
                            html.Label("Partner"),
                            dbc.Input(id="sav-partner", placeholder="e.g. Bank XYZ"),
                            dcc.Dropdown(id="sav-partner-suggest", options=partner_opts, placeholder="Pick from history (optional)", clearable=True, className="mt-1"),
                        ], md=3),
                        dbc.Col([
                            html.Label("Partner IBAN"),
                            dbc.Input(id="sav-iban", placeholder="e.g. AT12..."),
                            dcc.Dropdown(id="sav-iban-suggest", options=iban_opts, placeholder="Pick from history (optional)", clearable=True, className="mt-1"),
                        ], md=3),
                        dbc.Col([
                            html.Label("Amount"),
                            dbc.Input(id="sav-amount", type="text", placeholder="e.g. 1.234,56"),
                            dcc.RadioItems(
                                id="sav-direction",
                                options=[{"label":"Inflow (+)", "value":"IN"}, {"label":"Outflow (-)", "value":"OUT"}],
                                value="IN",
                                inline=True,
                                className="mt-2"
                            ),
                        ], md=3),
                    ], className="mb-3"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Remittance"),
                            dbc.Input(id="sav-remittance", placeholder="e.g. Interest 2025"),
                        ], md=6),
                        dbc.Col([
                            html.Label("Purpose"),
                            dbc.Input(id="sav-purpose", placeholder="Optional note / purpose"),
                        ], md=6),
                    ]),
                    dbc.Row([
                        dbc.Col(dbc.Button("Add Transaction", id="sav-add", color="primary", className="mt-3")),
                        dbc.Col(dbc.Button("Clear", id="sav-clear", color="secondary", className="mt-3"), className="col-auto"),
                        dbc.Col(html.Div(id="sav-msg", className="mt-3"), className="col"),
                    ])
                ])
            ], className="mb-4"),

            # Recent table
            html.H5("Last 10 entries"),
            dash_table.DataTable(
                id="sav-table",
                columns=[{"name": c, "id": c} for c in FINAL_COLS],
                data=last10.to_dict("records"),
                sort_action="native",
                style_table={"overflowX":"auto"},
                style_header={"backgroundColor":"#2c3e50", "color":"white"},
                style_cell={"backgroundColor":"#1e1e1e", "color":"white", "textAlign":"left"},
            ),
        ], fluid=True)
    ], style={"backgroundColor":"#121212", "minHeight":"100vh", "paddingTop":"12px"})


app.layout = build_layout # callable --> rebuilds options on hot-reload


# Callbacks 
@app.callback(
    Output("sav-partner", "value", allow_duplicate=True),
    Input("sav-partner-suggest", "value"),
    prevent_initial_call=True
)
def fill_partner_from_suggest(val):
    return val or dash.no_update


@app.callback(
    Output("sav-iban", "value", allow_duplicate=True),
    Input("sav-iban-suggest", "value"),
    prevent_initial_call=True
)
def fill_iban_from_suggest(val):
    return val or dash.no_update


@app.callback(
    Output("sav-msg", "children"),
    Output("sav-table", "data"),
    Output("sav-date", "date"),
    Output("sav-partner", "value", allow_duplicate=True),
    Output("sav-iban", "value", allow_duplicate=True),
    Output("sav-remittance", "value"),
    Output("sav-purpose", "value"),
    Output("sav-amount", "value"),
    Output("sav-direction", "value"),
    Input("sav-add", "n_clicks"),
    Input("sav-clear", "n_clicks"),
    State("sav-date", "date"),
    State("sav-partner", "value"),
    State("sav-iban", "value"),
    State("sav-remittance", "value"),
    State("sav-purpose", "value"),
    State("sav-amount", "value"),
    State("sav-direction", "value"),
    prevent_initial_call=True
)
def add_or_clear(n_add, n_clear, date_val, partner, iban, remit, purpose, amount_str, direction):
    trig = ctx.triggered_id
    if trig == "sav-clear":
        return "", dash.no_update, None, None, None, None, None, None, "IN"

    # Validate + normalize
    try:
        iso_date = _normalize_date_iso(date_val)
        partner = _normalize_text(partner)
        iban = _normalize_iban(iban)
        remit = _normalize_text(remit)
        purpose = _normalize_text(purpose)
        amt = _normalize_amount_str(amount_str)
        if direction == "OUT":
            amt = -abs(amt)
        else:
            amt = abs(amt)
    except Exception as e:
        return f"⚠️ {e}", dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Append row
    row = {
        "bookingDate": iso_date,
        "partner": partner,
        "partnerIBAN": iban,
        "remittance": remit,
        "purpose": purpose,
        "amount": amt,
    }
    df_updated = _append_row(SAVINGS_CSV_PATH, row)

    # Update recent preview
    last10 = (
        df_updated.sort("bookingDate", descending=True)
        .head(10)
        .to_pandas()
    )

    # Clear form
    return (
        f"✅ Added entry for {iso_date} ({amt:.2f}).",
        last10.to_dict("records"),
        None, None, None, None, None, None, "IN"
    )


# Main 
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8062)