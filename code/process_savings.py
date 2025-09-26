from pathlib import Path
import polars as pl

import constants

# Config
DEFAULT_PATH = Path(constants.SAVINGS_CSV_PATH)
SAV_PATH = Path(constants.SAVINGS_CSV_PROC_PATH)

# German -> English column map
COLMAP = {
    "buchungstag": "bookingDate",
    "name zahlungsbeteiligter": "partner",
    "iban zahlungsbeteiligter": "partnerIBAN",
    "buchungstext": "remittance",
    "verwendungszweck": "purpose",
    "betrag": "amount",
}

# Final expected columns
FINAL_COLS = ["bookingDate", "partner", "partnerIBAN", "remittance", "purpose", "amount"]

def _ci_rename(df: pl.DataFrame, colmap_ci: dict) -> pl.DataFrame:
    """
    Case-insensitive rename:
    - If a column matches a CI key, rename to its English target.
    - Leaves already-correct English names untouched.
    """
    rename_dict = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in colmap_ci:
            rename_dict[c] = colmap_ci[lc]
    if rename_dict:
        df = df.rename(rename_dict)
    drop_cols = [col for col in df.columns if col not in list(colmap_ci.values())]
    return df.drop(drop_cols)


def _normalize_amount(expr: pl.Expr) -> pl.Expr:
    """
    Handle EU formats like "1.234,56" and plain "1234.56".
    Convert to Float64. Treat empty as null.
    """
    return (
        expr.cast(pl.String)
        .str.replace_all(r"\s", "") # remove spaces
        .str.replace_all(r"\.", "") # remove thousand separators
        .str.replace(",", ".") # decimal comma -> dot
        .str.replace_all(r"^\+€", "") # corner-case artifacts
        .str.replace_all(r"^-€", "-0")
        .map_elements(lambda x: "0" if x in (None, "", ".") else x, return_dtype=pl.String)
        .cast(pl.Float64)
    )


def _normalize_iban(expr: pl.Expr) -> pl.Expr:
    """Uppercase and strip spaces for comparison."""
    return (
        expr.cast(pl.String)
        .str.replace_all(r"\s", "")
        .str.to_uppercase()
    )


def _normalize_partner(expr: pl.Expr) -> pl.Expr:
    """Trim whitespace; keep original case for readability."""
    return expr.cast(pl.String).str.strip_chars()


def _parse_booking_date(expr: pl.Expr) -> pl.Expr:
    """
    Try common export formats: DD.MM.YYYY, YYYY-MM-DD, DD/MM/YYYY.
    Output ISO date string 'YYYY-MM-DD' for CSV stability.
    """
    out = expr.cast(pl.String).str.to_date(format="%d.%m.%Y")
    return out


def process_csv(path: Path, sav_path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    # Read as strings
    df = pl.read_csv(path, infer_schema_length=0, ignore_errors=True, separator=";")

    # rename columns
    df = _ci_rename(df, COLMAP)

    # make sure all required columns exist
    for col in FINAL_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # normalize fields
    df = df.with_columns([
        _parse_booking_date(pl.col("bookingDate")).alias("bookingDate"),
        _normalize_partner(pl.col("partner")).alias("partner"),
        _normalize_iban(pl.col("partnerIBAN")).alias("partnerIBAN"),
        pl.col("remittance").cast(pl.Utf8),
        pl.col("purpose").cast(pl.Utf8),
        _normalize_amount(pl.col("amount")).alias("amount"),
    ])

    # drop rows with no date or no amount
    df = df.filter(
        pl.col("bookingDate").is_not_null() & pl.col("amount").is_not_null()
    )

    # filter out ignored IBANs / partners
    ign_ibans = set(i.replace(" ", "").upper() for i in getattr(constants, "IGNORE_IBANS", []))
    ign_partners = set(getattr(constants, "IGNORE_PARTNERS", []))

    if ign_ibans:
        df = df.filter(~pl.col("partnerIBAN").is_in(list(ign_ibans)))
    if ign_partners:
        df = df.filter(~pl.col("partner").is_in(list(ign_partners)))

    # column order; sort by date
    df = df.select(FINAL_COLS).sort("bookingDate")

    # save
    df.write_csv(sav_path)


def main():
    process_csv(DEFAULT_PATH, SAV_PATH)
    print(f"Processed and saved: {SAV_PATH}")


if __name__ == "__main__":
    main()
