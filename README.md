# Finance Analytics Dashboard

This is a fully automated personal finance dashboard for tracking, analyzing, and understanding your spending behavior over time.

## What It Does

- **Daily transaction download** from your bank account using GoCardless' data export feature
- **Automatic classification** of new transactions based on user-defined rules and dynamic heuristics
- **Manual labeling tool** (`label_app.py`) for unknown or new transactions, with rule updates stored to JSON
- **Interactive dashboard** (`fin_dashboard.py`) built with Python Dash to visualize:
  - Spending trends over time
  - Category-wise breakdowns
  - Account balances, summaries, and statistics

## Technologies Used

- **Python** for data handling, rules engine, and dashboard logic
- **Dash** for the interactive, browser-based UI
- **SQLite** for persistent storage
- **Bash + cron** for daily scheduled automation
- **JSON** rules and config management
- **GoCardless** (or any CSV-based bank export) as data source

## Automation Flow

1. `download_daily.sh` is triggered daily by a cronjob to fetch new data.
2. `download_transactions.py` updates the local database with newly downloaded bank transactions.
3. `classification_utils.py` classifies known transactions automatically using existing rules.
4. `label_app.py` offers a manual interface to classify unknowns and enrich the rule base.
5. `fin_dashboard.py` displays up-to-date, personalized financial insights.

> ⚠️ All sensitive files are .gitignored and never uploaded.

