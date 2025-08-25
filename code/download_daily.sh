#!/bin/bash

# Absolute paths
PROJECT_DIR="/home/fin/fin_dashboard"
VENV_DIR="$PROJECT_DIR/.venv"
SCRIPT="$PROJECT_DIR/code/download_transactions.py"
LABEL_SCRIPT="$PROJECT_DIR/code/label_transactions.py"
LOGFILE="$PROJECT_DIR/data/cron_download.log"
STAMP_FILE="$PROJECT_DIR/data/.last_download"

# Limit log file size to 1MB
MAX_SIZE=1048576
if [ -f "$LOGFILE" ] && [ "$(stat -c%s "$LOGFILE")" -ge "$MAX_SIZE" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') — 💾 Truncating log file (exceeded 1MB)" > "$LOGFILE"
fi

# Activate virtualenv
cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"

# Conditional download
if [ -f "$STAMP_FILE" ] && [ "$(date -r "$STAMP_FILE" +%Y-%m-%d)" = "$(date +%Y-%m-%d)" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') — Skipping download, already run today." >> "$LOGFILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') — Running download_transactions.py" >> "$LOGFILE"
    python "$SCRIPT" >> "$LOGFILE" 2>&1
    if [ $? -eq 0 ]; then
        touch "$STAMP_FILE"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') — ⚠️ Download script failed." >> "$LOGFILE"
    fi
fi

# Always run labeling
echo "$(date '+%Y-%m-%d %H:%M:%S') — Running label_transactions.py" >> "$LOGFILE"
python "$LABEL_SCRIPT" >> "$LOGFILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Done." >> "$LOGFILE"
echo "----------------------------------------" >> "$LOGFILE"
