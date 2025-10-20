# daily.py
from __future__ import annotations
import os, sys, subprocess, time
from datetime import datetime
from pathlib import Path
import config
import platform

if platform.system() == "Windows":
    PROJECT_DIR = Path(config.WIN_PATH)
else:
    # Linux/Unix default
    PROJECT_DIR = Path(config.LINUX_PATH)

# Fallback: if the chosen path doesn’t exist, use script’s parent
if not PROJECT_DIR.exists():
    PROJECT_DIR = Path(__file__).resolve().parent

VENV_DIR = PROJECT_DIR / ".venv"
SCRIPT = PROJECT_DIR / "code" / "download_transactions.py"
LABEL_SCRIPT = PROJECT_DIR / "code" / "label_transactions.py"
LOGFILE = PROJECT_DIR / "data" / "cron_download.log"
STAMP_FILE = PROJECT_DIR / "data" / ".last_download"
MAX_SIZE = 1_048_576  # 1MB

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _rotate_log(path: Path, max_bytes: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size >= max_bytes:
        path.write_text(f"{_now()} — 💾 Truncating log file (exceeded 1MB)\n")

def _log(msg: str):
    LOGFILE.parent.mkdir(parents=True, exist_ok=True)
    with LOGFILE.open("a", encoding="utf-8") as f:
        f.write(f"{_now()} — {msg}\n")

def _today_already_ran(stamp: Path) -> bool:
    if not stamp.exists(): 
        return False
    # Compare local dates
    ran = datetime.fromtimestamp(stamp.stat().st_mtime).date()
    return ran == datetime.now().date()

def _detect_python_interpreter() -> str:
    """
    Prefer the project's venv python if it exists; otherwise use current interpreter.
    Windows: .venv/Scripts/python.exe
    POSIX:   .venv/bin/python
    """
    candidates = [
        VENV_DIR / "Scripts" / "python.exe",
        VENV_DIR / "bin" / "python",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return sys.executable

def _run_and_log(cmd: list[str], cwd: Path | None = None) -> int:
    LOGFILE.parent.mkdir(parents=True, exist_ok=True)
    with LOGFILE.open("a", encoding="utf-8") as f:
        f.write(f"{_now()} — ▶ {' '.join(cmd)}\n")
        proc = subprocess.Popen(
            cmd, cwd=str(cwd) if cwd else None,
            stdout=f, stderr=subprocess.STDOUT
        )
        return proc.wait()

def run_daily() -> None:
    _rotate_log(LOGFILE, MAX_SIZE)
    py = _detect_python_interpreter()

    # Conditional download
    if _today_already_ran(STAMP_FILE):
        _log("Skipping download, already run today.")
    else:
        _log("Running download_transactions.py")
        rc = _run_and_log([py, str(SCRIPT)], cwd=SCRIPT.parent)
        if rc == 0:
            STAMP_FILE.parent.mkdir(parents=True, exist_ok=True)
            STAMP_FILE.touch()
        else:
            _log("⚠️ Download script failed.")
            # Do not exit: labeling should still run

    # Always label
    _log("Running label_transactions.py")
    _run_and_log([py, str(LABEL_SCRIPT)], cwd=LABEL_SCRIPT.parent)

    _log("Done.")
    with LOGFILE.open("a", encoding="utf-8") as f:
        f.write("----------------------------------------\n")
