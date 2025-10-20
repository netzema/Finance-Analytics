import sys
import time
import webbrowser
import subprocess
from pathlib import Path
from urllib.request import urlopen
import argparse
import os

ROOT = Path(__file__).resolve().parent

# Defaults
LABEL_SCRIPT = ROOT / "label_app.py"
PROCESS_SCRIPT = ROOT / "process_savings.py"
DASHBOARD_SCRIPT = ROOT / "fin_dashboard.py"
DOWNLOAD_SH = ROOT / "download_daily.sh"

LABEL_PORT = 8050
DASH_PORT = 8051

def is_up(url: str) -> bool:
    try:
        with urlopen(url, timeout=1) as r:
            return 200 <= r.status < 400
    except Exception:
        return False

def wait_until_up(url: str, timeout_s: int = 60):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        if is_up(url):
            return True
        time.sleep(0.4)
    return False

def wait_until_down(url: str, poll_s: float = 0.6):
    # keeps polling until the server is unreachable
    while True:
        if not is_up(url):
            return
        time.sleep(poll_s)

def run_server(script: Path, url: str, extra_args: list[str] | None = None):
    """
    Start a Python script that runs a Dash server.
    - If already up at `url`, don't start a new process; just open the browser and
      wait until the server goes down.
    - Otherwise start it, wait until it's up, open the browser, then wait until down.
    Works even with debug reloaders because we don't rely on child PIDs.
    """
    extra_args = extra_args or []
    already_running = is_up(url)

    proc = None
    if not already_running:
        print(f"➡️  Starting: {script}")
        env = os.environ.copy()
        proc = subprocess.Popen([sys.executable, str(script), *extra_args])

        if not wait_until_up(url, timeout_s=90):
            print(f"❌ Server did not come up at {url}. Stopping.")
            if proc and proc.poll() is None:
                proc.terminate()
            sys.exit(1)

    # open browser once it's reachable
    print(f"🌐 Opening {url}")
    webbrowser.open_new_tab(url)

    # wait until user closes the app
    print("⏳ Waiting for the app to be closed...")
    wait_until_down(url)

    # ensure child is gone
    if proc and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
    print(f"✅ {script.name} closed.")

def run_once(script: Path, args: list[str] | None = None):
    args = args or []
    print(f"➡️  Running: {script} {' '.join(args)}")
    res = subprocess.run([sys.executable, str(script), *args])
    if res.returncode != 0:
        print(f"❌ {script.name} exited with code {res.returncode}.")
        sys.exit(res.returncode)
    print(f"✅ {script.name} finished.")

def main():
    ap = argparse.ArgumentParser(description="Label → Process → Dashboard flow")
    ap.add_argument("--label", default=str(LABEL_SCRIPT), help="Path to label_app.py")
    ap.add_argument("--process", default=str(PROCESS_SCRIPT), help="Path to process_savings.py")
    ap.add_argument("--dash", default=str(DASHBOARD_SCRIPT), help="Path to fin_dashboard.py")
    ap.add_argument("--label-port", type=int, default=LABEL_PORT)
    ap.add_argument("--dash-port", type=int, default=DASH_PORT)
    args, passthrough = ap.parse_known_args()

    label_url = f"http://127.0.0.1:{args.label_port}/"
    dash_url = f"http://127.0.0.1:{args.dash_port}/"

    # 0) Download + auto-label
    subprocess.run(["bash", str(DOWNLOAD_SH)], check=True)
    
    # 1) Label app
    run_server(Path(args.label), label_url)

    # 2) Processing step
    run_once(Path(args.process))

    # 3) Dashboard
    run_server(Path(args.dash), dash_url)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Interrupted.")
        sys.exit(130)
