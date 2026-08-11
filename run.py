from app import create_app
import os
import sys
import subprocess
import threading
import time
import urllib.request

app = create_app()

_OPEN_GMAIL_SCRIPT = os.path.abspath(os.path.join(os.path.dirname(__file__), "scripts", "open_gmail.py"))
_SCRAPED_EMAILS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "scraped_emails.json"))
_GMAIL_CONTROL_SERVER = "http://127.0.0.1:40005"

# Shared with watchdog_loop() below so it can tell "the scraper we launched has
# actually exited" (poll() is not None) apart from "it's just mid-action" (a single
# slow request - e.g. a deep classify - can legitimately block the scrape loop for up
# to ~150s, see open_gmail.py's /get_documents timeout). Only the former should ever
# trigger a relaunch; the latter would spawn a second Chrome onto the same Playwright
# profile directory and fight the first one.
_gmail_process = None
_gmail_process_lock = threading.Lock()


def _gmail_control_server_reachable(timeout=2):
    """Is scripts/open_gmail.py's control server already up (launched by an earlier
    run.py, or standalone via the dashboard's "Open Gmail" button / directly)? Checked
    before ever spawning a new one - Chrome refuses a second instance on the same
    persistent profile directory (data/playwright_profile), so a redundant launch here
    doesn't create a second working browser, it just crashes loudly and does nothing
    useful, while the real (already-running) one keeps serving fine regardless."""
    try:
        with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/health", timeout=timeout):
            return True
    except Exception:
        return False


def _launch_gmail_process():
    global _gmail_process
    python_exe = sys.executable
    print("Automatically launching Playwright browser with Google Account...")
    try:
        proc = subprocess.Popen([python_exe, _OPEN_GMAIL_SCRIPT])
        with _gmail_process_lock:
            _gmail_process = proc
    except Exception as e:
        print(f"Failed to auto-launch Playwright browser: {e}")


def launch_browser():
    # Wait a couple of seconds for Flask to initialize and bind to the port
    time.sleep(2.5)
    if _gmail_control_server_reachable():
        print("Gmail automation browser is already running (control server reachable on :40005) - not launching another.")
        return
    _launch_gmail_process()


# Guards against the exact failure this was added for: the scraper process died (browser
# crashed, was closed, hit an unhandled exception) while run.py itself kept running, so
# nothing ever relaunched it and the dashboard's "a-cmr" list silently went stale until
# someone noticed and restarted everything by hand.
_WATCHDOG_INTERVAL_S = 60
# Matches tracking_api.py's own _SCRAPED_FRESH_SECONDS - if the scrape file is still
# this recent, SOMETHING is actively writing it (our tracked process, or one launched
# independently via the dashboard's "Process" button, which goes through tracking_api.py's
# /api/automation/open-gmail and this module has no handle on) - never relaunch on top
# of that, regardless of what our own subprocess handle shows.
_DATA_FRESH_S = 90
# Only relaunch once the file has been stale far longer than that AND our own tracked
# process has actually exited - both conditions guard against relaunching on top of a
# process that's simply slow to start (cold Gmail login) or busy with one long action (a
# deep classify can legitimately block the scrape loop for ~150s, see open_gmail.py's
# /get_documents timeout).
_WATCHDOG_STALE_THRESHOLD_S = 300


def watchdog_loop():
    while True:
        time.sleep(_WATCHDOG_INTERVAL_S)
        try:
            age = time.time() - os.path.getmtime(_SCRAPED_EMAILS_PATH)
        except OSError:
            age = None
        if age is not None and age < _DATA_FRESH_S:
            continue  # something is actively writing - nothing to do

        with _gmail_process_lock:
            proc = _gmail_process
        if proc is not None and proc.poll() is None:
            continue  # our own process is alive, just mid-action - give it more time

        if age is None or age >= _WATCHDOG_STALE_THRESHOLD_S:
            if _gmail_control_server_reachable():
                # Alive and well, just not OUR tracked handle (e.g. launched
                # independently) - the stale scrape file has some other cause (stuck
                # label switch, etc.), not "the browser is gone." Relaunching here
                # would only crash against the existing Chrome's profile lock.
                continue
            print(f"[Watchdog] Gmail automation appears stopped (data age={age}) - relaunching.")
            _launch_gmail_process()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 40000))

    # No reloader (use_reloader=False below) means no parent/child process split, so
    # these just need to start once, unconditionally.
    threading.Thread(target=launch_browser, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()

    # threaded=True: the Operations Process endpoints can block for tens of seconds
    # (LLM document classification, waiting for the Shypple browser to cold-start) -
    # without this, the single-threaded dev server would also stall unrelated requests
    # like the live email poll for that whole time.
    #
    # use_reloader=False: the auto-reloader restarts this whole process the instant it
    # sees ANY watched .py file change on disk, with no regard for requests in flight.
    # Multi-mail sweeps like classify_all_documents (tracking_api.py) hold one request
    # open for the entire batch (several seconds per mail, real browser fetches) - a
    # reload mid-batch kills that request outright and the browser gets nothing back,
    # even though earlier mails in the batch already finished. debug=True still gives
    # the interactive debugger/error pages; only the file-watch restart is disabled.
    # Restart manually (Ctrl+C, rerun) after editing code.
    app.run(host="0.0.0.0", port=port, debug=app.config["DEBUG"], threaded=True, use_reloader=False)
