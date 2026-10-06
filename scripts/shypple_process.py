"""Operations Process automation: drives a second, separate Playwright browser against
Shypple's admin backend to verify a shipment's containers and already-uploaded document
types against what was extracted from a tracked email.

Architecture mirrors scripts/open_gmail.py: a persistent Chrome profile (so the Shypple
login session survives restarts) plus a local HTTP control server the Flask app talks to.
Unlike open_gmail.py's per-action request queue, a whole batch of jobs is processed
sequentially in one go on the main thread (the only thread allowed to touch Playwright's
sync objects); the two points that need a human are implemented as a shared
threading.Event the main thread blocks on, set by the control server's /proceed endpoint.
"""

import os
import re
import json
import queue
import base64
import argparse
import mimetypes
import threading
import urllib.error
import urllib.parse
import urllib.request
import shutil
from datetime import datetime, timezone
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler, HTTPServer

from playwright.sync_api import sync_playwright

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from shared.download_utils import fetch_bytes_robust, get_downloads_dir

# Two independent instances of this same script can run side by side - one dedicated
# to the CMR pipeline, one to the LCL Arrivals/Release pipeline - each with its own
# Chrome profile/port, so a batch on one never has to wait on a batch running (or
# paused on a confirmation gate) on the other. Selected via --role on the command
# line (see main()); everything else in this file (batch_state, the job queue, the
# control server) is already just module-level globals scoped to one OS process, so
# running two processes gives full independence for free. CMR keeps the original
# port/profile so an existing logged-in instance isn't disrupted.
ROLE_CONFIG = {
    "cmr": {"port": 40006, "profile_dir": "shypple_profile"},
    "lcl": {"port": 40007, "profile_dir": "shypple_profile_lcl"},
}

SHYPPLE_LOGIN_URL = "https://app.shypple.com/login?redirect=%2Fdashboard"
SHYPPLE_ADMIN_BASE = "https://api.shypple.com"
# Any valid admin shipment page works here - its only purpose is to trigger (once per
# browser session) the Google SSO gate that /admin/* sits behind. The real,
# per-email shipment is found afterwards via the search box.
SHYPPLE_BOOTSTRAP_URL = f"{SHYPPLE_ADMIN_BASE}/admin/shipments/171400"

# scripts/open_gmail.py's control server (a separate automation script/browser) - a
# matched shipment with no organization set gets its source email forwarded there and
# moved off the a-cmr label, via that script's /forward_and_relabel action.
GMAIL_CONTROL_SERVER = "http://127.0.0.1:40005"
FORWARD_NO_ORG_TO = "nl.importsea@shypple.com"
FORWARD_NO_ORG_LABEL = "a-release-orders"

# This pipeline must run under "Shypple B.V." (organization_id=1). The admin profile
# may default to "Shypple Fresh B.V." - ensure_org_is_shypple_bv() switches it once
# per session, before any per-job work begins.
TARGET_ORG_NAME = "Shypple B.V."
# A matched shipment can legitimately come back with an empty organization under
# TARGET_ORG_NAME - some shipments only exist under this second legal entity instead.
# process_one_job switches to this org (after operator confirmation), re-searches the
# same container(s) there, and switches back to TARGET_ORG_NAME once that job is done -
# see switch_to_fresh_org / _switch_shypple_org.
FRESH_ORG_NAME = "Shypple Fresh B.V."

# The Flask app (app/routes/api/operations_api.py) - used to fetch the email's own copy
# of a document's bytes, and to run the Gemini same/different comparison, since that
# logic already lives there (document_classifier.py) rather than duplicating it here.
FLASK_BASE = f"http://127.0.0.1:{os.environ.get('PORT', '40000')}"

# Matches tracking_api.py's _LCL_LABEL_KEY / document_classifier.py's
# _LCL_ARRIVALS_LABEL / operations_api.py's _LCL_LABEL_KEY - passed to
# _fetch_email_document (-> Flask's /operations/document_file -> fetch_document_bytes)
# so a force-reclassify retry on an LCL mail can't corrupt its classification under
# the CMR Cmr/Other override rule.
_LCL_LABEL_KEY = "lcl-arrivals---release"

# Per the operator's explicit rule: an Arrival notice never gets containers attached;
# Arrival notice, Delivery order, and T1 document never get an organization/customer
# attached. Every other document type gets both.
CONTAINER_SKIP_TYPES = {"Arrival notice"}
ORG_SKIP_TYPES = {"Arrival notice", "Delivery order", "T1 DOC"}

# Every job status that means "a job is paused, waiting on a human to hit Proceed or
# Skip" - shared by the /proceed and /skip control-server handlers below so a new gate
# (either pipeline) only needs to be added in ONE place. The CMR pipeline's four gates
# plus the LCL Arrivals/Release pipeline's four gates (see process_lcl_arrival_job and
# its handlers).
_AWAITING_STATUSES = (
    "awaiting_upload_confirmation", "awaiting_forward_confirmation",
    "awaiting_submit_confirmation", "awaiting_no_record_confirmation",
    "awaiting_org_switch_confirmation",
    "awaiting_lcl_container_confirmation", "awaiting_lcl_submit_confirmation",
    "awaiting_lcl_delivery_confirmation", "awaiting_lcl_date_confirmation",
    "awaiting_arrival_notice_verification", "awaiting_lcl_document_diff_confirmation",
)

# Where a "My Jewellery" customer match halts the LCL Arrivals/Release flow gets
# recorded (see _record_my_jewellery_flag) - mirrors open_gmail.py's forwarded-mail
# tracker load/save pattern (data/forwarded_mails.json).
_MY_JEWELLERY_TRACKER_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "lcl_my_jewellery_mails.json")
)
_MY_JEWELLERY_TRACKER_LOCK = threading.Lock()

# Real, live-scraped option list of Shypple's own #shipment_document_document_type_ids
# dropdown - our internal canonical DOCUMENT_TYPES list doesn't map 1:1 onto Shypple's
# exact labels (spelling/wording differences, e.g. our "Custome Import Doc" vs
# Shypple's real "Customs import document"), which is what caused select2 match
# failures. This is the ground truth for the Operations Process review UI's
# type-override dropdown, so whatever a user picks there is GUARANTEED to exist as a
# real option, not another guess. Populated two ways: passively, as a side effect of
# every real fill_shipment_document_form call (which already has the select in scope
# for the actual upload); and on demand via the /refresh_document_type_options action,
# which navigates to a previously-visited shipment page purely to read this static
# dropdown (per earlier confirmation, all ~90 options are plain <option> tags in the
# page's initial HTML, not per-shipment/remote-loaded, so ANY shipment page works).
_DOC_TYPE_OPTIONS_CACHE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "shypple_document_type_options.json")
)
_DOC_TYPE_OPTIONS_LOCK = threading.Lock()


def _load_document_type_options():
    try:
        if os.path.exists(_DOC_TYPE_OPTIONS_CACHE_PATH):
            with open(_DOC_TYPE_OPTIONS_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _save_document_type_options(options):
    if not options:
        return
    with _DOC_TYPE_OPTIONS_LOCK:
        os.makedirs(os.path.dirname(_DOC_TYPE_OPTIONS_CACHE_PATH), exist_ok=True)
        tmp = _DOC_TYPE_OPTIONS_CACHE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"options": options, "captured_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        os.replace(tmp, _DOC_TYPE_OPTIONS_CACHE_PATH)


def _scrape_document_type_options(page):
    """Read every real <option> text off Shypple's own document-type select. Must run
    on the main thread, with the select already present on the current page (either
    mid-upload via fill_shipment_document_form, or after navigating to any shipment's
    Documents tab via _refresh_document_type_options)."""
    try:
        return page.evaluate("""() => {
            const sel = document.getElementById('shipment_document_document_type_ids');
            if (!sel) return null;
            const seen = new Set();
            const out = [];
            for (const opt of sel.options) {
                const text = (opt.textContent || '').trim();
                if (!text || seen.has(text)) continue;
                seen.add(text);
                out.push(text);
            }
            return out;
        }""")
    except Exception as e:
        log_system(f"_scrape_document_type_options error: {e}")
        return None


def _refresh_document_type_options(page):
    """Navigate to a previously-matched shipment's Documents tab (if we know one from
    batch history) purely to read the live document-type dropdown, and cache it. Must
    run on the main thread. Returns {"success": bool, "options"/"error"}."""
    last_shipment_path = None
    with STATE_LOCK:
        for j in reversed(batch_state.get("jobs", [])):
            if j.get("shipment_path"):
                last_shipment_path = j["shipment_path"]
                break
    if not last_shipment_path:
        return {
            "success": False,
            "error": "No known Shypple shipment page yet - process at least one shipment "
                     "first (a document-type list is captured automatically from that).",
        }
    try:
        _safe_goto(page, f"{SHYPPLE_ADMIN_BASE}{last_shipment_path}")
        open_documents_tab(page)
        options = _scrape_document_type_options(page)
        if not options:
            return {"success": False, "error": "Could not find the document type dropdown on the shipment page."}
        _save_document_type_options(options)
        return {"success": True, "options": options}
    except Exception as e:
        return {"success": False, "error": str(e)}

# Set once in main() from --role, before the HTTP server or batch loop start - read by
# run_batch's per-job dispatch as a defensive guard against a job landing on the wrong
# instance (e.g. a stale/misconfigured caller), never written anywhere else.
CURRENT_ROLE = "cmr"

STATE_LOCK = threading.Lock()
batch_state = {
    "running": False,
    "jobs": [],
    "current_index": -1,
    "paused_reason": None,  # None | "google_login"
    "started_at": None,
    "finished_at": None,
    "system_log": [],
}
proceed_event = threading.Event()
incoming_batches = queue.Queue()

# Set by the control server's /skip endpoint, alongside proceed_event, so an operator
# stuck behind a paused job (per the EMAIL_WORKFLOW_COMPLETE.md rule: no new selection
# can start while one is awaiting confirmation) can drop just that one job instead of
# being forced to either confirm it or relaunch Shypple entirely. Read (and cleared) by
# _wait_for_confirmation right after it wakes up - never read anywhere else, so it can't
# leak into a later, unrelated pause.
_skip_requested = False


def _open_confirmation_gate():
    """Clear proceed_event BEFORE the corresponding "awaiting_..." job status (or,
    for ensure_admin_access, batch_state["paused_reason"]) becomes visible to the
    operator - every caller must call this first, then make the gate visible, and
    only later call _wait_for_confirmation - never the other way round.

    This fixes a real bug: proceed_event.clear() used to live inside
    _wait_for_confirmation itself, called right before .wait(). That's fine when
    nothing but a log line sits between the status update and _wait_for_confirmation,
    but the org-switch gate (see process_one_job) also fires a best-effort blue-star
    click in that gap, which can take up to ~70s (see _star_source_email). An
    operator who clicked "Confirm switch" while that star click was still running hit
    proceed_event.set() from the /proceed handler BEFORE _wait_for_confirmation ever
    ran - so its .clear() silently wiped out that already-landed click, and the wait
    afterward blocked until a SECOND click set the event again. Clearing here, before
    the gate is even visible, closes that gap entirely: a click landing anytime after
    the button appears - including mid-star-click - sets an event that's already
    cleared and stays set until _wait_for_confirmation's .wait() picks it up, however
    long that takes."""
    proceed_event.clear()


def _wait_for_confirmation(job):
    """Block until the operator either confirms (proceed_event alone) or skips this job
    (/skip, which sets _skip_requested before also setting proceed_event to wake this
    same wait). The caller must already have called _open_confirmation_gate() before
    making the "awaiting_..." status visible - see that function's docstring for why
    this one no longer clears the event itself. Returns True to proceed with the
    paused action as normal, False if the operator chose to skip - callers must stop
    there and return without performing the action, leaving the source email
    untouched (no star, no read/unread change) so it's easy to find and reprocess
    later."""
    global _skip_requested
    proceed_event.wait()
    with STATE_LOCK:
        skipped = _skip_requested
        _skip_requested = False
    return not skipped

# Ad-hoc "go read the live document-type dropdown" requests - serviced only while idle
# (see main()'s loop), never mid-batch, since the batch-processing thread already owns
# `page` while a batch is running.
class _TypeOptionsRequest:
    def __init__(self):
        self.result = None
        self.event = threading.Event()


type_options_requests = queue.Queue()

_CONTAINER_RE = re.compile(r"[A-Z]{3}[UJZ]\d{7}")


def log_system(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[Shypple] {msg}")
    with STATE_LOCK:
        batch_state.setdefault("system_log", []).append(f"[{ts}] {msg}")


def log_job(job, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[Shypple] job={job.get('message_id')}: {msg}")
    with STATE_LOCK:
        job.setdefault("log", []).append(f"[{ts}] {msg}")


def set_job_status(job, status, **extra):
    with STATE_LOCK:
        job["status"] = status
        job.update(extra)


def _normalize_type(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


# Where locally-saved copies of documents this pipeline touches go - see
# save_document_locally()/_save_mail_document_locally(). The CURRENT machine/user's
# real Downloads folder (see get_downloads_dir) - never a hardcoded path, since this
# project runs on different company machines/accounts. Clicking a document link
# directly in Shypple's own admin UI triggers a native browser download with an
# extension-less UUID filename (Shypple's response doesn't set a usable
# Content-Disposition filename), which Chrome then can't open. Saving our own copy
# here, under the document's real filename, sidesteps that entirely - we already fetch
# these bytes anyway, either downloaded back from Shypple for the CMR deep-compare
# step, or fetched from the email itself before it's uploaded (LCL Arrivals/Release).
DOWNLOADS_DIR = get_downloads_dir()


def _safe_filename(name):
    """Strip path separators/traversal from a filename scraped off a web page before
    it's ever used to build a local file path."""
    name = os.path.basename((name or "").strip())
    return re.sub(r'[\\/:*?"<>|]', "_", name)


def _dedupe_local_path(path):
    """If path already exists (re-verifying the same document on a later run, or two
    different documents happening to share a filename), append " (2)", " (3)", ...
    rather than silently overwriting a previously-saved local copy."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    n = 2
    while os.path.exists(f"{base} ({n}){ext}"):
        n += 1
    return f"{base} ({n}){ext}"


def save_document_locally(data_bytes, filename):
    """Save already-fetched document bytes into this machine's real Downloads folder
    (DOWNLOADS_DIR) under their real filename as .pdf. Returns the saved file's
    basename."""
    safe_name = _safe_filename(filename) or "document.pdf"
    if not os.path.splitext(safe_name)[1]:
        safe_name = safe_name + ".pdf"
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    target_path = _dedupe_local_path(os.path.join(DOWNLOADS_DIR, safe_name))
    with open(target_path, "wb") as f:
        f.write(data_bytes)
    return os.path.basename(target_path)


def _save_mail_document_locally(job, doc_type, data_bytes, filename):
    """Save a document just fetched FROM THE EMAIL (before it's uploaded to Shypple -
    see handle_arrival_notice/handle_delivery_order) into the real Downloads folder,
    same as save_document_locally already does for a document downloaded back FROM
    Shypple during the CMR deep-compare step. Records it into job["downloaded_files"]
    with source="mail" so the dashboard can label it correctly (the existing entries
    from the Shypple-side save all implicitly mean source="shypple" - see
    renderOperationsPanel's job.downloaded_files block). Never raises - a failure to
    save a local copy is logged but must not abort the actual Shypple upload."""
    try:
        saved_filename = save_document_locally(data_bytes, filename or f"{doc_type}.pdf")
        log_job(job, f"Saved a local copy of '{doc_type}' ({saved_filename}) from the mail to Downloads.")
        with STATE_LOCK:
            job.setdefault("downloaded_files", []).append({"type": doc_type, "filename": saved_filename, "source": "mail"})
    except Exception as e:
        log_job(job, f"Could not save a local copy of '{doc_type}' from the mail: {e}")


def _find_uploaded_row_for_type(extracted_type, doc_rows, exclude=(), email_containers=()):
    """Which already-uploaded document row (if any) matches extracted_type AND email_containers.

    If email_containers is provided, it requires matching BOTH the document type
    AND at least one container number. If a row on Shypple matches the document type
    but is attached to a DIFFERENT container, it will not match (treating the document
    for this email's container as not uploaded yet).
    """
    target = _normalize_type(extracted_type)
    if not target:
        return None

    # Strips ALL non-alphanumeric characters (matching extract_container_from_label's
    # own normalization above), not just whitespace/hyphens - a container as extracted
    # from free-text email subjects/bodies can carry a "/" (or other punctuation)
    # separating the ISO 6346 check digit, e.g. "EGSU 031644/3", where Shypple's own
    # container tag for the exact same container reads cleanly as "EGSU0316443". Only
    # stripping whitespace/hyphens left that "/" in place on the email side, so it could
    # never match Shypple's clean container string - every row failed the container
    # check and this always fell through to "not uploaded yet", even when the type AND
    # container had genuinely already matched.
    norm_containers = set(re.sub(r"[^A-Z0-9]", "", c.upper()) for c in (email_containers or []))

    type_matched_rows = []
    for row in doc_rows:
        if id(row) in exclude:
            continue
        for raw in row.get("types") or []:
            norm = _normalize_type(raw)
            if norm and (norm == target or norm in target or target in norm):
                type_matched_rows.append(row)
                break

    if not type_matched_rows:
        return None

    if norm_containers:
        for row in type_matched_rows:
            row_containers = set(re.sub(r"[^A-Z0-9]", "", c.upper()) for c in (row.get("containers") or []))
            if norm_containers & row_containers:
                return row
        # No row on Shypple matched both type AND container number -> not uploaded yet for this container
        return None

    return type_matched_rows[0]


def _normalize_doc_filename(name):
    """Loose filename compare: strip the extension, case, and non-alphanumeric noise,
    so trivial differences (case, punctuation, a Shypple-appended ' (1)' de-dupe
    suffix) don't defeat an otherwise-exact filename match. Used to fast-path the
    "same type, same filename -> definitely the same document" case below without
    paying for a download + content compare."""
    base = os.path.splitext((name or "").strip())[0]
    return re.sub(r"[^a-z0-9]", "", base.lower())


def extract_container_from_label(label):
    """A containers-tab entry looks like "40' HC REEFER - HLBU9909920" - pull out just
    the trailing ISO-6346 container number."""
    compact = re.sub(r"[^A-Z0-9]", "", (label or "").upper())
    m = _CONTAINER_RE.search(compact)
    return m.group(0) if m else None


def ensure_shypple_login(page, email, password):
    log_system("Checking Shypple session state...")
    # Fast-path: check if we are already on an authenticated admin or dashboard page
    current_url = page.url
    if "api.shypple.com/admin" in current_url or "/dashboard" in current_url:
        log_system("Already logged in (active browser session).")
        return

    # Try navigating directly to the admin bootstrap URL first to avoid extra login redirects
    try:
        _safe_goto(page, SHYPPLE_BOOTSTRAP_URL)
        page.wait_for_timeout(1500)
        if _has_admin_access(page, timeout=3000):
            log_system("Already authenticated on Shypple admin.")
            return
    except Exception:
        pass

    # If login form appears, auto-fill credentials if provided
    try:
        page.wait_for_selector('input[name="email"]', timeout=3000)
        if email and password:
            page.fill('input[name="email"]', email)
            page.fill('input[name="password"]', password)
            page.press('input[name="password"]', "Enter")
            page.wait_for_timeout(2500)
            log_system("Submitted Shypple login form.")
    except Exception:
        log_system("No email login form present or already authenticated.")


def _has_admin_access(page, timeout=6000):
    try:
        page.wait_for_selector("#shipment-search", timeout=timeout)
        return True
    except Exception:
        return False


def _safe_click(el, timeout_ms=4000, label="element"):
    """Click an ElementHandle without letting a bad match (not found, not attached,
    still animating) take the whole calling step down with it - Playwright's default
    actionability wait is much longer than timeout_ms, and an uncaught timeout here
    previously aborted the entire step. Returns True if the click actually happened."""
    if el is None:
        return False
    try:
        el.click(timeout=timeout_ms)
        return True
    except Exception as e:
        log_system(f"_safe_click: click on {label} did not complete ({e}) - continuing anyway.")
        return False


def _safe_goto(page, url, attempts=3):
    """page.goto() that tolerates being raced by an in-flight navigation the page
    itself triggered - e.g. Google's own post-login redirect chain (a "SetSID" bounce,
    then back to the consent/continue URL) is still running when the user clicks
    Proceed, and our own goto right then collides with it ("Navigation ... interrupted
    by another navigation"). Retry a couple of times rather than treating that as fatal."""
    last_err = None
    for attempt in range(attempts):
        try:
            page.goto(url)
            return
        except Exception as e:
            last_err = e
            log_system(f"Navigation to {url} was interrupted (attempt {attempt + 1}/{attempts}): {e}")
            page.wait_for_timeout(1500)
    raise last_err


def _save_shypple_session_tokens(page):
    """Save authentication cookies, local storage, and session tokens to disk
    so 2FA and login sessions persist reliably across restarts."""
    try:
        storage_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "shypple_profile", "storage_state.json"))
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        page.context.storage_state(path=storage_path)
        log_system(f"Saved Shypple authentication & 2FA tokens to: {storage_path}")
    except Exception as e:
        log_system(f"Could not save session tokens: {e}")


def _try_google_gate_password_login(page):
    """On Shypple's Google-SSO gate, click through to the "Or click here to login
    with email and password instead" fallback form and sign in as Deepak (whose
    account has direct admin access via password, unlike SHYPPLE_EMAIL/PASSWORD's
    Google-only account) - so a batch never has to block on a human completing the
    Google OAuth flow. Returns True once the fallback form was found and submitted
    (the caller re-checks admin access itself to confirm it actually worked), False
    if the fallback link never showed up (e.g. a genuinely different gate), in which
    case the caller falls back to the manual-confirmation gate."""
    email = os.environ.get("SHYPPLE_ADMIN_EMAIL", "")
    password = os.environ.get("SHYPPLE_ADMIN_PASSWORD", "")
    if not email or not password:
        log_system("SHYPPLE_ADMIN_EMAIL/SHYPPLE_ADMIN_PASSWORD not set - cannot auto-fill the Google-login fallback form.")
        return False
    try:
        toggle = page.wait_for_selector('a[data-target="#passwordSection"]', timeout=5000)
    except Exception:
        return False
    try:
        toggle.click()
        page.wait_for_selector('#user_email', timeout=5000)
        page.fill('#user_email', email)
        page.fill('#user_password', password)
        page.click('#passwordSection input[type="submit"]')
        log_system("Submitted Shypple admin sign-in with email/password (Google-login fallback).")
        return True
    except Exception as e:
        log_system(f"Could not complete email/password fallback sign-in: {e}")
        return False


def ensure_admin_access(page):
    """Navigate to the admin app; if it's gated behind Google SSO, sign in via the
    email/password fallback form as Deepak instead of pausing for a human to
    complete Google sign-in. Only falls back to the manual confirmation gate if
    that fallback form isn't present or the sign-in doesn't actually land on an
    admin page."""
    log_system("Opening Shypple admin...")
    _safe_goto(page, SHYPPLE_BOOTSTRAP_URL)
    page.wait_for_timeout(2000)
    if _has_admin_access(page):
        _save_shypple_session_tokens(page)
        return

    if _try_google_gate_password_login(page):
        page.wait_for_timeout(2000)
        if _has_admin_access(page, timeout=8000):
            log_system("Admin access confirmed (email/password sign-in).")
            _save_shypple_session_tokens(page)
            return
        try:
            _safe_goto(page, SHYPPLE_BOOTSTRAP_URL)
            page.wait_for_timeout(2000)
        except Exception as e:
            log_system(f"Navigation after email/password sign-in was interrupted, checking current page anyway: {e}")
        if _has_admin_access(page, timeout=10000):
            log_system("Admin access confirmed.")
            _save_shypple_session_tokens(page)
            return
        log_system("Email/password fallback sign-in did not reach the admin page - falling back to manual Google sign-in.")

    log_system("Admin access needs a manual Google sign-in. Waiting for Proceed...")
    _open_confirmation_gate()
    with STATE_LOCK:
        batch_state["paused_reason"] = "google_login"
    proceed_event.wait()
    with STATE_LOCK:
        batch_state["paused_reason"] = None

    # Google's own post-login redirect chain can still be mid-bounce right as Proceed is
    # clicked. Give it a moment to settle, then check whether it already landed us on a
    # usable admin page BEFORE issuing our own navigation - clicking Proceed the instant
    # that chain lands (as it did here) is exactly what caused the old "interrupted by
    # another navigation" failure, even though the page ends up exactly where we want.
    page.wait_for_timeout(2000)
    if _has_admin_access(page, timeout=5000):
        log_system("Admin access confirmed (already on the right page after sign-in).")
        _save_shypple_session_tokens(page)
        return

    try:
        _safe_goto(page, SHYPPLE_BOOTSTRAP_URL)
        page.wait_for_timeout(2000)
    except Exception as e:
        log_system(f"Navigation after sign-in was interrupted, checking current page anyway: {e}")

    if not _has_admin_access(page, timeout=10000):
        raise RuntimeError("Still can't reach the Shypple admin after manual login - please retry.")
    log_system("Admin access confirmed.")
    _save_shypple_session_tokens(page)


def _switch_shypple_org(page, org_name):
    """Switch the Shypple admin session to ``org_name`` if it isn't already active.

    Reads the trigger button's current text - if it already says ``org_name``,
    returns immediately (idempotent). Otherwise opens the modal and clicks the
    quick-link whose visible text is EXACTLY ``org_name`` (trimmed, exact-match only -
    "Shypple B.V.", "Shypple Fresh B.V." and "Shypple Asia Ltd." all contain "Shypple"
    and would false-match on any substring). After the click, re-reads the trigger
    button to confirm the switch landed; if it didn't, raises - callers proceeding
    under the wrong org would silently attach every upload to the wrong company."""
    log_system(f"Checking active Shypple organization (expected: {org_name})...")
    try:
        trigger_text = page.evaluate(
            '() => { const b = document.querySelector("button[data-target=\'#organization-impersonate\']"); return b ? b.textContent.trim() : null; }'
        )
    except Exception as e:
        raise RuntimeError(f"Could not read the org-switch trigger button: {e}")

    if trigger_text is None:
        raise RuntimeError(
            "Org-switch trigger button not found - ensure_admin_access must succeed before calling this."
        )

    if trigger_text.strip() == org_name:
        log_system(f"Organization already set to '{org_name}' - no switch needed.")
        return

    log_system(f"Current org is '{trigger_text}' - switching to '{org_name}'...")

    modal_appeared = False
    for attempt in range(3):
        trigger_handle = page.evaluate_handle(
            '() => document.querySelector("button[data-target=\'#organization-impersonate\']")',
        )
        trigger_el = trigger_handle.as_element()
        if not trigger_el:
            raise RuntimeError("Org-switch trigger button not found.")
        
        log_system(f"Clicking org-switch trigger button (attempt {attempt + 1})...")
        if _safe_click(trigger_el, timeout_ms=3000, label="org-switch trigger button"):
            try:
                page.wait_for_selector("#organization-impersonate", timeout=3000)
                modal_appeared = True
                break
            except Exception:
                log_system("Modal did not appear yet, retrying click...")
                page.wait_for_timeout(1000)
        else:
            log_system("Failed to click the trigger button, retrying...")
            page.wait_for_timeout(1000)

    if not modal_appeared:
        raise RuntimeError("Org-switch modal (#organization-impersonate) did not appear.")
    page.wait_for_timeout(300)

    # Exact text match only - substrings like "Shypple" would match multiple orgs.
    clicked = page.evaluate(
        """(name) => {
            const modal = document.getElementById('organization-impersonate');
            if (!modal) return false;
            const link = Array.from(modal.querySelectorAll('a')).find(a => a.textContent.trim() === name);
            if (!link) return false;
            link.click();
            return true;
        }""",
        org_name,
    )
    if not clicked:
        # Fall back to the select2 + Switch-button form path.
        log_system(f"Quick-link for '{org_name}' not found - trying select2 form fallback.")
        picked = _select2_pick(page, "organization_id", org_name)
        if not picked:
            raise RuntimeError(
                f"Could not find '{org_name}' in either the quick-link list "
                "or the select2 dropdown of the org-switch modal."
            )
        submit_handle = page.evaluate_handle(
            """() => {
                const modal = document.getElementById('organization-impersonate');
                return modal ? modal.querySelector('input[type="submit"]') : null;
            }"""
        )
        submit_el = submit_handle.as_element()
        if not _safe_click(submit_el, timeout_ms=6000, label="org-switch Submit button"):
            raise RuntimeError("Could not click the org-switch Submit button.")

    # Switching organization triggers a real page reload in Shypple's admin UI (not
    # just an in-place AJAX update) - a fixed 2s sleep was sometimes shorter than that
    # actual navigation, so the re-read below landed while Chrome had already thrown
    # away the old page's JS context for the incoming document, raising "Execution
    # context was destroyed, most likely because of a navigation". Wait for the
    # navigation to actually settle first, then retry the re-read a few times in case
    # a residual race (e.g. a second redirect right behind the first) still catches it.
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(1000)

    # A None read here means the button plain wasn't found in the DOM yet (JS "b ? ... :
    # null" - not the string "None" naming some other org), and str-formats into the
    # error below as the literal word "None", easy to misread as an actual org name.
    # The loop below previously only retried on a raised JS exception (execution
    # context destroyed mid-navigation) and treated a clean None result as final -
    # but the post-switch reload can easily still be rendering this button on the
    # first read or two, well within the wait_for_load_state above having already
    # returned, so that null was often just "not rendered yet," not a real failure.
    new_text, last_err = None, None
    for attempt in range(4):
        try:
            new_text = page.evaluate(
                '() => { const b = document.querySelector("button[data-target=\'#organization-impersonate\']"); return b ? b.textContent.trim() : null; }'
            )
            last_err = None
            if new_text is not None:
                break
        except Exception as e:
            last_err = e
        page.wait_for_timeout(1000 * (attempt + 1))
    if last_err is not None:
        raise RuntimeError(f"Could not re-read the org-switch trigger button after switching: {last_err}")

    # Still not found after retrying in place - one more possibility before giving up:
    # the post-switch reload landed somewhere other than a shipment page entirely
    # (this button only lives on pages under the admin shell that show it), not just
    # "still rendering." Re-navigate to the known-good bootstrap shipment page once and
    # re-check there before concluding the switch genuinely failed.
    if new_text is None:
        log_system("Org-switch trigger button still not found after retrying in place - "
                    "re-navigating to the admin shipment page to check there.")
        _safe_goto(page, SHYPPLE_BOOTSTRAP_URL)
        page.wait_for_timeout(1500)
        try:
            new_text = page.evaluate(
                '() => { const b = document.querySelector("button[data-target=\'#organization-impersonate\']"); return b ? b.textContent.trim() : null; }'
            )
        except Exception as e:
            raise RuntimeError(f"Could not re-read the org-switch trigger button after re-navigating: {e}")

    if (new_text or "").strip() != org_name:
        raise RuntimeError(
            f"Org-switch did not land: trigger button now reads '{new_text}', "
            f"expected '{org_name}'. Stopping to avoid uploading under the wrong organization."
        )
    log_system(f"Organization successfully switched to '{org_name}'.")


def ensure_org_is_shypple_bv(page):
    """Switch to TARGET_ORG_NAME ("Shypple B.V.") - called once per batch, right after
    ensure_admin_access, before any per-job container search or upload work begins, and
    again after a per-job Fresh B.V. detour (see switch_to_fresh_org) so the NEXT job
    in the batch starts back under the default org."""
    _switch_shypple_org(page, TARGET_ORG_NAME)


def switch_to_fresh_org(page):
    """Switch to FRESH_ORG_NAME ("Shypple Fresh B.V.") - used when a matched shipment's
    organization comes back empty under TARGET_ORG_NAME, since some shipments only
    exist under this second legal entity. See process_one_job; the caller is
    responsible for switching back via ensure_org_is_shypple_bv once done with this
    job."""
    _switch_shypple_org(page, FRESH_ORG_NAME)


def _ensure_org_before_documents_tab(page, job, expected_org=None):
    """Safety-net check run right before every Documents-tab open, across both the
    CMR and LCL Arrivals/Release pipelines: verifies the currently active Shypple
    organization is expected_org (defaults to TARGET_ORG_NAME, "Shypple B.V." - the
    primary org for every shipment) and switches to it first if not.
    _switch_shypple_org is already idempotent (a no-op if the expected org is already
    active), so this is cheap on the normal path - it only does real work if an
    earlier job in the same batch left Shypple on the wrong organization (e.g. a
    failed Fresh B.V. switch-back after that job's own upload), which would otherwise
    silently carry over into THIS job's document upload with no warning.

    A CMR job whose shipment was only found under FRESH_ORG_NAME via the
    empty-organization fallback (see process_one_job/switch_to_fresh_org) must pass
    job.get("organization_switched_to") as expected_org - forcing TARGET_ORG_NAME here
    for that job would incorrectly switch AWAY from the only organization its
    shipment actually exists under, mid-upload.

    Sets the job to "error" and returns False on failure so the caller can stop
    before opening Documents tab; True means it's safe to proceed."""
    expected_org = expected_org or TARGET_ORG_NAME
    try:
        _switch_shypple_org(page, expected_org)
        return True
    except Exception as e:
        set_job_status(job, "error",
                        error=f"Could not verify/switch to organization '{expected_org}' before opening "
                              f"the Documents tab: {e}")
        log_job(job, f"Organization check before Documents tab failed: {e}")
        return False


def find_matching_shipment(page, year):
    """Reads the search-results table's own <thead> to locate the Organization/ETA/
    Status columns (robust to column reordering). ETA year is a hard requirement (must
    equal ``year``, and must not be empty); a Status of Cancelled/Deleted is also a
    hard disqualifier (checked here rather than left to the caller, since it lives in
    the same row as ETA); Organization is informational only - real shipments
    legitimately have it blank, so a blank org never disqualifies a row on its own.
    Always returns every candidate row's raw values too, so a non-match can be
    explained precisely (empty ETA vs. past-date ETA vs. cancelled/deleted vs. empty
    organization) instead of a generic "no match"."""
    return page.evaluate(
        """(year) => {
            const table = document.querySelector('.table-responsive table');
            if (!table) return { matched: false, candidates: [] };
            const headerCells = Array.from(table.querySelectorAll('thead th'));
            let orgIdx = -1, etaIdx = -1, statusIdx = -1, col = 0;
            headerCells.forEach(th => {
                const span = parseInt(th.getAttribute('colspan') || '1', 10);
                const text = th.textContent.trim().toLowerCase();
                if (text === 'organization') orgIdx = col;
                if (text === 'eta') etaIdx = col;
                if (text === 'status') statusIdx = col;
                col += span;
            });

            const rows = Array.from(table.querySelectorAll('tbody tr.shipment-search-result'));
            const candidates = rows.map(row => {
                const cells = row.querySelectorAll('td');
                const orgCell = orgIdx >= 0 ? cells[orgIdx] : null;
                const etaCell = etaIdx >= 0 ? cells[etaIdx] : null;
                const statusCell = statusIdx >= 0 ? cells[statusIdx] : null;
                const org = orgCell ? orgCell.textContent.trim() : '';
                const etaTimeEl = etaCell ? etaCell.querySelector('time') : null;
                const etaDatetime = etaTimeEl ? etaTimeEl.getAttribute('datetime') : '';
                const etaYear = etaDatetime ? new Date(etaDatetime).getFullYear() : null;
                const status = statusCell ? statusCell.textContent.trim() : '';
                const statusLower = status.toLowerCase();
                const cancelledOrDeleted = /cancel|delet/.test(statusLower);
                return {
                    path: row.getAttribute('data-shipment-path'), org: org, eta: etaDatetime,
                    etaYear: etaYear, status: status, cancelledOrDeleted: cancelledOrDeleted,
                };
            });

            const qualifying = candidates.filter(c => c.eta && c.etaYear === year && !c.cancelledOrDeleted);
            if (qualifying.length === 0) {
                return { matched: false, candidates: candidates };
            }
            // Organization is informational, not a hard requirement - but if several
            // current-year rows qualify, prefer one that has it set as a tie-breaker.
            const withOrg = qualifying.find(c => c.org);
            const chosen = withOrg || qualifying[0];
            return { matched: true, chosen: chosen, ambiguous: qualifying.length > 1, candidates: candidates };
        }""",
        year,
    )


def _describe_candidate_issue(candidate, year):
    """Human-readable reason a search-result row was NOT accepted, so a "no match" can
    be reported precisely instead of vaguely."""
    org = candidate.get("org") or ""
    eta = candidate.get("eta") or ""
    eta_year = candidate.get("etaYear")
    status = candidate.get("status") or ""
    issues = []
    if candidate.get("cancelledOrDeleted"):
        issues.append(f"status is '{status}' (cancelled/deleted)")
    if not eta:
        issues.append("ETA is empty")
    elif eta_year != year:
        if eta_year is not None and eta_year < year:
            issues.append(f"ETA is a past date ({eta_year})")
        else:
            issues.append(f"ETA is not the current year ({eta_year})")
    if not org:
        issues.append("organization has no data (informational only, not disqualifying)")
    return ", ".join(issues) if issues else "unknown reason"


def _open_tab(page, link_selector):
    href = page.get_attribute(link_selector, "href")
    if not href:
        return False
    page.goto(SHYPPLE_ADMIN_BASE + href)
    return True


def open_containers_tab(page):
    if not _open_tab(page, "#containers-link"):
        raise RuntimeError("Containers tab link not found on the shipment page.")
    page.wait_for_selector(".containers-list", timeout=10000)


def open_containers_tab_lcl(page):
    """LCL-only variant of open_containers_tab. The CMR flow
    (_verify_and_upload_documents) waits on '.containers-list' because it reads
    container labels out of that list - a real requirement there. The LCL helpers
    below (check_container_tab_data/add_container_and_devanning_date) only ever read
    the two input fields <input id="container_container_number">/
    <input id="container_devanning_date"> (confirmed real DOM - see
    add_container_and_devanning_date's docstring), which render regardless of whether
    '.containers-list' shows any rows. Waiting on '.containers-list' there was timing
    out (10s) on real shipments and blocking the Delivery order / delay-or-devanning
    jobs for no reason - wait on the field itself instead."""
    if not _open_tab(page, "#containers-link"):
        raise RuntimeError("Containers tab link not found on the shipment page.")
    page.wait_for_selector("#container_container_number", timeout=10000)


def open_documents_tab(page):
    if not _open_tab(page, "#shipment-documents-link"):
        raise RuntimeError("Documents tab link not found on the shipment page.")
    page.wait_for_selector("#shipment-document-table", timeout=10000)


def scrape_document_rows(page):
    """Per-row breakdown of the Documents tab: each already-uploaded document's
    type(s), container(s), and its download link - richer than a flat type list,
    since verifying content requires fetching the SPECIFIC row's file, not just
    knowing the type appears somewhere in the table.

    Reads document type(s) and container(s) from both native <option> elements and
    jQuery Select2 choice elements for 100% accuracy."""
    return page.evaluate("""() => {
        const rows = Array.from(document.querySelectorAll('#shipment-document-table .body form'));
        return rows.map(row => {
            const typeSelect = row.querySelector('.document_types_selector');
            let types = [];
            if (typeSelect) {
                const optTypes = Array.from(typeSelect.options)
                    .filter(o => o.selected || o.hasAttribute('selected'))
                    .map(o => (o.textContent || '').trim());
                const choiceTypes = Array.from(row.querySelectorAll('.document_types_selector ~ .select2 .select2-selection__choice'))
                    .map(el => (el.getAttribute('title') || el.textContent || '').replace(/^[×x]/i, '').trim());
                types = Array.from(new Set([...optTypes, ...choiceTypes])).filter(Boolean);
            }
            const containerSelect = row.querySelector('.containers_selector')
                || row.querySelector('.document_containers_selector')
                || row.querySelector('select[id*="container_ids"]')
                || row.querySelector('select[name*="container_ids"]');
            let containers = [];
            if (containerSelect) {
                const optContainers = Array.from(containerSelect.options)
                    .filter(o => o.selected || o.hasAttribute('selected'))
                    .map(o => (o.textContent || '').trim());
                const choiceContainers = Array.from(row.querySelectorAll('.containers_selector ~ .select2 .select2-selection__choice'))
                    .map(el => (el.getAttribute('title') || el.textContent || '').replace(/^[×x]/i, '').trim());
                containers = Array.from(new Set([...optContainers, ...choiceContainers])).filter(Boolean);
            }
            const link = row.querySelector('a[href*="/download"]');
            return {
                types: types,
                containers: containers,
                downloadHref: link ? link.getAttribute('href') : null,
                filename: link ? link.textContent.trim() : null,
            };
        });
    }""")


def click_save_containers_changes(page):
    """Click "Save changes" (<button class="btn btn-primary" name="commit"
    type="submit" value="Update">Save changes</button>) on the Containers tab - the
    actual submit for the container number/devanning date mini-form
    add_container_and_devanning_date fills. Matched by visible TEXT ("save changes"),
    not the value="Update" attribute - Shypple reuses that same value on the Edit
    tab's differently-labeled "Update Shipment" button (#shipment-update-button), so
    matching on value alone would be ambiguous if both forms were ever on the page at
    once. Per the operator's explicit confirmation, this mini-form does NOT auto-save
    on fill/blur - this was an open, unconfirmed assumption when the pipeline was
    first built (see the plan file referenced in the pipeline's own memory notes).
    Same validation-error-scan pattern as submit_shipment_document_form/
    click_update_shipment, since Shypple's own server-side validation can reject a
    save silently except for a banner."""
    try:
        btn_handle = page.evaluate_handle("""() => {
            const buttons = Array.from(document.querySelectorAll('button[type="submit"]'));
            return buttons.find(b => /save changes/i.test((b.textContent || '').trim()));
        }""")
        btn = btn_handle.as_element()
        if btn is None:
            return {"success": False, "error": "Save changes button not found on the Containers tab."}
        if not _safe_click(btn, timeout_ms=8000, label="Save changes button"):
            return {"success": False, "error": "Save changes button click did not complete."}
        page.wait_for_timeout(1500)
        error_text = page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll(
                '.alert, .alert-danger, .alert-error, [role="alert"], .invalid-feedback, .flash, .notice, .toast'
            ));
            for (const el of candidates) {
                if (el.offsetParent === null) continue;
                const text = (el.innerText || el.textContent || '').trim();
                if (text && /can.?t be blank|must be greater than|invalid|error/i.test(text)) {
                    return text;
                }
            }
            return null;
        }""")
        if error_text:
            return {"success": False, "error": error_text[:300]}
        return {"success": True, "error": None}
    except Exception as e:
        return {"success": False, "error": str(e)}


def add_container_and_devanning_date(page, container_number, devanning_date, save=True):
    """Fill container number and devanning date on the Containers tab:
    <input class="form-control" placeholder="Container number" type="text" name="container[container_number]" id="container_container_number">
    <input class="form-control" placeholder="Devanning Date" value="..." type="date" name="container[devanning_date]" id="container_devanning_date">

    ``save=True`` (default) also clicks "Save changes" (click_save_containers_changes)
    to actually persist it - pass save=False to only fill the fields (e.g. to build a
    review preview before the operator has confirmed anything should be written yet)
    and call click_save_containers_changes separately once they have. Returns
    {"success": True, "error": None} when save=False (nothing to fail yet), otherwise
    click_save_containers_changes's result."""
    open_containers_tab_lcl(page)
    if container_number:
        c_el = page.query_selector("#container_container_number")
        if c_el:
            c_el.fill(container_number)
    if devanning_date:
        d_el = page.query_selector("#container_devanning_date")
        if d_el:
            d_el.fill(devanning_date)
    if not save:
        return {"success": True, "error": None}
    return click_save_containers_changes(page)


def check_container_tab_data(page):
    """Check if Containers tab has data in container number and devanning date fields."""
    open_containers_tab_lcl(page)
    return page.evaluate("""() => {
        const cEl = document.getElementById('container_container_number');
        const dEl = document.getElementById('container_devanning_date');
        let cVal = cEl ? cEl.value.trim() : "";
        let dVal = dEl ? dEl.value.trim() : "";
        const rows = document.querySelectorAll('.containers-list tbody tr, .container-row');
        
        if ((!cVal || !dVal) && rows.length > 0) {
            for (const row of rows) {
                // Find container number in row
                let rowContainer = "";
                const links = row.querySelectorAll('a');
                for (const link of links) {
                    const txt = link.textContent.trim();
                    if (/[A-Z]{4}\\d{7}/i.test(txt)) {
                        rowContainer = txt.toUpperCase();
                        break;
                    }
                }
                if (!rowContainer) {
                    const cells = row.querySelectorAll('td');
                    for (const cell of cells) {
                        const txt = cell.textContent.trim();
                        if (/[A-Z]{4}\\d{7}/i.test(txt)) {
                            rowContainer = txt.toUpperCase();
                            break;
                        }
                    }
                }

                // Find devanning date in row
                let rowDate = "";
                const dateInput = row.querySelector('input[type="date"], input[name*="devanning_date"], input[id*="devanning_date"], input[name*="date"], input[id*="date"]');
                if (dateInput) {
                    rowDate = dateInput.value.trim();
                }
                if (!rowDate) {
                    const inputs = row.querySelectorAll('input');
                    for (const input of inputs) {
                        const val = input.value.trim();
                        if (/^\\d{4}-\\d{2}-\\d{2}$/.test(val) || /^\\d{2}[-/]\\d{2}[-/]\\d{4}$/.test(val)) {
                            rowDate = val;
                            break;
                        }
                    }
                }
                if (!rowDate) {
                    const cells = row.querySelectorAll('td');
                    for (const cell of cells) {
                        const txt = cell.textContent.trim();
                        const m1 = txt.match(/\\b\\d{4}-\\d{2}-\\d{2}\\b/);
                        if (m1) {
                            rowDate = m1[0];
                            break;
                        }
                        const m2 = txt.match(/\\b(\\d{2})[-/](\\d{2})[-/](\\d{4})\\b/);
                        if (m2) {
                            rowDate = m2[3] + '-' + m2[2] + '-' + m2[1];
                            break;
                        }
                    }
                }

                if (rowContainer && !cVal) {
                    cVal = rowContainer;
                }
                if (rowDate && !dVal) {
                    dVal = rowDate;
                }
                if (cVal && dVal) {
                    break;
                }
            }
        }

        return {
            has_container: !!cVal || rows.length > 0,
            has_devanning_date: !!dVal,
            container_number: cVal,
            devanning_date: dVal
        };
    }""")


def edit_preceding_customs_and_cfs(page, customs_number, cfs_address):
    """Click Edit tab (<a class="btn btn-primary ml-2" href="/admin/shipments/.../edit">Edit</a>),
    fill Preceding Customs Number (#shipment_preceding_customs_number),
    and select Discharge CFS (#select2-shipment_discharge_cfs_id-container).

    Discharge CFS is matched against the extracted free text by longest-common-
    prefix (see the in-page match() below) - NOT a blind first-3-letters-only
    match. Returns {"cfs_matched": bool, "cfs_selected_text": str|None} so
    callers can tell a confident match from a shot in the dark; callers must
    NOT treat cfs_address as "verified"/"corrected" when cfs_matched is False.
    """
    edit_link = page.query_selector('a[href*="/edit"]')
    if edit_link:
        href = edit_link.get_attribute("href")
        if href:
            _safe_goto(page, SHYPPLE_ADMIN_BASE + href)

    page.wait_for_selector("#shipment_preceding_customs_number", timeout=8000)

    if customs_number:
        cust_el = page.query_selector("#shipment_preceding_customs_number")
        if cust_el:
            cust_el.fill(customs_number)

    result = {"cfs_matched": None, "cfs_selected_text": None}
    if cfs_address:
        # Open select2 for shipment_discharge_cfs_id
        _select2_open(page, "shipment_discharge_cfs_id")
        page.wait_for_timeout(300)

        # Root cause of a real bug: Shypple's Discharge CFS options can share the
        # same first 3 letters (confirmed: "CTG Export" AND "CTG Logistics B.V."
        # both start with "CTG") - a plain first-3-letters match picks whichever
        # of them happens to come FIRST in DOM order, silently saving the WRONG
        # warehouse even though the extracted text (from a document that plainly
        # said e.g. "CTG Logistics") was correct. Worse, the old code's fallback
        # when NOTHING matched was to blindly pick sel.options[1] - some
        # arbitrary "first real option" - which is never the right answer and
        # was reported back as a success ("matched": true).
        #
        # Fixed by: (1) keeping the one CONFIRMED, operator-verified collision
        # (any extracted text containing "CTG" - whether the bare word or a full
        # free-text name like "CTG Cargo Terminal Gadering BV" - always means CTG
        # Logistics on the real dropdown, never CTG Export) as an explicit rule
        # checked first; (2) for everything else, matching against the FULL extracted text at
        # decreasing prefix lengths (longest first) and only accepting a
        # length once it resolves to EXACTLY ONE option - so "CTG Logistics"
        # (13 chars) matches its one option directly, long before the search
        # ever reaches the ambiguous 3-letter "CTG" prefix that both options
        # share; (3) never guessing - if no prefix length down to 3 chars
        # resolves to a single option, the dropdown is left UNCHANGED and
        # cfs_matched comes back False instead of a wrong option being saved.
        result = page.evaluate("""(fullTarget) => {
            const sel = document.getElementById('shipment_discharge_cfs_id');
            if (!sel) return {cfs_matched: false, cfs_selected_text: null};
            const norm = s => (s || '').replace(/\\s+/g, ' ').trim().toUpperCase();
            const options = Array.from(sel.options).filter(o => o.value);
            const target = norm(fullTarget);

            const finalize = (opt) => {
                sel.value = opt.value;
                if (window.jQuery) {
                    window.jQuery(sel).trigger('change').trigger('select2:select');
                }
                return {cfs_matched: true, cfs_selected_text: (opt.textContent || '').trim()};
            };

            if (target.includes('CTG')) {
                const ctgLogistics = options.find(o => norm(o.textContent).includes('CTG LOGISTICS'));
                if (ctgLogistics) return finalize(ctgLogistics);
            }

            for (let len = target.length; len >= 3; len--) {
                const prefix = target.slice(0, len);
                const candidates = options.filter(o => norm(o.textContent).startsWith(prefix));
                if (candidates.length === 1) return finalize(candidates[0]);
                if (candidates.length > 1 && len === 3) break;
            }

            return {cfs_matched: false, cfs_selected_text: null};
        }""", cfs_address)
        log_system(
            f"Discharge CFS for '{cfs_address}': "
            + (f"matched '{result.get('cfs_selected_text')}'" if result.get("cfs_matched")
               else "NO confident match found - left unchanged, needs manual review")
        )
    return result


def check_edit_tab_customs_and_cfs(page):
    """Read-only check of the shipment Edit tab's Preceding Customs Number
    (#shipment_preceding_customs_number) and Discharge CFS (#shipment_discharge_cfs_id)
    fields - the SAME selectors edit_preceding_customs_and_cfs uses to WRITE these
    fields for the Arrival Notice flow, reused here purely to report whether Shypple
    already has data in them. Delivery order's manual-verification step (see
    handle_delivery_order) only needs to SHOW these values to the operator alongside
    the Containers tab's container/devanning date - it never fills or saves anything
    on this tab, unlike the Arrival Notice flow."""
    edit_link = page.query_selector('a[href*="/edit"]')
    if edit_link:
        href = edit_link.get_attribute("href")
        if href:
            _safe_goto(page, SHYPPLE_ADMIN_BASE + href)
    page.wait_for_selector("#shipment_preceding_customs_number", timeout=8000)
    return page.evaluate("""() => {
        const custEl = document.getElementById('shipment_preceding_customs_number');
        const custVal = custEl ? custEl.value.trim() : "";
        const cfsSel = document.getElementById('shipment_discharge_cfs_id');
        let cfsVal = "";
        if (cfsSel && cfsSel.selectedOptions && cfsSel.selectedOptions.length) {
            const opt = cfsSel.selectedOptions[0];
            if (opt && opt.value) cfsVal = (opt.textContent || '').trim();
        }
        return {
            customs_number: custVal,
            has_customs_number: !!custVal,
            cfs_address: cfsVal,
            has_cfs_address: !!cfsVal,
        };
    }""")


def _looks_like_error_page(data, mime):
    """True if ``data``/``mime`` looks like an HTML/XML error or login page rather
    than a real document - e.g. Shypple's session cookie got rejected, or an S3/GCS
    presigned URL returned an <Error>/<AccessDenied> XML body. A bare "not None"
    check on fetch_bytes_robust's result isn't enough to catch this: the fetch
    genuinely succeeds (HTTP 200, non-empty body), it's just NOT the file - and
    saving/serving that mislabeled blob as if it were the real document is exactly
    what previously showed up as a blank/white page when the operator opened it,
    since a PDF viewer can't render an HTML or XML payload. Deliberately
    conservative: only flags the unambiguous case (empty body, or a body that starts
    with HTML/XML markup) so a real binary document is never wrongly rejected."""
    if not data:
        return True
    mime_clean = (mime or "").split(";")[0].strip().lower()
    if mime_clean in ("text/html", "application/xhtml+xml", "application/xml", "text/xml"):
        return True
    head = data[:256].lstrip().lower()
    return head.startswith((b"<!doctype html", b"<html", b"<?xml", b"<error"))


def download_shypple_document_bytes(page, href):
    """Fetch an already-uploaded Shypple document's raw bytes via the authenticated
    browser session. Uses shared.download_utils.fetch_bytes_robust: a plain
    page.request.get (reuses the browser's own cookies, no separate auth needed),
    handling redirects to S3 presigned URLs without auth header pollution, and
    falling back to native Playwright download / in-page JS fetch if rejected.
    _looks_like_error_page guards each technique in turn - a technique that
    "succeeds" with an HTML/XML error or login page instead of the real file is
    treated the same as an outright failure, so the next technique gets a chance
    instead of that mislabeled content silently being saved/served as the document
    (see its own docstring for why this matters)."""
    if not href:
        return None, None
    url = href if href.startswith("http") else SHYPPLE_ADMIN_BASE + href
    try:
        data, mime = fetch_bytes_robust(page, url)
        if data is not None and not _looks_like_error_page(data, mime):
            return data, mime
        if data is not None:
            log_system(f"download_shypple_document_bytes: fetch_bytes_robust returned an HTML/XML "
                       f"error/login page instead of the document (mime={mime!r}) - trying native download instead.")
    except Exception as e:
        log_system(f"download_shypple_document_bytes fetch error: {e}")

    # Fallback to in-page native Playwright download if page has the link
    try:
        if page:
            clean_href = href.split("?")[0]
            link_loc = page.locator(f'a[href="{href}"], a[href*="{clean_href}"]')
            if link_loc.count() > 0:
                with page.expect_download(timeout=10000) as download_info:
                    link_loc.first.click(force=True)
                download = download_info.value
                temp_path = download.path()
                if temp_path and os.path.exists(temp_path):
                    with open(temp_path, "rb") as f:
                        data = f.read()
                    return data, "application/pdf"
    except Exception as e:
        log_system(f"download_shypple_document_bytes native download fallback error: {e}")

    return None, None


def read_shipment_customer(page):
    """Read the Customer/organization name from the shipment's Info tab (id="show-link")
    - needed to tag a newly-uploaded document's organization, skipped entirely for
    Arrival notice/Delivery order/T1 DOC per the operator's rule. Leaves the browser on
    the Info tab; the caller is responsible for navigating back to Documents."""
    try:
        info_href = page.get_attribute("#show-link", "href")
        if info_href:
            page.goto(info_href if info_href.startswith("http") else SHYPPLE_ADMIN_BASE + info_href)
        page.wait_for_timeout(800)
        return page.evaluate("""() => {
            const blocks = Array.from(document.querySelectorAll('div'));
            for (const b of blocks) {
                const label = b.querySelector(':scope > span.text-muted');
                if (label && label.textContent.trim() === 'Customer') {
                    const link = b.querySelector('a');
                    if (link) return link.textContent.trim();
                }
            }
            return null;
        }""")
    except Exception as e:
        log_system(f"read_shipment_customer error: {e}")
        return None


def verify_info_tab(page):
    """Verify cluster, load type, and customer name on Shypple shipment Info tab:
    - Returns cluster_name, is_cluster_3 (bool), load_type, is_lcl (bool), customer_name.
    """
    try:
        info_href = page.get_attribute("#show-link", "href")
        if info_href:
            page.goto(info_href if info_href.startswith("http") else SHYPPLE_ADMIN_BASE + info_href)
        page.wait_for_timeout(1000)

        data = page.evaluate("""() => {
            let clusterText = "";
            let isCluster3 = false;
            let loadType = "";
            let customerName = "";

            // Check cluster selector: <div class="row"><div class="col-4"><b>Cluster</b></div><div class="col"><a href="/admin/clusters/1">Cluster 1</a></div></div>
            const rows = Array.from(document.querySelectorAll('.row'));
            for (const r of rows) {
                const b = r.querySelector('.col-4 b');
                if (b && b.textContent.trim() === 'Cluster') {
                    const a = r.querySelector('.col a');
                    if (a) clusterText = a.textContent.trim();
                }
            }
            if (!clusterText) {
                const clusterEl = document.querySelector('a[href*="/admin/clusters/"]');
                if (clusterEl) clusterText = clusterEl.textContent.trim();
            }
            if (clusterText.toLowerCase().includes('cluster 3')) {
                isCluster3 = true;
            }

            // Check load type selector: <span class="load-type">LCL</span>
            const ltEl = document.querySelector('span.load-type');
            if (ltEl) {
                loadType = ltEl.textContent.trim().toUpperCase();
            }

            // Check customer selector: <span class="text-muted text-md">Customer</span><a ...>
            const customerBlocks = Array.from(document.querySelectorAll('div'));
            for (const cb of customerBlocks) {
                const lbl = cb.querySelector('.text-muted');
                if (lbl && lbl.textContent.trim().toLowerCase() === 'customer') {
                    const a = cb.querySelector('a');
                    if (a) customerName = a.textContent.trim();
                }
            }

            return {
                clusterText: clusterText,
                isCluster3: isCluster3,
                loadType: loadType,
                isLcl: loadType === 'LCL',
                customerName: customerName
            };
        }""")
        return data
    except Exception as e:
        log_system(f"verify_info_tab error: {e}")
        return {"clusterText": "", "isCluster3": False, "loadType": "", "isLcl": False, "customerName": ""}


def _search_shipment_box(page, query, query_label):
    """Shared #shipment-search lookup: type ``query`` into it, press Enter, and return
    the first result row's shipment path. Used both by find_shipment_by_sf_number's own
    fallback and find_shipment_by_container_number - identical mechanism, just a
    different search term. Returns {"success": bool, "shipment_path": str or None,
    "error": str or None}."""
    try:
        _safe_goto(page, f"{SHYPPLE_ADMIN_BASE}/admin/shipments")
        page.wait_for_selector("#shipment-search", timeout=15000)
        page.fill("#shipment-search", query or "")
        page.press("#shipment-search", "Enter")
        page.wait_for_selector(".table-responsive", timeout=15000)
        page.wait_for_timeout(500)
        shipment_path = page.evaluate("""() => {
            const row = document.querySelector(
                '.table-responsive table tbody tr.shipment-search-result, .table-responsive table tbody tr'
            );
            return row ? row.getAttribute('data-shipment-path') : null;
        }""")
        if not shipment_path:
            return {"success": False, "shipment_path": None, "error": f"No shipment found searching for {query_label} '{query}'."}

        # Finding the row only tells us WHICH shipment matched - page is still sitting on
        # this search-results list, not the shipment's own record. Callers downstream
        # (verify_info_tab, etc.) assume page IS the shipment record - find_shipment_by_
        # sf_number's direct-nav shortcut already lands there directly, so this box-search
        # path has to do the same navigation itself to honor that same contract.
        _safe_goto(page, SHYPPLE_ADMIN_BASE + shipment_path)
        page.wait_for_selector("#containers-link, #show-link, #shipment-documents-link", timeout=8000)
        return {"success": True, "shipment_path": shipment_path, "error": None}
    except Exception as e:
        return {"success": False, "shipment_path": None, "error": str(e)}


def find_shipment_by_sf_number(page, sf_number):
    """Locate a shipment by its SF number for the LCL Arrivals/Release flow. The digits
    after "SF" match Shypple's own internal shipment id (confirmed against a live
    example: "SF169508" <-> /admin/shipments/169508/edit) - try a direct navigation
    first (one request, no search-box interaction), and only fall back to typing the
    raw SF number into #shipment-search (same search box process_one_job uses for
    container numbers) if that doesn't land on a real shipment page - e.g. the
    numbering assumption doesn't hold for some SF numbers. Returns
    {"success": bool, "shipment_path": str or None, "error": str or None}."""
    digits = re.sub(r"[^0-9]", "", sf_number or "")
    if digits:
        shipment_path = f"/admin/shipments/{digits}"
        try:
            _safe_goto(page, SHYPPLE_ADMIN_BASE + shipment_path)
            page.wait_for_selector("#containers-link, #show-link, #shipment-documents-link", timeout=8000)
            return {"success": True, "shipment_path": shipment_path, "error": None}
        except Exception:
            log_system(f"find_shipment_by_sf_number: direct navigation to {shipment_path} did not land "
                       "on a shipment page - falling back to the search box.")

    return _search_shipment_box(page, sf_number, "SF number")


def find_shipment_by_container_number(page, container_number):
    """Locate a shipment by container number - fallback for process_lcl_arrival_job when
    a mail carries no SF number at all (e.g. an Arrival Notice whose subject/body never
    prints one), reusing whatever container_number was already pulled from the attached
    document (extract_lcl_arrival_data / extract_lcl_fields_via_llm in
    operations_api.py's lcl_arrivals_process). Unlike find_shipment_by_sf_number, there
    is no digits-equal-shipment-id shortcut here - a container number's digits have no
    such relationship to Shypple's internal shipment id, so guessing one the same way
    would risk silently landing on a wrong, unrelated shipment. Always goes through the
    search box. Returns {"success": bool, "shipment_path": str or None, "error": str or
    None}."""
    clean = re.sub(r"[\s\-]", "", container_number or "").upper()
    if not clean:
        return {"success": False, "shipment_path": None, "error": "Empty container number."}
    return _search_shipment_box(page, clean, "container")


def click_update_shipment(page):
    """Click "Update Shipment" (#shipment-update-button) on the shipment Edit form -
    the final step of edit_preceding_customs_and_cfs's flow. Same validation-error-scan
    pattern as submit_shipment_document_form, since Shypple's own server-side
    validation can reject an edit silently except for a banner."""
    try:
        btn = page.query_selector("#shipment-update-button")
        if not btn:
            return {"success": False, "error": "Update Shipment button (#shipment-update-button) not found."}
        if not _safe_click(btn, timeout_ms=8000, label="Update Shipment button"):
            return {"success": False, "error": "Update Shipment button click did not complete."}
        page.wait_for_timeout(1500)
        error_text = page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll(
                '.alert, .alert-danger, .alert-error, [role="alert"], .invalid-feedback, .flash, .notice, .toast'
            ));
            for (const el of candidates) {
                if (el.offsetParent === null) continue;
                const text = (el.innerText || el.textContent || '').trim();
                if (text && /can.?t be blank|must be greater than|invalid|error/i.test(text)) {
                    return text;
                }
            }
            return null;
        }""")
        if error_text:
            return {"success": False, "error": error_text[:300]}
        return {"success": True, "error": None}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _select2_open(page, select_id):
    """Open the select2 dropdown wrapping hidden <select id=select_id> - a no-op
    (returns True immediately) if it's already open, since clicking an open multi-select
    widget again would close it instead."""
    try:
        already_open = page.evaluate(
            """(id) => {
                const sel = document.getElementById(id);
                const span = sel ? sel.nextElementSibling : null;
                return !!(span && span.querySelector('.select2-container--open'));
            }""",
            select_id,
        )
        if already_open:
            return True
        handle = page.evaluate_handle(
            """(id) => {
                const sel = document.getElementById(id);
                const span = sel ? sel.nextElementSibling : null;
                return span ? span.querySelector('.select2-selection') : null;
            }""",
            select_id,
        )
        el = handle.as_element()
        return _safe_click(el, label=f"select2 widget #{select_id}")
    except Exception as e:
        log_system(f"_select2_open error for #{select_id}: {e}")
        return False


def _select2_set_native(page, select_id, search_text):
    """Set a select2-wrapped <select>'s value by writing directly to the underlying
    native element's options and firing jQuery 'change' and 'select2:select' events."""
    target = _normalize_type(search_text)
    return page.evaluate(
        """(args) => {
            const sel = document.getElementById(args.id);
            if (!sel) return null;
            const norm = (s) => (s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
            let best = null;
            for (const opt of sel.options) {
                const n = norm(opt.textContent);
                if (n && (n === args.target || n.includes(args.target) || args.target.includes(n))) {
                    best = opt;
                    break;
                }
            }
            if (!best) return null;
            best.selected = true;
            sel.value = best.value;
            if (window.jQuery) {
                window.jQuery(sel).val(best.value).trigger('change').trigger('select2:select');
            } else {
                sel.dispatchEvent(new Event('change', { bubbles: true }));
            }
            return best.textContent.trim();
        }""",
        {"id": select_id, "target": target},
    )


def _select2_clear_native(page, select_id):
    """Deselect every option in a select2-wrapped multi-<select>, writing directly to
    the native element and firing the same events _select2_set_native uses. Needed
    before picking OUR OWN container(s) on the document-upload form: Shypple's own
    "New document" form can arrive with the shipment's real container(s) already
    pre-selected by default. If the document's own extracted container number then
    ISN'T found by _select2_pick below (a genuine mismatch - e.g. the document prints
    a container that isn't this shipment's, or an OCR misread), the pick loop already
    correctly skips adding it, but without this clear step whatever Shypple had
    pre-selected stayed checked regardless - silently tagging the uploaded document
    against container(s) it was never actually confirmed to belong to, instead of
    leaving the field honestly empty so a human reviewing before submit can see
    nothing matched."""
    try:
        page.evaluate(
            """(id) => {
                const sel = document.getElementById(id);
                if (!sel) return;
                for (const opt of sel.options) opt.selected = false;
                if (window.jQuery) {
                    window.jQuery(sel).val(null).trigger('change');
                } else {
                    sel.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }""",
            select_id,
        )
    except Exception as e:
        log_system(f"_select2_clear_native error for #{select_id}: {e}")


_SELECT2_RESULTS_SELECTOR = (
    ".select2-container--open .select2-results__option, .select2-dropdown .select2-results__option"
)


def _select2_pick(page, select_id, search_text):
    """Open the select2 widget for #select_id, type search_text into its search box,
    and click the best (normalized substring) matching visible option. Select2's DOM
    conventions are a stable, well-known third-party library pattern (unlike Gmail's
    obfuscated build), so this is higher-confidence than the Forward/Labels selectors -
    but still worth confirming live. Returns the matched option's text, or None.

    Polls for the match rather than trusting one fixed sleep after typing: Select2
    re-filters its result list on every keystroke (debounced), so a single wait right
    after typing the whole search_text can land before the filter for the LAST
    keystroke has actually settled - especially for longer text like a full document
    type name - which previously made a genuinely-present option (confirmed live, e.g.
    "Arrival Notice") read back as "no matching option found"."""
    if not _select2_open(page, select_id):
        return None
    page.wait_for_timeout(300)

    search_box = page.query_selector(
        ".select2-container--open .select2-search__field, .select2-dropdown .select2-search__field"
    )
    if search_box:
        try:
            search_box.click(timeout=3000)
            search_box.type(search_text, delay=20)
        except Exception as e:
            log_system(f"_select2_pick: search box interaction failed for '{search_text}': {e}")

    target = _normalize_type(search_text)
    best_index, best_text = None, None
    waited_ms = 0
    while best_index is None and waited_ms < 3000:
        page.wait_for_timeout(200)
        waited_ms += 200
        try:
            option_data = page.evaluate(
                f"""() => Array.from(document.querySelectorAll('{_SELECT2_RESULTS_SELECTOR}'))
                    .map((o, i) => ({{ index: i, text: (o.textContent || '').trim() }}))"""
            )
        except Exception as e:
            log_system(f"_select2_pick: could not read options for '{search_text}': {e}")
            break
        for opt in option_data:
            norm = _normalize_type(opt["text"])
            if norm and (norm == target or norm in target or target in norm):
                best_index, best_text = opt["index"], opt["text"]
                break

    if best_index is None:
        page.keyboard.press("Escape")
        return None

    option_handle = page.evaluate_handle(
        f"(idx) => document.querySelectorAll('{_SELECT2_RESULTS_SELECTOR}')[idx]", best_index
    )
    option_el = option_handle.as_element()
    if not _safe_click(option_el, label=f"select2 option '{best_text}'"):
        page.keyboard.press("Escape")
        return None
    page.wait_for_timeout(300)
    return best_text


def fill_shipment_document_form(page, doc_type, file_bytes, file_mime, filename, container_numbers,
                                 customer_name, type_override=None):
    """Fill the "new shipment document" upload form - file, document type, and, per
    the operator's explicit rules, containers (skipped for Arrival notice) and
    organization/customer (skipped for Arrival notice, Delivery order, T1 DOC) -
    WITHOUT submitting. Submission is a separate, explicit step
    (submit_shipment_document_form) so a human can verify what was filled in first.
    Must be called while already on the shipment's Documents tab.

    ``type_override``, if given (the user's choice from the Operations Process review
    UI's dropdown, which is populated from Shypple's OWN real option list - see
    _scrape_document_type_options), is used ONLY for the actual select2 pick below.
    ``doc_type`` (our internal canonical type) still drives every business rule here
    (CONTAINER_SKIP_TYPES/ORG_SKIP_TYPES) and the caller's file lookup/comparison -
    those are keyed on our own classification, not Shypple's label wording."""
    try:
        file_input = page.query_selector("#shipment_document_file")
        if not file_input:
            return {"success": False, "error": "file input (#shipment_document_file) not found"}

        # Gmail's own generic fallback name ("Attachment 1" etc., used when a real
        # filename can't be extracted from the DOM) has no extension - it's truthy, so
        # the old "filename or f'{doc_type}.pdf'" fallback never triggered for it, and
        # Shypple's upload form rejects a nameless/extension-less file server-side
        # ("not allowed to upload \"\" files", plus a "file can't be blank" bundle of
        # errors from the same rejected upload) even though the actual bytes are fine.
        # Always ensure a real extension, derived from the MIME type rather than
        # assuming .pdf, before it ever reaches the file input.
        if not file_bytes or len(file_bytes) == 0:
            return {"success": False, "error": f"File bytes for '{doc_type}' are empty (0 bytes)"}

        # Defense in depth against a real, confirmed bug: an upstream fetch that
        # "succeeds" (non-empty body) but actually returned an HTML/XML error or
        # login/interstitial page instead of the real document (see
        # download_utils.fetch_bytes_robust's own fix for where this came from on
        # the Gmail-attachment side) previously sailed straight through here - the
        # is_html check right below would even happily convert that error page to a
        # PDF and upload IT, "successfully", producing a document that shows a
        # blank/garbage page whenever anyone opens it afterwards. Reject it here
        # too, at the last point before anything is written to Shypple.
        if _looks_like_error_page(file_bytes, file_mime):
            return {
                "success": False,
                "error": f"File bytes for '{doc_type}' look like an HTML/XML error or login page, "
                         "not the real document - refusing to upload it. Try Process/verification again.",
            }

        upload_name = filename or ""
        file_ext = os.path.splitext(upload_name)[1].lower()
        mime_clean = (file_mime or "").split(";")[0].strip().lower()

        # Shypple rejects HTML files ("You are not allowed to upload 'html' files").
        # If the attachment/document is HTML, convert it to a PDF via Playwright before uploading.
        is_html = (
            file_ext in (".html", ".htm") or
            mime_clean in ("text/html", "application/xhtml+xml") or
            (file_bytes[:512].strip().lower().startswith((b"<!doctype html", b"<html")) or b"<head" in file_bytes[:512].lower())
        )

        if is_html:
            try:
                html_str = file_bytes.decode("utf-8", errors="ignore")
                pdf_page = page.context.new_page()
                pdf_page.set_content(html_str)
                pdf_bytes = pdf_page.pdf(format="A4", print_background=True)
                pdf_page.close()
                file_bytes = pdf_bytes
                file_mime = "application/pdf"
                base_name = os.path.splitext(upload_name)[0] or doc_type
                upload_name = f"{base_name}.pdf"
                log_system(f"Converted HTML document '{filename or doc_type}' to PDF for Shypple upload.")
            except Exception as e:
                log_system(f"Failed to convert HTML document to PDF: {e}")
                return {"success": False, "error": f"Shypple does not allow uploading HTML files and PDF conversion failed: {e}"}

        ALLOWED_SHYPPLE_EXTENSIONS = {
            ".bmp", ".csv", ".doc", ".docx", ".eml", ".jpg", ".jpeg", ".gif",
            ".msg", ".pdf", ".png", ".rtf", ".tif", ".tiff", ".txt", ".xlsx",
            ".xls", ".xlsb", ".xlsm", ".xml", ".json"
        }
        current_ext = os.path.splitext(upload_name)[1].lower()
        if not current_ext or current_ext not in ALLOWED_SHYPPLE_EXTENSIONS:
            ext = mimetypes.guess_extension((file_mime or "").split(";")[0].strip()) or ".pdf"
            if ext.lower() not in ALLOWED_SHYPPLE_EXTENSIONS:
                ext = ".pdf"
            base_name = os.path.splitext(upload_name)[0] or doc_type
            upload_name = f"{base_name}{ext}"
            if ext == ".pdf":
                file_mime = "application/pdf"

        file_input.set_input_files({
            "name": upload_name,
            "mimeType": file_mime or "application/pdf",
            "buffer": file_bytes,
        })
        page.evaluate("""() => {
            const fi = document.getElementById('shipment_document_file');
            if (fi) {
                fi.dispatchEvent(new Event('change', { bubbles: true }));
                fi.dispatchEvent(new Event('input', { bubbles: true }));
            }
        }""")
        page.wait_for_timeout(500)

        # Passive capture: the select is guaranteed present/populated right here, on
        # every real upload - cheapest possible way to keep the review UI's dropdown
        # list current without a dedicated live-refresh navigation most of the time.
        _save_document_type_options(_scrape_document_type_options(page))

        matched_type = _select2_set_native(page, "shipment_document_document_type_ids", type_override or doc_type)
        if not matched_type:
            search_text = type_override or doc_type
            return {"success": False, "error": f"No matching document type option found for '{search_text}'"}

        picked_containers = []
        if doc_type not in CONTAINER_SKIP_TYPES:
            # Start from a clean slate - see _select2_clear_native's own docstring for
            # why: Shypple's form can arrive with container(s) already pre-selected by
            # default, and without this, a container we fail to find below (e.g. the
            # document's real container legitimately doesn't match this shipment) left
            # that pre-selection in place instead of the field honestly ending up
            # empty, silently tagging the document against the wrong container(s).
            _select2_clear_native(page, "shipment_document_container_ids")
            for cn in container_numbers or []:
                if _select2_pick(page, "shipment_document_container_ids", cn):
                    picked_containers.append(cn)
                else:
                    log_system(f"fill_shipment_document_form: container '{cn}' not found in the container picker.")
        else:
            log_system(f"fill_shipment_document_form: skipping containers for '{doc_type}' per the Arrival notice rule.")

        picked_org = None
        if doc_type not in ORG_SKIP_TYPES and customer_name:
            picked_org = _select2_pick(page, "shipment_document_organization_ids", customer_name)
            if not picked_org:
                log_system(f"fill_shipment_document_form: organization '{customer_name}' not found in the organization picker.")
        elif doc_type in ORG_SKIP_TYPES:
            log_system(f"fill_shipment_document_form: skipping organization for '{doc_type}' per the operator's rule.")

        return {
            "success": True,
            "matched_type": matched_type,
            "picked_containers": picked_containers,
            "picked_org": picked_org,
            "filename": upload_name,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def submit_shipment_document_form(page):
    """Click "Create Shipment document" - the only thing that actually submits the
    form filled by fill_shipment_document_form. Kept separate so the batch can pause
    for a human confirmation in between."""
    try:
        submit_handle = page.evaluate_handle("""() => {
            const buttons = Array.from(document.querySelectorAll('button[type="submit"]'));
            return buttons.find(b => /create shipment document/i.test((b.textContent || '').trim()));
        }""")
        submit_el = submit_handle.as_element()
        if submit_el is None:
            return {"success": False, "error": "Create Shipment document button not found"}
        if not _safe_click(submit_el, timeout_ms=8000, label="Create Shipment document button"):
            return {"success": False, "error": "Create Shipment document button click did not complete"}
        page.wait_for_timeout(1500)

        # Previously reported success purely from having clicked the button, without
        # checking whether Shypple's own server-side validation actually accepted the
        # submission - a rejected upload (e.g. the extension-less-filename bug this
        # fixed) still gets clicked fine, so the batch logged "Uploaded" while
        # Shypple's page showed a validation error banner the whole time. First-pass,
        # unconfirmed-against-live-DOM selector (same caveat as this file's other
        # guessed selectors) - scans for a visible alert/flash element whose text looks
        # like a validation failure, rather than assuming a specific CSS class.
        error_text = page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll(
                '.alert, .alert-danger, .alert-error, [role="alert"], .invalid-feedback, .flash, .notice, .toast'
            ));
            for (const el of candidates) {
                if (el.offsetParent === null) continue; // skip hidden/template elements
                const text = (el.innerText || el.textContent || '').trim();
                if (text && /not allowed to upload|can.?t be blank|must be greater than|invalid|error/i.test(text)) {
                    return text;
                }
            }
            return null;
        }""")
        if error_text:
            return {"success": False, "error": error_text[:300]}
        return {"success": True, "error": None}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _fetch_email_document(message_id, doc_type, subject="", attachment_index=None, label=None):
    """GET the email's own attachment bytes for doc_type from the Flask backend (which
    owns the classification cache + Gmail access) - returns (bytes, mime, filename, None)
    on success, or (None, None, None, reason) on failure. Same pattern as
    _compare_document_versions_remote above: propagates the *specific* reason (e.g.
    Flask's 404 body explaining exactly which attachment couldn't be found) instead of
    a bare None, which previously left the caller logging an unhelpful generic
    "could not fetch the email's own file" with no way to tell why. subject lets
    Flask's fetch_document_bytes search Gmail for this message if its row has scrolled
    out of the currently-loaded list view since it was first classified. attachment_index
    disambiguates a mail with two or more same-typed attachments (e.g. two "Other"
    documents) - without it doc_type alone always resolves to the first match. ``label``
    is passed through to Flask's /operations/document_file, which forwards it to
    fetch_document_bytes' own force-reclassify retry (defaults to "a-cmr" there if
    omitted) - LCL callers MUST pass label=_LCL_LABEL_KEY, or a retry on an LCL mail
    could corrupt its classification under the CMR Cmr/Other override rule."""
    try:
        params = {"message_id": message_id, "doc_type": doc_type, "subject": subject or ""}
        if attachment_index is not None:
            params["attachment_index"] = attachment_index
        if label:
            params["label"] = label
        url = f"{FLASK_BASE}/api/operations/document_file?{urllib.parse.urlencode(params)}"
        # For a scraped ('pw_') message, Flask's fetch_document_bytes proxies this
        # through open_gmail.py's /get_documents, which can legitimately take up to
        # ~150s for a multi-attachment email (see document_classifier.py's matching
        # 160s urlopen timeout on that call) - this timeout must stay comfortably above
        # that whole chain, or a perfectly successful-but-slow fetch gets reported as
        # "timed out" here well before the real work finishes.
        with urllib.request.urlopen(url, timeout=180) as response:
            data_bytes = response.read()
            mime = response.headers.get("Content-Type", "application/pdf")
            filename = response.headers.get("X-Document-Filename", "")
        if not data_bytes or len(data_bytes) == 0:
            return None, None, None, "Fetched document file is empty (0 bytes)."
        return data_bytes, mime, filename, None
    except urllib.error.HTTPError as e:
        try:
            err_body = json.loads(e.read().decode("utf-8"))
            reason = err_body.get("error") or str(e)
        except Exception:
            reason = str(e)
        log_system(f"_fetch_email_document error for '{doc_type}': {reason}")
        return None, None, None, reason
    except Exception as e:
        log_system(f"_fetch_email_document error for '{doc_type}': {e}")
        return None, None, None, str(e)


def _compare_document_versions_remote(message_id, doc_type, other_bytes, other_mime, subject="", attachment_index=None, label=None):
    """POST to Flask's /operations/compare_document, which fetches the email's own copy
    and runs the Gemini same/different comparison (that logic lives in
    document_classifier.py, not duplicated here). Returns the result dict on success,
    or {"same": None, "reason": <the real error>} on failure - NOT a bare None, which
    previously discarded the actual reason (e.g. Flask's 404 body explaining exactly
    which attachment couldn't be found) and left the caller logging an unhelpful
    generic "no response". attachment_index disambiguates a mail with two or more
    same-typed attachments (e.g. two "Other" documents). label is omitted from the
    request body when None, so Flask's own default ("a-cmr") still applies for CMR's
    unlabeled calls - an LCL caller (verify_existing_document_before_upload) MUST pass
    label=_LCL_LABEL_KEY, same rule as _fetch_email_document's own label param."""
    try:
        body_dict = {
            "message_id": message_id,
            "doc_type": doc_type,
            "subject": subject,
            "attachment_index": attachment_index,
            "other_file_base64": base64.b64encode(other_bytes).decode("ascii"),
            "other_mime": other_mime or "application/pdf",
        }
        if label:
            body_dict["label"] = label
        body = json.dumps(body_dict).encode("utf-8")
        req = urllib.request.Request(
            f"{FLASK_BASE}/api/operations/compare_document", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        # Same reasoning as _fetch_email_document's timeout above: this endpoint fetches
        # the email's own copy (which can take up to ~150s for a scraped multi-attachment
        # message) THEN runs the Gemini comparison on top of that - 60s was cut off well
        # before either step could realistically finish.
        with urllib.request.urlopen(req, timeout=200) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success"):
            return result
        log_system(f"_compare_document_versions_remote: Flask reported failure for '{doc_type}': {result.get('error')}")
        return {"same": None, "reason": result.get("error") or "comparison request failed"}
    except urllib.error.HTTPError as e:
        try:
            err_body = json.loads(e.read().decode("utf-8"))
            reason = err_body.get("error") or str(e)
        except Exception:
            reason = str(e)
        log_system(f"_compare_document_versions_remote error for '{doc_type}': {reason}")
        return {"same": None, "reason": reason}
    except Exception as e:
        log_system(f"_compare_document_versions_remote error for '{doc_type}': {e}")
        return {"same": None, "reason": str(e)}


def _extract_lcl_fields_remote(data_bytes, mime, filename=""):
    """POST to Flask's /operations/extract_lcl_fields_from_bytes, which runs the
    regex+LLM field-extraction pipeline directly against a document's bytes (that
    logic lives in document_classifier.py, not duplicated here) - same
    request/response shape as _compare_document_versions_remote above, but for a
    document with no message_id involved (e.g. one just downloaded from Shypple's own
    Documents tab - see the Delivery Order -> Arrival Notice cross-verification flow
    in handle_delivery_order). Returns the extracted dict
    ({container_number/devanning_date/customs_number/cfs_address}) on success, or {}
    on any failure - never raises, so a Flask/network hiccup here degrades to "could
    not extract" (manual verification) rather than crashing the job."""
    try:
        body = json.dumps({
            "file_base64": base64.b64encode(data_bytes).decode("ascii"),
            "mime": mime or "application/pdf",
            "filename": filename,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{FLASK_BASE}/api/operations/extract_lcl_fields_from_bytes", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=200) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success"):
            return result.get("extracted") or {}
        log_system(f"_extract_lcl_fields_remote: Flask reported failure: {result.get('error')}")
        return {}
    except Exception as e:
        log_system(f"_extract_lcl_fields_remote error: {e}")
        return {}


def _search_bl_number_remote(bl_number):
    """Ask the Gmail automation to search the lcl-arrivals---release label for
    bl_number (see scripts/open_gmail.py's new /search_label action, backed by
    search_lcl_label) - used by the Delivery Order -> Arrival Notice
    cross-verification flow (handle_delivery_order) when no Arrival Notice document
    is found on Shypple yet. Returns the list of matching mails (same per-mail shape
    the normal LCL scrape already produces - id/subject/snippet/attachmentNames/
    hasAttachment/...) or [] on any failure - never raises, so a Gmail-automation
    hiccup here just means "no Arrival Notice mail found," falling through to
    continuing the Delivery Order unchanged rather than blocking the job."""
    try:
        params = urllib.parse.urlencode({"label": _LCL_LABEL_KEY, "query": bl_number})
        url = f"{GMAIL_CONTROL_SERVER}/search_label?{params}"
        with urllib.request.urlopen(url, timeout=25) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success"):
            return result.get("results") or []
        log_system(f"_search_bl_number_remote: Gmail automation reported failure: {result.get('error')}")
        return []
    except Exception as e:
        log_system(f"_search_bl_number_remote error: {e}")
        return []


def _star_source_email(job, color):
    """Ask the Gmail automation to mark this job's source email with a colored star -
    "blue" fires when a matched shipment has no organization (independent of the
    forward confirmation gate below), "purple" fires when the shipment's status is
    cancelled/deleted. Always records job["star_status"] ("done"/"failed") and
    job["star_color"] so the dashboard can show whether it actually landed and which
    color, not just log it."""
    message_id = job.get("message_id") or ""
    if not message_id.startswith("pw_"):
        log_job(job, "Skipped starring: this email isn't from the delegated mailbox the "
                     "Gmail automation can act on.")
        with STATE_LOCK:
            job["star_status"] = "failed"
            job["star_color"] = color
            job["star_error"] = "Not a delegated-mailbox email."
        return
    raw_id = message_id[len("pw_"):]
    try:
        params = urllib.parse.urlencode({"id": raw_id, "color": color})
        url = f"{GMAIL_CONTROL_SERVER}/star_color?{params}"
        # Must exceed open_gmail.py's own /star_color wait (widened to 100s - the star
        # click loop's real worst case, including retries on "element not attached to
        # the DOM", can run close to ~90s; a shorter client timeout here would abort
        # the connection and report a spurious failure while that server-side wait was
        # still genuinely in progress - reproduced live at 70.5s with the old timeouts).
        with urllib.request.urlopen(url, timeout=105) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success"):
            log_job(job, f"Marked with a {color} star.")
            with STATE_LOCK:
                job["star_status"] = "done"
                job["star_color"] = color
        else:
            log_job(job, f"Could not set the {color} star: {result.get('error')}")
            with STATE_LOCK:
                job["star_status"] = "failed"
                job["star_color"] = color
                job["star_error"] = result.get("error")
    except Exception as e:
        log_job(job, f"Could not reach the Gmail automation to set the star: {e}")
        with STATE_LOCK:
            job["star_status"] = "failed"
            job["star_color"] = color
            job["star_error"] = str(e)


def _mark_source_email_unread(job):
    """Ask the Gmail automation to mark this job's source email UNREAD - used for
    error/needs-manual-attention outcomes (e.g. no document found, no date found,
    shipment fails verification) in BOTH the CMR and LCL/FCL Arrivals/Release
    pipelines, so the email stays visibly flagged - unread + yellow star (alongside
    the star _star_source_email already sets) - until someone looks at it. A
    successful terminal outcome (document uploaded, or already up to date) uses
    _mark_source_email_read instead - see that function. Records
    job["read_status"] ("done"/"failed") and job["read_action"] = "unread" so the
    dashboard can show the right wording."""
    message_id = job.get("message_id") or ""
    with STATE_LOCK:
        job["read_action"] = "unread"
    if not message_id.startswith("pw_"):
        log_job(job, "Skipped marking unread: this email isn't from the delegated mailbox the "
                     "Gmail automation can act on.")
        with STATE_LOCK:
            job["read_status"] = "failed"
            job["read_error"] = "Not a delegated-mailbox email."
        return
    raw_id = message_id[len("pw_"):]
    try:
        # subject lets the control server's row-not-found recovery search for this
        # mail instead of giving up outright - without it, a row that scrolled out of
        # the currently-rendered list (e.g. after _star_source_email's up-to-~70s
        # click-cycling loop runs first, during which the list can reorder/refresh)
        # can never be found, and this call fails even though the mail is still
        # right there in the label, just not in the current DOM snapshot.
        params = urllib.parse.urlencode({"id": raw_id, "type": "mark_unread", "subject": job.get("subject", "")})
        url = f"{GMAIL_CONTROL_SERVER}/action?{params}"
        # Must exceed the control server's own 40s wait (see /action's handler) - the
        # subject-search fallback can legitimately take close to that, and a shorter
        # client timeout would abort the request (reporting a spurious failure here)
        # while the server-side click/search was still genuinely in progress.
        with urllib.request.urlopen(url, timeout=45) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success"):
            log_job(job, "Marked the source email as unread.")
            with STATE_LOCK:
                job["read_status"] = "done"
        else:
            log_job(job, f"Could not mark the source email as unread: {result.get('error')}")
            with STATE_LOCK:
                job["read_status"] = "failed"
                job["read_error"] = result.get("error")
    except Exception as e:
        log_job(job, f"Could not reach the Gmail automation to mark the email unread: {e}")
        with STATE_LOCK:
            job["read_status"] = "failed"
            job["read_error"] = str(e)


def _mark_source_email_read(job):
    """Ask the Gmail automation to mark this job's source email READ - used whenever
    processing finishes successfully, whether or not a document was actually
    (re-)uploaded (already-identical/up-to-date counts too), for BOTH the CMR and
    LCL/FCL Arrivals/Release pipelines, per the operator's explicit rule. An outcome
    that needs manual attention (nothing usable found, verification failed, etc.)
    uses _mark_source_email_unread instead - see that function. Records
    job["read_status"] ("done"/"failed") and job["read_action"] = "read" so the
    dashboard can show the right wording."""
    message_id = job.get("message_id") or ""
    with STATE_LOCK:
        job["read_action"] = "read"
    if not message_id.startswith("pw_"):
        log_job(job, "Skipped marking read: this email isn't from the delegated mailbox the Gmail automation can act on.")
        with STATE_LOCK:
            job["read_status"] = "failed"
            job["read_error"] = "Not a delegated-mailbox email."
        return
    raw_id = message_id[len("pw_"):]
    try:
        # subject lets the control server's row-not-found recovery search for this
        # mail instead of giving up outright - see the matching comment in
        # _mark_source_email_unread above for why this matters (the row can scroll
        # out of the currently-rendered list during _star_source_email's slow
        # click-cycling loop, which always runs right before this call).
        params = urllib.parse.urlencode({"id": raw_id, "type": "mark_read", "subject": job.get("subject", "")})
        url = f"{GMAIL_CONTROL_SERVER}/action?{params}"
        # Must exceed the control server's own 40s wait - see the matching comment in
        # _mark_source_email_unread above.
        with urllib.request.urlopen(url, timeout=45) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success"):
            log_job(job, "Marked the source email as read.")
            with STATE_LOCK:
                job["read_status"] = "done"
        else:
            log_job(job, f"Could not mark the source email as read: {result.get('error')}")
            with STATE_LOCK:
                job["read_status"] = "failed"
                job["read_error"] = result.get("error")
    except Exception as e:
        log_job(job, f"Could not reach the Gmail automation to mark the email read: {e}")
        with STATE_LOCK:
            job["read_status"] = "failed"
            job["read_error"] = str(e)


def verify_existing_document_before_upload(page, job, doc_type, mail_bytes, mail_mime, containers, attachment_index=None):
    """Scrape #shipment-document-table on Shypple's Documents tab before uploading.
    If a document matching doc_type AND container number is already uploaded:
      1. Download the existing document from Shypple by clicking its download link.
      2. Save a local copy in the system Downloads folder.
      3. Extract/compare content from BOTH documents (mail copy vs Shypple copy).
      4. Returns dict:
         {"exists": True/False, "same": True/False, "differences": [...], "reason": str}

    LCL-only (both current callers - handle_arrival_notice/handle_delivery_order -
    process LCL mail): passes label=_LCL_LABEL_KEY to _compare_document_versions_remote
    so the email-side fetch it triggers resolves under the LCL label - required, or a
    retry on this mail could corrupt its classification under the CMR Cmr/Other
    override rule (see _fetch_email_document's docstring for the same rule)."""
    open_documents_tab(page)
    doc_rows = scrape_document_rows(page)
    row = _find_uploaded_row_for_type(doc_type, doc_rows, email_containers=containers)
    if not row or not row.get("downloadHref"):
        return {"exists": False, "same": False, "differences": [], "reason": "not uploaded yet"}

    log_job(job, f"Found existing '{doc_type}' on Documents tab ({row.get('filename')}). Downloading to compare...")
    shypple_bytes, shypple_mime = download_shypple_document_bytes(page, row["downloadHref"])
    if shypple_bytes is None:
        log_job(job, f"Could not download existing '{doc_type}' from Shypple to compare.")
        return {"exists": True, "same": False, "differences": [], "reason": "could not download existing copy"}

    try:
        saved_filename = save_document_locally(shypple_bytes, row.get("filename") or f"{doc_type}.pdf")
        log_job(job, f"Saved local copy of existing '{doc_type}' ({saved_filename}) to system Downloads folder.")
        with STATE_LOCK:
            job.setdefault("downloaded_files", []).append({"type": doc_type, "filename": saved_filename, "source": "shypple"})
    except Exception as e:
        log_job(job, f"Could not save local copy of existing '{doc_type}': {e}")

    verdict = _compare_document_versions_remote(
        job["message_id"], doc_type, shypple_bytes, shypple_mime, subject=job.get("subject", ""),
        attachment_index=attachment_index, label=_LCL_LABEL_KEY,
    )
    if verdict and verdict.get("same") is True:
        log_job(job, f"Existing '{doc_type}' on Shypple is IDENTICAL to the email's document.")
        return {"exists": True, "same": True, "differences": [], "reason": "both documents are same"}

    differences = verdict.get("differences") if verdict else []
    reason = verdict.get("reason") if verdict else "content differs"
    log_job(job, f"Existing '{doc_type}' on Shypple DIFFERS from email's document: {reason}")
    return {"exists": True, "same": False, "differences": differences, "reason": reason}


def _record_my_jewellery_flag(job):
    """Append a note for a "My Jewellery" customer shipment to
    data/lcl_my_jewellery_mails.json - per the operator's explicit rule, this halts
    further automation for the mail (no star/read-state change), just leaves a record
    for manual follow-up. Mirrors open_gmail.py's forwarded-mail tracker load/save
    pattern (data/forwarded_mails.json)."""
    entry = {
        "message_id": job.get("message_id"),
        "subject": job.get("subject", ""),
        "sf_number": job.get("sf_number", ""),
        "mail_date": job.get("date", ""),
        "flagged_at": datetime.now(timezone.utc).isoformat(),
    }
    with _MY_JEWELLERY_TRACKER_LOCK:
        entries = []
        if os.path.exists(_MY_JEWELLERY_TRACKER_PATH):
            try:
                with open(_MY_JEWELLERY_TRACKER_PATH, "r", encoding="utf-8") as f:
                    entries = json.load(f)
            except Exception:
                entries = []
        entries.append(entry)
        os.makedirs(os.path.dirname(_MY_JEWELLERY_TRACKER_PATH), exist_ok=True)
        tmp = _MY_JEWELLERY_TRACKER_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2)
        os.replace(tmp, _MY_JEWELLERY_TRACKER_PATH)
    log_job(job, "Customer is 'My Jewellery' - recorded and halted (no Shypple automation for this mail).")


def _forward_and_relabel_source_email(job):
    """Ask the (separate) Gmail automation to forward this job's source email to
    FORWARD_NO_ORG_TO and move it to FORWARD_NO_ORG_LABEL. Not currently called from
    process_one_job's empty-organization path anymore (that now switches to
    FRESH_ORG_NAME and retries instead of forwarding away - see
    awaiting_org_switch_confirmation) - kept here as a standalone capability in case
    another flow needs the same forward-and-relabel action. Only works for emails from
    the delegated mailbox that automation drives (scraped 'pw_' ids);
    anything else is skipped with a note. Always records job["forward_status"]
    ("sent"/"sent_not_labeled"/"already_forwarded"/"failed") so the dashboard can show
    whether the email actually went out and whether the a-release-orders label was
    confirmed applied to the forwarded copy - "already_forwarded" means a previous run
    already sent this exact message (see open_gmail.py's forward-tracker), so no
    duplicate was sent; "sent_not_labeled" means the forward went out but the label
    could not be confirmed there (the dashboard's "Label pending release-orders"
    button retries these)."""
    message_id = job.get("message_id") or ""
    if not message_id.startswith("pw_"):
        log_job(job, "Skipped forward: this email isn't from the delegated mailbox the "
                     "Gmail automation can act on.")
        with STATE_LOCK:
            job["forward_status"] = "failed"
            job["forward_error"] = "Not a delegated-mailbox email."
        return
    raw_id = message_id[len("pw_"):]
    try:
        params = urllib.parse.urlencode({
            "id": raw_id, "to": FORWARD_NO_ORG_TO, "label": FORWARD_NO_ORG_LABEL,
        })
        url = f"{GMAIL_CONTROL_SERVER}/forward_and_relabel?{params}"
        with urllib.request.urlopen(url, timeout=40) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("skipped"):
            log_job(job, f"Already forwarded to {FORWARD_NO_ORG_TO} in a previous run - not sending a duplicate.")
            with STATE_LOCK:
                job["forward_status"] = "already_forwarded"
        elif result.get("success"):
            if result.get("relabeled"):
                log_job(job, f"Forwarded to {FORWARD_NO_ORG_TO} and labeled {FORWARD_NO_ORG_LABEL} there.")
                with STATE_LOCK:
                    job["forward_status"] = "sent"
            else:
                relabel_error = result.get("relabel_error") or "unknown reason"
                log_job(job, f"Forwarded to {FORWARD_NO_ORG_TO}, but could NOT confirm the "
                              f"{FORWARD_NO_ORG_LABEL} label was applied to the forwarded copy there "
                              f"({relabel_error}) - use 'Label pending release-orders' to retry.")
                with STATE_LOCK:
                    job["forward_status"] = "sent_not_labeled"
                    job["relabel_error"] = relabel_error
        else:
            log_job(job, f"Forward/relabel did not complete: {result.get('error')}")
            with STATE_LOCK:
                job["forward_status"] = "failed"
                job["forward_error"] = result.get("error")
    except Exception as e:
        log_job(job, f"Could not reach the Gmail automation to forward this email: {e}")
        with STATE_LOCK:
            job["forward_status"] = "failed"
            job["forward_error"] = str(e)


# ---------------------------------------------------------------------------------
# LCL Arrivals / Release pipeline - a second, parallel job flow through this same
# batch/confirmation-gate machinery (see process_lcl_arrival_job and run_batch's
# dispatch below). Jobs for this flow carry job["flow"] == "lcl_arrivals" and a
# job["mail_type"] of "shipment_not_released" (resolved entirely in
# app/routes/api/operations_api.py before this ever runs), "delay_or_devanning",
# "arrival_notice", or "delivery_order", plus job["sf_number"] and
# job["extracted"] (container_number/devanning_date/customs_number/cfs_address, from
# extract_lcl_arrival_data). Every outcome here leaves the source mail UNREAD +
# yellow-starred - same terminal rule the CMR pipeline uses (see
# _mark_source_email_unread) - so a successfully-processed email stays visible in an
# unread+yellow-star sweep for both pipelines alike.
# ---------------------------------------------------------------------------------

def handle_delay_or_devanning(page, job):
    extracted = job.get("extracted") or {}
    extracted_date = extracted.get("devanning_date")

    set_job_status(job, "processing", phase="Reading current devanning date on Shypple")
    current = check_container_tab_data(page)
    with STATE_LOCK:
        job["shypple_devanning_date"] = current.get("devanning_date") or ""
        job["extracted_devanning_date"] = extracted_date or ""

    if not extracted_date:
        log_job(job, "There is no date in the mail and the document for delay/devanning - marking unread + "
                     "yellow star anyway so it can be reviewed manually.")
        _star_source_email(job, "yellow")
        _mark_source_email_unread(job)
        set_job_status(job, "lcl_no_date_found", reason="There is no date in the mail and the document for delay and devanning.")
        return

    if (current.get("devanning_date") or "") == extracted_date:
        log_job(job, f"Devanning date already matches Shypple ({extracted_date}) - no change needed.")
    else:
        _open_confirmation_gate()
        set_job_status(
            job, "awaiting_lcl_date_confirmation",
            phase="Waiting for confirmation to update the devanning date",
            date_preview={"current": current.get("devanning_date") or "(empty)", "extracted": extracted_date},
        )
        log_job(job, f"Devanning date differs - Shypple has '{current.get('devanning_date') or '(empty)'}', "
                     f"email says '{extracted_date}'. Waiting for confirmation before updating.")
        if not _wait_for_confirmation(job):
            set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
            log_job(job, "Skipped by operator - date left unchanged.")
            return
        set_job_status(job, "processing", phase="Updating devanning date")
        save_result = add_container_and_devanning_date(page, None, extracted_date)
        if not save_result.get("success"):
            set_job_status(job, "error", error=f"Could not save devanning date: {save_result.get('error')}")
            log_job(job, f"Save changes (Containers tab) failed: {save_result.get('error')}")
            return
        log_job(job, f"Updated devanning date to {extracted_date} and saved.")

    _star_source_email(job, "yellow")
    _mark_source_email_read(job)
    set_job_status(job, "lcl_done", phase="Done - devanning date verified/updated")


def handle_arrival_notice(page, job):
    extracted = job.get("extracted") or {}
    container_number = extracted.get("container_number")
    devanning_date = extracted.get("devanning_date")
    customs_number = extracted.get("customs_number")
    cfs_address = extracted.get("cfs_address")

    set_job_status(job, "processing", phase="Filling container number and devanning date")
    # save=False: only fill the fields here, so the confirmation banner below shows
    # the operator what's ABOUT to be saved before anything actually gets written -
    # the real "Save changes" click happens after they confirm, matching this whole
    # pipeline's manual-confirmation-gates-before-anything-is-written design.
    add_container_and_devanning_date(page, container_number, devanning_date, save=False)

    _open_confirmation_gate()
    set_job_status(
        job, "awaiting_lcl_container_confirmation",
        phase="Waiting for confirmation - container & devanning date filled",
        container_preview={"container_number": container_number, "devanning_date": devanning_date},
    )
    log_job(job, f"Filled container '{container_number}' / devanning date '{devanning_date}' on the "
                 "Containers tab. Waiting for confirmation before saving and editing customs/CFS.")
    if not _wait_for_confirmation(job):
        set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
        log_job(job, "Skipped by operator - left as-is, nothing edited.")
        return

    set_job_status(job, "processing", phase="Saving container number / devanning date")
    save_result = click_save_containers_changes(page)
    if not save_result.get("success"):
        set_job_status(job, "error", error=f"Could not save container/devanning date: {save_result.get('error')}")
        log_job(job, f"Save changes (Containers tab) failed: {save_result.get('error')}")
        return
    log_job(job, "Container number / devanning date saved on the Containers tab.")

    set_job_status(job, "processing", phase="Editing customs number / CFS and updating shipment")
    cfs_result = edit_preceding_customs_and_cfs(page, customs_number, cfs_address)
    if cfs_address and not cfs_result.get("cfs_matched"):
        with STATE_LOCK:
            job["cfs_match_warning"] = (
                f"Could not confidently match Discharge CFS option for '{cfs_address}' - left unchanged, needs manual review."
            )
        log_job(job, job["cfs_match_warning"])
    update_result = click_update_shipment(page)
    if not update_result.get("success"):
        set_job_status(job, "error", error=f"Could not update shipment: {update_result.get('error')}")
        log_job(job, f"Update Shipment failed: {update_result.get('error')}")
        return
    log_job(job, "Shipment updated (customs number / CFS saved).")

    set_job_status(job, "processing", phase="Preparing Arrival notice document upload")
    file_bytes, file_mime, filename, fetch_error = _fetch_email_document(
        job["message_id"], "Arrival notice", subject=job.get("subject", ""), label=_LCL_LABEL_KEY
    )
    if file_bytes is None:
        log_job(job, f"Could not fetch the Arrival notice attachment ({fetch_error}) - marking unread + "
                     "yellow star anyway so it isn't lost, but this needs a manual look.")
        _star_source_email(job, "yellow")
        _mark_source_email_unread(job)
        set_job_status(job, "lcl_no_document_found",
                       reason=f"Could not fetch the Arrival notice attachment: {fetch_error}")
        return

    _save_mail_document_locally(job, "Arrival notice", file_bytes, filename)

    if not _ensure_org_before_documents_tab(page, job):
        return

    check = verify_existing_document_before_upload(page, job, "Arrival notice", file_bytes, file_mime, [container_number] if container_number else [])
    if check.get("exists") and check.get("same"):
        _star_source_email(job, "yellow")
        _mark_source_email_read(job)
        set_job_status(job, "lcl_done", phase="Done - Arrival notice already uploaded and identical (no upload needed)")
        log_job(job, "Arrival notice already present on Shypple and content is identical. Marked email read + yellow star.")
        return

    if check.get("exists") and not check.get("same"):
        # An Arrival notice IS already on Shypple but its content differs from the
        # email's copy - show the operator exactly what differs (field-by-field, from
        # verify_existing_document_before_upload's Gemini compare) and wait for an
        # explicit decision before (re-)uploading, per the operator's explicit
        # request. Falls through into the normal fill/submit flow below once
        # confirmed - same as the "not uploaded yet" case.
        _open_confirmation_gate()
        set_job_status(
            job, "awaiting_lcl_document_diff_confirmation",
            phase="Existing 'Arrival notice' on Shypple differs from the email's document - waiting for confirmation",
            document_diff_preview={
                "doc_type": "Arrival notice", "reason": check.get("reason"),
                "differences": check.get("differences") or [],
            },
        )
        log_job(job, f"'Arrival notice' already on Shypple but DIFFERS from the email's document: "
                     f"{check.get('reason')}. Waiting for confirmation before uploading.")
        if not _wait_for_confirmation(job):
            set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
            log_job(job, "Skipped by operator - Arrival notice was NOT uploaded.")
            return

    open_documents_tab(page)
    fill_result = fill_shipment_document_form(page, "Arrival notice", file_bytes, file_mime, filename, [], None)
    if not fill_result.get("success"):
        set_job_status(job, "error", error=f"Could not prepare the Arrival notice upload: {fill_result.get('error')}")
        log_job(job, f"Failed to prepare the Arrival notice upload: {fill_result.get('error')}")
        return

    _open_confirmation_gate()
    set_job_status(
        job, "awaiting_lcl_submit_confirmation",
        phase="Waiting for confirmation to submit the Arrival notice document",
        submit_preview={"filename": fill_result.get("filename"), "matched_type": fill_result.get("matched_type")},
    )
    log_job(job, f"Form filled for Arrival notice (file: {filename}). Waiting for confirmation before submitting.")
    if not _wait_for_confirmation(job):
        set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
        log_job(job, "Skipped by operator - Arrival notice was NOT submitted.")
        return

    set_job_status(job, "processing", phase="Submitting Arrival notice document")
    submit_result = submit_shipment_document_form(page)
    if not submit_result.get("success"):
        set_job_status(job, "upload_failed", error=submit_result.get("error"))
        log_job(job, f"Failed to submit Arrival notice: {submit_result.get('error')}")
        return

    log_job(job, "Arrival notice uploaded successfully.")
    # Per the operator's explicit rule, a completed upload marks the mail READ (not
    # unread) - matches the CMR pipeline's _verify_and_upload_documents and
    # handle_delivery_order's primary upload below.
    _star_source_email(job, "yellow")
    _mark_source_email_read(job)
    set_job_status(job, "lcl_done", phase="Done - Arrival notice processed")


def _values_match_customs(extracted, current):
    """Normalized equality for Customs Number - both sides are plain text, so a
    trim+uppercase+strip-punctuation compare is enough (unlike Discharge CFS below,
    neither side is a dropdown label needing prefix matching)."""
    if not extracted or not current:
        return False
    norm = lambda s: re.sub(r"[^A-Z0-9]", "", s.upper())
    return norm(extracted) == norm(current)


def _extract_bl_number(text):
    """Local copy of document_classifier.extract_bl_number's regex - this script runs
    as a separate subprocess with no import path to app.* (see _extract_lcl_fields_remote/
    _compare_document_versions_remote above for how everything ELSE that needs
    document_classifier.py's logic goes over HTTP instead), but a BL number is a pure
    regex over subject text already sitting in job["subject"] - not worth a Flask
    round-trip for. Keep this pattern in sync with the real definition if it ever
    changes there."""
    if not text:
        return None
    m = re.search(r"\bB\s*/\s*L\s*[:\s]\s*([A-Z0-9][A-Z0-9\-]{3,25})\b", text, re.IGNORECASE)
    return m.group(1).upper() if m else None


def _values_match_cfs(extracted, current):
    """Judge whether the Edit tab's CURRENT Discharge CFS dropdown label already
    corresponds to what the Arrival Notice document says. A strict string compare
    would false-flag every match: the Edit tab holds a dropdown LABEL (e.g. "CTG
    Logistics B.V."), the document gives free text (e.g. "VLS BELGIUM" or "CTG").

    Root cause of a real bug: this used to truncate `extracted` to just its first
    3 letters before comparing. Two different real Discharge CFS options can share
    the same first 3 letters (confirmed: "CTG Export" and "CTG Logistics B.V.") -
    so whenever the CURRENT (wrong) dropdown value happened to share a 3-letter
    prefix with the document's ACTUAL warehouse, this returned a false "already
    matches", and the caller (_verify_existing_arrival_notice) skipped correcting
    it - the document plainly named the right warehouse, but Shypple kept showing
    a different one, silently, forever. Fixed by trying the longest possible
    prefix of `extracted` first (most specific - least likely to collide with an
    unrelated option) and only falling back to shorter prefixes, down to a 3-char
    floor, if the full text doesn't match - mirroring the longest-prefix-wins
    logic edit_preceding_customs_and_cfs itself uses to SELECT the option. Any
    extracted text containing "CTG" (bare word or a full free-text name like "CTG
    Cargo Terminal Gadering BV") is treated the same confirmed way: it always means
    CTG Logistics, never CTG Export - mirroring edit_preceding_customs_and_cfs's own
    widened rule."""
    if not extracted or not current:
        return False
    norm = lambda s: re.sub(r"\s+", " ", s).strip().upper()
    extracted_n, current_n = norm(extracted), norm(current)
    if "CTG" in extracted_n:
        return "ctg logistics" in current.lower()
    for length in range(len(extracted_n), 2, -1):
        if current_n.startswith(extracted_n[:length]):
            return True
    return False


def _reupload_arrival_notice(page, job, file_bytes, file_mime, filename):
    """Operator selected "No" during Arrival Notice verification (see
    _verify_existing_arrival_notice) - verification is not accepted until the
    Arrival Notice document is (re-)uploaded to the Documents tab, through the same
    upload+confirm flow handle_arrival_notice already uses for a fresh Arrival
    Notice. Returns True/False as handle_delivery_order expects (continue / stop -
    job status is already set on failure)."""
    if file_bytes is None:
        set_job_status(job, "error", error="Could not download the existing Arrival Notice from Shypple to re-upload it.")
        return False
    if not _ensure_org_before_documents_tab(page, job):
        return False
    open_documents_tab(page)
    fill_result = fill_shipment_document_form(page, "Arrival notice", file_bytes, file_mime, filename, [], None)
    if not fill_result.get("success"):
        set_job_status(job, "error", error=f"Could not prepare Arrival Notice re-upload: {fill_result.get('error')}")
        return False

    _open_confirmation_gate()
    set_job_status(
        job, "awaiting_lcl_submit_confirmation",
        phase="Waiting for confirmation to submit the Arrival Notice document (re-verification)",
        submit_preview={"filename": fill_result.get("filename"), "matched_type": fill_result.get("matched_type")},
    )
    log_job(job, "Re-uploading the Arrival Notice document for verification. Waiting for confirmation.")
    if not _wait_for_confirmation(job):
        set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
        log_job(job, "Skipped by operator - Arrival Notice re-upload was NOT submitted.")
        return False

    set_job_status(job, "processing", phase="Submitting Arrival Notice document")
    submit_result = submit_shipment_document_form(page)
    if not submit_result.get("success"):
        set_job_status(job, "upload_failed", error=submit_result.get("error"))
        log_job(job, f"Failed to submit Arrival Notice re-upload: {submit_result.get('error')}")
        return False

    with STATE_LOCK:
        job["arrival_notice_verified"] = True
    log_job(job, "Arrival Notice document uploaded - verification complete.")
    return True


def _verify_existing_arrival_notice(page, job, arrival_row):
    """An Arrival Notice document already exists on the shipment's Documents tab
    (arrival_row, from _find_uploaded_row_for_type) - download it, extract Customs
    Number / Discharge CFS from its content, compare against the Edit tab's CURRENT
    values, and open a Yes/No confirmation gate for the operator (see
    awaiting_arrival_notice_verification in dashboard.html). Yes corrects any
    mismatched-and-extractable field from the Arrival Notice (source of truth); No
    treats verification as incomplete and re-uploads the Arrival Notice document
    instead (_reupload_arrival_notice). Returns True/False as handle_delivery_order
    expects."""
    set_job_status(job, "processing", phase="Downloading existing Arrival Notice for verification")
    download_href = arrival_row.get("downloadHref")
    if download_href:
        file_bytes, file_mime = download_shypple_document_bytes(page, download_href)
    else:
        log_job(job, "Arrival Notice row on the Documents tab has no download link - "
                     "cannot verify its content, falling back to manual verification.")
        file_bytes, file_mime = None, None

    if file_bytes is not None:
        # Per the operator's explicit request: the Arrival Notice pulled off
        # Shypple's Documents tab for this verification must land in the system's
        # real Downloads folder too, same as every other document this pipeline
        # touches (save_document_locally already does this for the CMR deep-compare
        # step; _save_mail_document_locally does it for a document fetched from the
        # email) - previously this was the one document read+used by the pipeline
        # that never got a local copy saved at all.
        try:
            saved_filename = save_document_locally(file_bytes, arrival_row.get("filename") or "Arrival Notice.pdf")
            log_job(job, f"Saved a local copy of the existing Arrival Notice ({saved_filename}) to Downloads.")
            with STATE_LOCK:
                job.setdefault("downloaded_files", []).append(
                    {"type": "Arrival notice", "filename": saved_filename, "source": "shypple"}
                )
        except Exception as e:
            log_job(job, f"Could not save a local copy of the existing Arrival Notice: {e}")

    extracted = _extract_lcl_fields_remote(file_bytes, file_mime, arrival_row.get("filename") or "") if file_bytes else {}

    set_job_status(job, "processing", phase="Reading current Edit tab data")
    current = check_edit_tab_customs_and_cfs(page)
    # check_edit_tab_customs_and_cfs navigated to the shipment's /edit page to read
    # these fields (no form submit) - return to the main shipment page before the
    # gate below, so anything after a Yes/No decision (the correction write, or the
    # re-upload's own Documents-tab open) starts from a known page state.
    if job.get("shipment_path"):
        _safe_goto(page, SHYPPLE_ADMIN_BASE + job["shipment_path"])

    customs_match = _values_match_customs(extracted.get("customs_number"), current.get("customs_number"))
    cfs_match = _values_match_cfs(extracted.get("cfs_address"), current.get("cfs_address"))
    needs_manual = not extracted.get("customs_number") and not extracted.get("cfs_address")

    check_result = {
        "filename": arrival_row.get("filename"),
        "extracted": extracted,
        "current_edit_tab": current,
        "customs_match": customs_match,
        "cfs_match": cfs_match,
        "needs_manual_verification": needs_manual,
    }
    with STATE_LOCK:
        job["arrival_notice_check"] = check_result

    # If Edit tab data and Arrival Notice document data are identical:
    if customs_match and cfs_match and not needs_manual:
        log_job(job, "Edit tab data and Arrival Notice document data are identical. No update needed.")
        with STATE_LOCK:
            job["arrival_notice_verified"] = True
        return True

    _open_confirmation_gate()
    set_job_status(job, "awaiting_arrival_notice_verification",
                    phase="Waiting for confirmation - verify Arrival Notice data against Edit tab",
                    arrival_notice_check=check_result)
    log_job(job, f"Arrival Notice found on Documents tab ('{arrival_row.get('filename')}') - "
                 f"customs_match={customs_match}, cfs_match={cfs_match}, needs_manual={needs_manual}. "
                 "Waiting for confirmation.")

    if not _wait_for_confirmation(job):
        log_job(job, "Operator selected No - Arrival Notice not verified.")
        set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
        return False

    # Guarantee Arrival Notice PDF is saved locally to system Downloads folder
    if file_bytes is None and download_href:
        file_bytes, file_mime = download_shypple_document_bytes(page, download_href)
        if file_bytes is not None:
            try:
                saved_filename = save_document_locally(file_bytes, arrival_row.get("filename") or "Arrival Notice.pdf")
                log_job(job, f"Saved a local copy of the existing Arrival Notice ({saved_filename}) to Downloads.")
                with STATE_LOCK:
                    job.setdefault("downloaded_files", []).append(
                        {"type": "Arrival notice", "filename": saved_filename, "source": "shypple"}
                    )
            except Exception as e:
                log_job(job, f"Could not save a local copy of the existing Arrival Notice: {e}")

    fixes = {}
    if extracted.get("customs_number") and not customs_match:
        fixes["customs_number"] = extracted["customs_number"]
    if extracted.get("cfs_address") and not cfs_match:
        fixes["cfs_address"] = extracted["cfs_address"]
    if fixes:
        set_job_status(job, "processing", phase="Correcting Edit tab from Arrival Notice data")
        cfs_result = edit_preceding_customs_and_cfs(page, fixes.get("customs_number"), fixes.get("cfs_address"))
        if fixes.get("cfs_address") and not cfs_result.get("cfs_matched"):
            # Don't let this look like a clean correction: the Arrival Notice
            # genuinely said a different Discharge CFS, but no dropdown option
            # could be confidently matched to it, so the Edit tab's CFS was left
            # UNCHANGED (not corrected, and not corrected-to-something-wrong
            # either - see edit_preceding_customs_and_cfs) - the operator still
            # needs to pick it manually.
            with STATE_LOCK:
                job["cfs_match_warning"] = (
                    f"Could not confidently match Discharge CFS option for "
                    f"'{fixes['cfs_address']}' (from the Arrival Notice) - left unchanged, needs manual review."
                )
            log_job(job, job["cfs_match_warning"])
        update_result = click_update_shipment(page)
        if not update_result.get("success"):
            set_job_status(job, "error", error=f"Could not save Arrival Notice correction: {update_result.get('error')}")
            return False
        log_job(job, f"Corrected Edit tab from Arrival Notice data: {fixes}")
        if job.get("shipment_path"):
            _safe_goto(page, SHYPPLE_ADMIN_BASE + job["shipment_path"])

    with STATE_LOCK:
        job["arrival_notice_verified"] = True
    return True


def _process_discovered_arrival_notice(page, job, found_mail):
    """Build a synthetic job for an Arrival Notice mail discovered via BL-number
    search (see verify_or_fetch_arrival_notice), insert it into the batch as its OWN
    visible job right after the current one (so its confirmation gates render
    normally in the dashboard, exactly like any queued job - see run_batch's
    _inline_handled guard and dashboard.html's __activeBatchMessageIds fix), process
    it end-to-end via the existing handle_arrival_notice UNCHANGED, then return to
    this Delivery Order's own shipment page. Returns True/False as
    handle_delivery_order expects - a skipped/failed sub-job does not abort the
    parent Delivery Order (logged, not blocking), matching this pipeline's
    never-block-unnecessarily philosophy."""
    extracted = _extract_lcl_fields_remote(found_mail["bytes"], found_mail["mime"], found_mail.get("filename") or "")
    sub_job = {
        "message_id": found_mail["id"],
        "subject": found_mail["subject"],
        "mail_type": "arrival_notice",
        "flow": "lcl_arrivals",
        "extracted": extracted,
        "status": "queued",
        "log": [],
        "parent_message_id": job.get("message_id"),
        "_inline_handled": True,
    }
    with STATE_LOCK:
        try:
            idx = batch_state["jobs"].index(job)
        except ValueError:
            idx = len(batch_state["jobs"]) - 1
        batch_state["jobs"].insert(idx + 1, sub_job)

    log_job(job, f"Found Arrival Notice mail '{found_mail['subject']}' via BL-number search - "
                 "processing it first (see the job listed right after this one).")

    handle_arrival_notice(page, sub_job)

    if sub_job.get("status") != "lcl_done":
        log_job(job, f"Arrival Notice sub-process did not complete (status='{sub_job.get('status')}') - "
                     "continuing this Delivery Order anyway, but customs/CFS data may still be unverified.")
    else:
        log_job(job, "Arrival Notice sub-process completed - returning to this Delivery Order.")

    if job.get("shipment_path"):
        _safe_goto(page, SHYPPLE_ADMIN_BASE + job["shipment_path"])
    return True


def verify_or_fetch_arrival_notice(page, job):
    """Step run at start of handle_delivery_order: make sure this shipment's Arrival
    Notice data (Customs Number, Discharge CFS) is verified/correct against Edit tab
    before a Delivery Order goes out."""
    set_job_status(job, "processing", phase="Checking Documents tab for an existing Arrival Notice")
    if not _ensure_org_before_documents_tab(page, job):
        return False
    open_documents_tab(page)
    doc_rows = scrape_document_rows(page)
    arrival_row = _find_uploaded_row_for_type("Arrival notice", doc_rows)

    if arrival_row:
        return _verify_existing_arrival_notice(page, job, arrival_row)

    bl_number = _extract_bl_number(job.get("subject", ""))
    if not bl_number:
        log_job(job, "No Arrival Notice on Shypple yet, and no BL number could be extracted "
                     "from this Delivery Order mail's subject - continuing without verification.")
        return True

    log_job(job, f"No Arrival Notice on Shypple yet - searching the LCL inbox for BL number '{bl_number}'.")
    matches = _search_bl_number_remote(bl_number)
    found_mail = None
    for m in matches:
        if not m.get("hasAttachment"):
            continue
        test_bytes, test_mime, test_filename, _fetch_error = _fetch_email_document(
            m["id"], "Arrival notice", subject=m.get("subject", ""), label=_LCL_LABEL_KEY,
        )
        if test_bytes is not None:
            found_mail = {
                "id": m["id"], "subject": m.get("subject", ""),
                "bytes": test_bytes, "mime": test_mime, "filename": test_filename,
            }
            break

    if not found_mail:
        log_job(job, f"No Arrival Notice mail found for BL number '{bl_number}' either - "
                     "continuing Delivery Order without verification.")
        return True

    return _process_discovered_arrival_notice(page, job, found_mail)


def handle_delivery_order(page, job):
    # 1. Extract data from Edit tab and Arrival Notice document, verify and update if needed
    if not verify_or_fetch_arrival_notice(page, job):
        return

    # 2. Check if Delivery order is ALREADY uploaded on Shypple Documents tab
    set_job_status(job, "processing", phase="Checking Documents tab for existing Delivery order document")
    if not _ensure_org_before_documents_tab(page, job):
        return
    open_documents_tab(page)
    doc_rows = scrape_document_rows(page)
    existing_do = _find_uploaded_row_for_type("Delivery order", doc_rows)

    if existing_do:
        _star_source_email(job, "yellow")
        _mark_source_email_read(job)
        set_job_status(
            job, "lcl_done",
            phase="The document was already uploaded, don't need to upload",
            reason="The document was already uploaded, don't need to upload",
        )
        log_job(job, f"Delivery order document ('{existing_do.get('filename')}') was already uploaded on Shypple. No need to upload. Marked email read + yellow star.")
        return

    # 3. Containers tab check
    set_job_status(job, "processing", phase="Checking Containers tab")
    current = check_container_tab_data(page)
    with STATE_LOCK:
        job["container_tab_check"] = current

    if not (current.get("has_container") and current.get("has_devanning_date")):
        set_job_status(
            job, "lcl_incomplete_container",
            reason="Container tab is missing the container number and/or devanning date - "
                   "process this shipment's Arrival Notice mail first.",
        )
        log_job(job, "Container tab is incomplete - not uploading the Delivery order yet.")
        return

    # 4. Fetch email attachment for Delivery order
    set_job_status(job, "processing", phase="Preparing Delivery order document upload")
    file_bytes, file_mime, filename, fetch_error = _fetch_email_document(
        job["message_id"], "Delivery order", subject=job.get("subject", ""), label=_LCL_LABEL_KEY
    )
    if file_bytes is None:
        log_job(job, f"Could not fetch the Delivery order attachment ({fetch_error}) - marking unread + "
                     "yellow star anyway so it isn't lost, but this needs a manual look.")
        _star_source_email(job, "yellow")
        _mark_source_email_unread(job)
        set_job_status(job, "lcl_no_document_found",
                       reason=f"Could not fetch the Delivery order attachment: {fetch_error}")
        return

    _save_mail_document_locally(job, "Delivery order", file_bytes, filename)

    container_number = current.get("container_number")
    if not _ensure_org_before_documents_tab(page, job):
        return

    # 5. Fill and submit Delivery order document form directly (single manual verification rule)
    open_documents_tab(page)
    fill_result = fill_shipment_document_form(
        page, "Delivery order", file_bytes, file_mime, filename,
        [container_number] if container_number else [], None,
    )
    if not fill_result.get("success"):
        set_job_status(job, "error", error=f"Could not prepare the Delivery order upload: {fill_result.get('error')}")
        log_job(job, f"Failed to prepare the Delivery order upload: {fill_result.get('error')}")
        return

    set_job_status(job, "processing", phase="Submitting Delivery order document")
    submit_result = submit_shipment_document_form(page)
    if not submit_result.get("success"):
        set_job_status(job, "upload_failed", error=submit_result.get("error"))
        log_job(job, f"Failed to submit Delivery order: {submit_result.get('error')}")
        return

    log_job(job, "Delivery order uploaded successfully.")

    extra_indices = job.get("extra_doc_attachment_indices") or []
    for extra_i, attachment_index in enumerate(extra_indices, 1):
        total_extra = len(extra_indices)
        set_job_status(job, "processing",
                       phase=f"Preparing additional Delivery order document {extra_i}/{total_extra}")
        extra_bytes, extra_mime, extra_filename, extra_fetch_error = _fetch_email_document(
            job["message_id"], "Delivery order", subject=job.get("subject", ""),
            attachment_index=attachment_index, label=_LCL_LABEL_KEY,
        )
        if extra_bytes is None:
            log_job(job, f"Could not fetch additional Delivery order document {extra_i}/{total_extra} "
                         f"(attachment_index={attachment_index}): {extra_fetch_error} - skipping it.")
            continue

        _save_mail_document_locally(job, "Delivery order", extra_bytes, extra_filename)

        if not _ensure_org_before_documents_tab(page, job):
            log_job(job, f"Skipping additional Delivery order document {extra_i}/{total_extra} - "
                         "could not verify/switch the Shypple organization.")
            continue
        open_documents_tab(page)
        extra_fill_result = fill_shipment_document_form(
            page, "Delivery order", extra_bytes, extra_mime, extra_filename,
            [container_number] if container_number else [], None,
        )
        if not extra_fill_result.get("success"):
            log_job(job, f"Could not prepare additional Delivery order document {extra_i}/{total_extra} "
                         f"upload: {extra_fill_result.get('error')} - skipping it.")
            continue

        set_job_status(job, "processing", phase=f"Submitting additional Delivery order document {extra_i}/{total_extra}")
        extra_submit_result = submit_shipment_document_form(page)
        if not extra_submit_result.get("success"):
            log_job(job, f"Failed to submit additional Delivery order document {extra_i}/{total_extra}: {extra_submit_result.get('error')}")
            continue

        log_job(job, f"Additional Delivery order document {extra_i}/{total_extra} uploaded successfully.")

    _mark_source_email_read(job)
    _star_source_email(job, "yellow")
    set_job_status(job, "lcl_done", phase="Done - Delivery order uploaded successfully")

    # Per the operator's explicit rule: this mail can carry a SECOND real document
    # that also resolves to "Delivery order" on its own merit (e.g. a carrier's own
    # "Release" PDF alongside the main Delivery order PDF - see
    # classify_documents_lcl's per-attachment classification and lcl_arrivals_process's
    # find_all_document_sources call). Field extraction above was driven entirely by
    # the FIRST such document; each additional one is uploaded here AS-IS - no
    # extraction re-run on it, just its own fill/confirm/submit pass, reusing the same
    # container number already confirmed above. A failure or operator skip on one of
    # these logs and moves on rather than aborting the whole job - the primary
    # document (which carries the actual shipment data) already succeeded above.
    extra_indices = job.get("extra_doc_attachment_indices") or []
    for extra_i, attachment_index in enumerate(extra_indices, 1):
        total_extra = len(extra_indices)
        set_job_status(job, "processing",
                       phase=f"Preparing additional Delivery order document {extra_i}/{total_extra}")
        extra_bytes, extra_mime, extra_filename, extra_fetch_error = _fetch_email_document(
            job["message_id"], "Delivery order", subject=job.get("subject", ""),
            attachment_index=attachment_index, label=_LCL_LABEL_KEY,
        )
        if extra_bytes is None:
            log_job(job, f"Could not fetch additional Delivery order document {extra_i}/{total_extra} "
                         f"(attachment_index={attachment_index}): {extra_fetch_error} - skipping it.")
            continue

        _save_mail_document_locally(job, "Delivery order", extra_bytes, extra_filename)

        # Defensive re-navigation: fill_shipment_document_form requires being on the
        # Documents tab, and the previous submit's post-submit page state isn't
        # guaranteed (a redirect elsewhere would silently break this fill otherwise).
        if not _ensure_org_before_documents_tab(page, job):
            log_job(job, f"Skipping additional Delivery order document {extra_i}/{total_extra} - "
                         "could not verify/switch the Shypple organization.")
            continue
        open_documents_tab(page)
        extra_fill_result = fill_shipment_document_form(
            page, "Delivery order", extra_bytes, extra_mime, extra_filename,
            [container_number] if container_number else [], None,
        )
        if not extra_fill_result.get("success"):
            log_job(job, f"Could not prepare additional Delivery order document {extra_i}/{total_extra} "
                         f"upload: {extra_fill_result.get('error')} - skipping it.")
            continue

        _open_confirmation_gate()
        set_job_status(
            job, "awaiting_lcl_submit_confirmation",
            phase=f"Waiting for confirmation to submit additional Delivery order document {extra_i}/{total_extra}",
            submit_preview={
                "filename": extra_fill_result.get("filename"), "matched_type": extra_fill_result.get("matched_type"),
                "containers": extra_fill_result.get("picked_containers"),
            },
        )
        log_job(job, f"Form filled for additional Delivery order document {extra_i}/{total_extra} "
                     f"(file: {extra_filename}). Waiting for confirmation before submitting.")
        if not _wait_for_confirmation(job):
            log_job(job, f"Skipped by operator - additional Delivery order document {extra_i}/{total_extra} "
                         "was NOT submitted.")
            continue

        set_job_status(job, "processing",
                       phase=f"Submitting additional Delivery order document {extra_i}/{total_extra}")
        extra_submit_result = submit_shipment_document_form(page)
        if not extra_submit_result.get("success"):
            log_job(job, f"Failed to submit additional Delivery order document {extra_i}/{total_extra}: "
                         f"{extra_submit_result.get('error')}")
            continue
        log_job(job, f"Additional Delivery order document {extra_i}/{total_extra} uploaded successfully.")

    # The primary Delivery order document was already uploaded earlier in this function
    # (see the _mark_source_email_read call right after it succeeds, above) - per the
    # operator's explicit rule, a completed upload marks the mail READ.
    _star_source_email(job, "yellow")
    _mark_source_email_read(job)
    set_job_status(job, "lcl_done", phase="Done - Delivery order processed")


def process_lcl_arrival_job(page, job):
    sf_number = job.get("sf_number")
    mail_type = job.get("mail_type")
    container_number = (job.get("extracted") or {}).get("container_number")
    if not sf_number and not container_number:
        set_job_status(job, "error", error="No SF number or container number was extracted from this email.")
        log_job(job, "Skipped - no SF number or container number extracted from this email.")
        return

    if sf_number:
        set_job_status(job, "processing", phase="Finding shipment by SF number")
        found = find_shipment_by_sf_number(page, sf_number)
        search_desc = f"SF number '{sf_number}'"
    else:
        # No SF number printed anywhere on this mail - fall back to the container
        # number already extracted from the attached document rather than giving up.
        set_job_status(job, "processing", phase="Finding shipment by container number")
        log_job(job, f"No SF number on this email - falling back to container number '{container_number}'.")
        found = find_shipment_by_container_number(page, container_number)
        search_desc = f"container '{container_number}'"
    if not found.get("success"):
        set_job_status(job, "no_match", reason=found.get("error") or "Shipment not found.")
        log_job(job, f"Could not find a shipment for {search_desc}: {found.get('error')}")
        return
    with STATE_LOCK:
        job["shipment_path"] = found["shipment_path"]
    log_job(job, f"Found shipment at {found['shipment_path']}.")

    set_job_status(job, "processing", phase="Verifying cluster / load type / customer")
    info = verify_info_tab(page)
    with STATE_LOCK:
        job["cluster"] = info.get("clusterText", "")
        job["load_type"] = info.get("loadType", "")
        job["customer_name"] = info.get("customerName", "")

    if info.get("isCluster3") or not info.get("isLcl"):
        reasons = []
        if info.get("isCluster3"):
            reasons.append("cluster is 3")
        if not info.get("isLcl"):
            reasons.append(f"load type is '{info.get('loadType') or 'unknown'}', not LCL")
        reason_str = " and ".join(reasons)
        log_job(job, f"Shipment fails verification ({reason_str}) - marking unread + yellow star, no further action.")
        _star_source_email(job, "yellow")
        _mark_source_email_unread(job)
        set_job_status(job, "skipped_cluster_or_fcl", reason=reason_str)
        return

    if (info.get("customerName") or "").strip().lower() == "my jewellery":
        _record_my_jewellery_flag(job)
        set_job_status(job, "flagged_my_jewellery", reason="Customer is My Jewellery - flagged for manual handling.")
        return

    if mail_type == "delay_or_devanning":
        handle_delay_or_devanning(page, job)
    elif mail_type == "arrival_notice":
        handle_arrival_notice(page, job)
    elif mail_type == "delivery_order":
        handle_delivery_order(page, job)
    else:
        set_job_status(
            job, "error",
            error=f"Unknown mail type '{mail_type}' - use the review panel's type override and reprocess.",
        )


def process_one_job(page, job):
    containers = job.get("containers") or []
    # Captured ONCE, before either SF-number fallback below can reassign `containers`
    # to the shipment's own (possibly multi-container) list - `containers` from here
    # on is purely "what do we search Shypple with to find the right shipment", which
    # a fallback is free to replace. This variable stays the DOCUMENT's own container(s)
    # as extracted, used later for which container the upload/verification actually
    # tags the document against. Conflating the two previously meant: a document whose
    # own printed container (e.g. "MOAU1439260") has no direct Shypple record -> SF
    # number finds the real shipment -> that shipment's OWN unrelated containers (e.g.
    # two other containers on the same multi-container shipment) silently replaced the
    # document's real container everywhere downstream, including which container(s)
    # got ticked on the actual document upload form - tagging the document against
    # containers it has nothing to do with, not the one it was actually printed for.
    extracted_containers = list(containers)
    if not containers:
        # No container number could be extracted from the email/document at all - very
        # plausible for a CMR that's a scanned, HANDWRITTEN form (hard for OCR/the LLM
        # to read reliably, see _extract_containers_via_llm's own docstring). Per the
        # operator's explicit rule: fall back to finding the shipment by its SF number
        # instead - a machine-printed reference in the subject line, far more reliable
        # than a handwritten container number - then read the REAL container number(s)
        # straight off THAT shipment's own Containers tab (Shypple's own data entry
        # there, not an OCR/LLM read of a scan). Once populated this way, `containers`
        # just flows into the exact same search-by-container loop below as if it had
        # been extracted from the email originally - same org-check/upload flow,
        # unchanged - which also naturally re-derives the organization/ETA from the
        # search-results row rather than needing a special case for it here.
        sf_number = job.get("sf_number")
        if not sf_number:
            set_job_status(job, "no_match", reason="No container numbers were extracted from this email, and no SF number was found either.")
            log_job(job, "Skipped - no container numbers or SF number extracted from this email.")
            return

        set_job_status(job, "processing", phase=f"No container in email - finding shipment by SF number {sf_number}")
        log_job(job, f"No container number found in the email - looking up Shypple by SF number '{sf_number}' instead.")
        found = find_shipment_by_sf_number(page, sf_number)
        if not found.get("success"):
            set_job_status(job, "no_match", reason=f"No container number in the email, and SF number '{sf_number}' lookup failed: {found.get('error')}")
            log_job(job, f"Could not find a shipment for SF number '{sf_number}': {found.get('error')}")
            return

        set_job_status(job, "processing", phase=f"Reading containers off shipment {found['shipment_path']}")
        page.goto(SHYPPLE_ADMIN_BASE + found["shipment_path"])
        page.wait_for_timeout(1000)
        open_containers_tab(page)
        labels = page.eval_on_selector_all(".containers-list a", "els => els.map(e => e.textContent.trim())")
        found_containers = sorted(set(filter(None, (extract_container_from_label(l) for l in labels))))
        if not found_containers:
            set_job_status(job, "no_match", reason=f"Found shipment via SF number '{sf_number}' but its Containers tab is empty.")
            log_job(job, f"Shipment for SF number '{sf_number}' has no containers listed on its Containers tab - nothing to verify against.")
            return

        containers = found_containers
        with STATE_LOCK:
            job["containers"] = containers
        log_job(job, f"Read container number(s) from Shypple's Containers tab via SF number '{sf_number}': {containers}.")

    set_job_status(job, "processing", phase="Searching containers")
    current_year = datetime.now().year

    def _search_containers(container_list):
        """One pass of the search-box loop below, factored out so it can be re-run with
        a different container list (see the SF-number retry right after the first call)
        without duplicating the loop body."""
        match, tried, diagnostics = None, [], []
        any_candidates_seen = False
        cancelled_or_deleted_seen = False
        for raw_container in container_list:
            clean = re.sub(r"[\s\-]", "", raw_container).upper()
            tried.append(clean)
            log_job(job, f"Searching Shypple for container {clean}...")
            page.goto(f"{SHYPPLE_ADMIN_BASE}/admin/shipments")
            page.wait_for_selector("#shipment-search", timeout=15000)
            page.fill("#shipment-search", clean)
            page.press("#shipment-search", "Enter")
            page.wait_for_selector(".table-responsive", timeout=15000)
            page.wait_for_timeout(500)
            result = find_matching_shipment(page, current_year)
            if result.get("matched"):
                match = result["chosen"]
                job["matched_container"] = clean
                if result.get("ambiguous"):
                    log_job(job, f"Multiple current-year results for {clean} - picked "
                                  f"organization '{match.get('org') or '(none)'}'.")
                break
            candidates = result.get("candidates", [])
            if candidates:
                any_candidates_seen = True
            for candidate in candidates:
                diagnostics.append(f"{clean} -> {_describe_candidate_issue(candidate, current_year)}")
                if candidate.get("cancelledOrDeleted"):
                    cancelled_or_deleted_seen = True
        return match, tried, diagnostics, any_candidates_seen, cancelled_or_deleted_seen

    match, tried, diagnostics, any_candidates_seen, cancelled_or_deleted_seen = _search_containers(containers)

    # Per the operator's explicit rule: when the DOCUMENT's own extracted container
    # number comes back with a genuinely empty results table, that's treated as a real
    # "no record" outcome - go straight to the no-record/purple-star flow below, do NOT
    # fall back to the mail's SF number (or any subject-extracted container) to go find
    # a DIFFERENT shipment. A previous version of this function retried via SF number
    # here, reasoning a misread was as likely as a genuine no-record case - but a real
    # example proved that reasoning wrong: the document's container was read correctly
    # (confirmed against the actual CMR image), yet the SF number in the subject
    # pointed to a completely unrelated shipment (different sender, different cargo) -
    # the email's subject/SF-reference simply didn't match its own attached document.
    # Falling back there would have offered to upload this document against a shipment
    # it has nothing to do with. See the empty-containers branch above (no container
    # extracted from the email AT ALL) for the one case that still legitimately uses
    # the SF number - that's a different situation (nothing to search with in the
    # first place), not this one (something was extracted and searched, and genuinely
    # isn't there).
    if not match:
        reason = "; ".join(diagnostics) if diagnostics else "no search results at all"
        if cancelled_or_deleted_seen:
            # Per the operator's explicit rule: a cancelled/deleted shipment status
            # never gets a document uploaded - just a purple star so it surfaces in the
            # existing "Process purple-starred" -> "_0 India shipments" sweep. Final
            # status set BEFORE the star (same reasoning as the org-switch gate above -
            # the star click can take up to ~70s in the worst case) so the dashboard
            # shows this job as done right away instead of looking stuck mid-"processing".
            log_job(job, "Shypple shows this shipment's status as cancelled/deleted for one or "
                          f"more of the extracted container(s) - not uploading. {reason}")
            set_job_status(job, "cancelled_or_deleted", tried_containers=tried, reason=reason)
            _star_source_email(job, "purple")
            return
        if not any_candidates_seen:
            _open_confirmation_gate()
            set_job_status(
                job, "awaiting_no_record_confirmation",
                phase="Waiting for confirmation - no record found on Shypple",
                tried_containers=tried, reason=reason,
            )
            log_job(job, "Every container search came back with a genuinely empty results table "
                          "(not just an org/ETA mismatch). Waiting for confirmation before marking "
                          "unread + purple star.")
            if not _wait_for_confirmation(job):
                set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
                log_job(job, "Skipped by operator - left as-is.")
                return
            set_job_status(job, "processing", phase="Marking unread...")
            # Per the operator's explicit rule: no label move for this outcome - just
            # unread + purple star, same mechanism every other outcome in this file
            # uses (no separate "flag and relabel" endpoint, which was also a real
            # source of failure on its own - Gmail's Labels button isn't always
            # findable the way this needed it to be).
            _mark_source_email_unread(job)
            # Final status set before the star, same reasoning as the cancelled/deleted
            # branch above - _star_source_email's row-list retry can take up to ~70s in
            # the worst case, and the dashboard should show this job as done right away
            # instead of looking stuck mid-"processing" for that whole window.
            set_job_status(job, "no_match", tried_containers=tried, reason=reason)
            _star_source_email(job, "purple")
        else:
            set_job_status(job, "no_match", tried_containers=tried, reason=reason)
            log_job(job, f"No matching shipment found after trying: {', '.join(tried)}. {reason}")
        return

    org_label = match.get("org") or "(no organization set)"
    log_job(job, f"Matched shipment - organization: {org_label}, ETA: {match.get('eta') or 'n/a'}.")
    if not match.get("org"):
        log_job(job, f"Organization is empty under '{TARGET_ORG_NAME}' - some shipments only "
                      f"exist under '{FRESH_ORG_NAME}' instead.")
        # Set the awaiting-confirmation status BEFORE the blue star, not after - the
        # star click (_star_source_email below) retries up to 12 times against a Gmail
        # row list that a separate background scrape re-renders every ~2s, and can
        # legitimately take up to ~70s in the worst case (see _star_source_email's own
        # comment). That's fine for the star itself, but it must never delay the
        # confirmation banner - an operator waiting to click "Confirm switch" doesn't
        # care about a decorative star and shouldn't have to wait up to a minute just to
        # see the banner appear. _open_confirmation_gate() (called first, below) is
        # what actually guarantees a click landing during that ~70s window isn't lost -
        # see its own docstring for the bug this fixed (a confirm click used to need a
        # second click to register whenever it landed while the star was still retrying).
        #
        # The star itself runs on a background thread, NOT inline before
        # _wait_for_confirmation() - a previous version called it inline here ("best-
        # effort visual marker - never blocks the gate above"), but that comment was
        # only true for the banner becoming VISIBLE, not for the confirm click actually
        # being ACTED on: _wait_for_confirmation() only runs after this call returns, so
        # a click landing (and setting proceed_event) anywhere during that up-to-~70s
        # window was correctly retained, but this thread couldn't reach the .wait() that
        # picks it up - and therefore couldn't proceed with the actual org switch -
        # until the star finished. That's what looked like the "Confirm switch" button
        # doing nothing for up to a minute, with an operator re-clicking it (harmlessly,
        # but pointlessly) in the meantime. Backgrounding it means this thread reaches
        # _wait_for_confirmation() immediately, so a confirm click is acted on the
        # instant it lands.
        _open_confirmation_gate()
        set_job_status(job, "awaiting_org_switch_confirmation",
                        phase="Waiting for confirmation to switch organization",
                        tried_containers=tried, target_org=FRESH_ORG_NAME)
        log_job(job, f"Waiting for confirmation before switching Shypple to '{FRESH_ORG_NAME}' and "
                      f"re-searching container(s) ({', '.join(tried)}) there.")
        threading.Thread(target=_star_source_email, args=(job, "blue"), daemon=True).start()
        if not _wait_for_confirmation(job):
            set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
            log_job(job, "Skipped by operator - left as-is, organization not switched.")
            return

        set_job_status(job, "processing", phase=f"Switching organization to '{FRESH_ORG_NAME}'...")
        try:
            switch_to_fresh_org(page)
        except Exception as e:
            set_job_status(job, "error", error=f"Could not switch organization to '{FRESH_ORG_NAME}': {e}")
            log_job(job, f"Failed to switch organization: {e}")
            return
        job["organization_switched_to"] = FRESH_ORG_NAME

        set_job_status(job, "processing", phase=f"Re-searching containers under '{FRESH_ORG_NAME}'")
        fresh_match = None
        for raw_container in containers:
            clean = re.sub(r"[\s\-]", "", raw_container).upper()
            log_job(job, f"Re-searching Shypple (as '{FRESH_ORG_NAME}') for container {clean}...")
            page.goto(f"{SHYPPLE_ADMIN_BASE}/admin/shipments")
            page.wait_for_selector("#shipment-search", timeout=15000)
            page.fill("#shipment-search", clean)
            page.press("#shipment-search", "Enter")
            page.wait_for_selector(".table-responsive", timeout=15000)
            page.wait_for_timeout(500)
            result = find_matching_shipment(page, current_year)
            if result.get("matched"):
                fresh_match = result["chosen"]
                job["matched_container"] = clean
                break

        if not fresh_match:
            log_job(job, f"Still no matching shipment found under '{FRESH_ORG_NAME}'.")
            set_job_status(job, "no_match", tried_containers=tried,
                            reason=f"No match under '{TARGET_ORG_NAME}' or '{FRESH_ORG_NAME}'.")
            # Switch back before returning - the NEXT job in this batch must not
            # inherit this job's Fresh B.V. detour.
            try:
                ensure_org_is_shypple_bv(page)
            except Exception as e:
                log_job(job, f"Could not switch organization back to '{TARGET_ORG_NAME}': {e}")
            return

        match = fresh_match
        log_job(job, f"Matched shipment under '{FRESH_ORG_NAME}' - organization: "
                      f"{match.get('org') or '(still none)'}, ETA: {match.get('eta') or 'n/a'}.")

    # Use the DOCUMENT's own extracted container(s) for verification/upload-tagging,
    # not whatever `containers` ended up holding after a possible SF-number fallback
    # (that fallback exists purely to locate the right SHIPMENT, via a machine-printed
    # SF reference, when the document's own container had no direct match - the
    # shipment's other containers it reads off the Containers tab in that process are
    # not necessarily what THIS document is actually for). Only when nothing was ever
    # extracted from the document/email in the first place (extracted_containers is
    # empty - the mail never had a container number to begin with) is there nothing
    # document-specific to fall back to, so the shipment-derived list is used instead.
    verify_containers = extracted_containers if extracted_containers else containers

    # try/finally, not a plain sequential call: an unhandled exception out of
    # _verify_and_upload_documents (e.g. a Playwright timeout mid-upload) must still
    # switch the org back before propagating - otherwise the NEXT job in this batch
    # would silently inherit this job's Fresh B.V. detour instead of a bug here just
    # failing this one job, which is far worse (documents attached to the wrong org).
    try:
        _verify_and_upload_documents(page, job, match, verify_containers)
    finally:
        if job.get("organization_switched_to"):
            try:
                ensure_org_is_shypple_bv(page)
                log_job(job, f"Switched Shypple back to '{TARGET_ORG_NAME}' for the next email.")
            except Exception as e:
                log_job(job, f"Could not switch organization back to '{TARGET_ORG_NAME}' after this job: {e}")


def _verify_and_upload_documents(page, job, match, containers):
    """Open the matched shipment and run the container/document verification + upload
    flow. Extracted out of process_one_job so it can be reused for either the normal
    match (organization present under TARGET_ORG_NAME) or the one found after
    switching to FRESH_ORG_NAME, without duplicating this ~200-line flow - and so
    process_one_job can unconditionally switch the organization back afterward
    regardless of which of this function's several early returns fired."""
    set_job_status(job, "processing", phase="Opening shipment",
                   organization=match.get("org") or "", shipment_path=match["path"])
    page.goto(SHYPPLE_ADMIN_BASE + match["path"])
    page.wait_for_timeout(1000)

    set_job_status(job, "processing", phase="Verifying containers")
    open_containers_tab(page)
    labels = page.eval_on_selector_all(".containers-list a", "els => els.map(e => e.textContent.trim())")
    shipment_containers = set(filter(None, (extract_container_from_label(l) for l in labels)))
    email_containers = set(re.sub(r"[\s\-]", "", c).upper() for c in containers)
    missing_containers = sorted(email_containers - shipment_containers)
    matched_containers = sorted(email_containers & shipment_containers)
    with STATE_LOCK:
        job["shipment_containers"] = sorted(shipment_containers)
        job["matched_containers"] = matched_containers
        job["missing_containers"] = missing_containers
    if missing_containers:
        log_job(job, f"Container(s) from the email NOT found on the shipment: {missing_containers}")
    else:
        log_job(job, "All container numbers from the email are present on the shipment.")

    set_job_status(job, "processing", phase="Verifying documents")
    if not _ensure_org_before_documents_tab(page, job, expected_org=job.get("organization_switched_to")):
        return
    open_documents_tab(page)
    doc_rows = scrape_document_rows(page)
    uploaded_types_raw = [t for row in doc_rows for t in (row.get("types") or [])]
    # One entry per ATTACHMENT, not deduped by type - an a-cmr mail can carry two or
    # more non-Cmr attachments, all typed "Other" by classify_documents_cmr, and each
    # one is a real file that needs its own upload (previously "Other" was excluded
    # here entirely, so any attachment beyond the primary Cmr one was silently never
    # uploaded). attachment_index (parallel list from operations_api's
    # extract_operations) disambiguates same-typed attachments from each other.
    raw_types = job.get("doc_types") or []
    raw_indices = job.get("doc_attachment_indices") or []
    raw_filenames = job.get("doc_filenames") or []
    extracted = [
        {
            "type": t,
            "attachment_index": raw_indices[i] if i < len(raw_indices) else None,
            "mail_filename": raw_filenames[i] if i < len(raw_filenames) else "",
        }
        for i, t in enumerate(raw_types) if t != "No DOC"
    ]

    # For each extracted document: if it's genuinely missing, it needs uploading. If a
    # same-typed document IS already there, don't just trust the type label - download
    # it and deep-compare against the email's own version (dates/parties/amounts)
    # before deciding it's really the same document, per the operator's explicit
    # request: only skip the upload when the two are confirmed EXACTLY the same: any
    # real difference, however small, routes to needs_upload with the specific
    # field-by-field differences attached so the confirmation banner can show them,
    # and a human must approve before it's actually uploaded. The same applies when
    # verification simply couldn't happen (no download link, fetch failure,
    # inconclusive Gemini verdict) - that is NOT treated as "probably fine" anymore;
    # not being able to verify is itself a reason to ask a human, not skip one.
    #
    # One exception, and only one: when the SAME document type is already on Shypple
    # under the EXACT same filename as the email's own attachment, that's a confirmed
    # identical document by name alone - skip straight past the download + content
    # compare below. The moment the filename differs (even with the type matching),
    # that's exactly the case the operator wants a real verification for - so it falls
    # straight through into the existing content-compare path, same as before.
    needs_upload = []  # [{"type": t, "attachment_index": i, "reason": str, "differences": [...] (optional)}]
    claimed_rows = set()  # id(row) already matched to an earlier same-typed candidate
    for entry in extracted:
        t = entry["type"]
        idx = entry["attachment_index"]
        mail_filename = entry.get("mail_filename") or ""
        row = _find_uploaded_row_for_type(t, doc_rows, exclude=claimed_rows, email_containers=containers)
        if row is None:
            needs_upload.append({"type": t, "attachment_index": idx, "reason": "not uploaded yet"})
            continue
        claimed_rows.add(id(row))

        shypple_filename = row.get("filename") or ""
        if mail_filename and shypple_filename and _normalize_doc_filename(mail_filename) == _normalize_doc_filename(shypple_filename):
            log_job(job, f"'{t}' already on Shypple as '{shypple_filename}' - same filename as the email's attachment, no upload needed.")
            continue

        if not row.get("downloadHref"):
            log_job(job, f"'{t}' appears uploaded but has no download link - flagging for manual review.")
            needs_upload.append({"type": t, "attachment_index": idx, "reason": "could not verify: uploaded document has no download link"})
            continue

        shypple_bytes, shypple_mime = download_shypple_document_bytes(page, row["downloadHref"])
        if shypple_bytes is None:
            log_job(job, f"Could not download the uploaded '{t}' from Shypple to compare - flagging for manual review.")
            needs_upload.append({"type": t, "attachment_index": idx, "reason": "could not verify: could not download Shypple's copy to compare"})
            continue

        try:
            saved_filename = save_document_locally(shypple_bytes, row.get("filename") or f"{t}.pdf")
            log_job(job, f"Saved a local copy of '{t}' ({saved_filename}) to downloads.")
            with STATE_LOCK:
                job.setdefault("downloaded_files", []).append({"type": t, "filename": saved_filename})
        except Exception as e:
            log_job(job, f"Could not save a local copy of '{t}': {e}")

        verdict = _compare_document_versions_remote(
            job["message_id"], t, shypple_bytes, shypple_mime, subject=job.get("subject", ""), attachment_index=idx
        )
        if verdict is None or verdict.get("same") is None:
            reason = verdict.get("reason") if verdict else "no response"
            log_job(job, f"Could not get a comparison verdict for '{t}' ({reason}) - flagging for manual review.")
            needs_upload.append({"type": t, "attachment_index": idx, "reason": f"could not verify: {reason}"})
            continue

        if verdict.get("same"):
            log_job(job, f"'{t}' already on Shypple and matches the email's version exactly - no upload needed.")
        else:
            reason = verdict.get("reason", "")
            differences = verdict.get("differences") or []
            if differences:
                diff_summary = "; ".join(
                    f"{d.get('field')}: email='{d.get('email_value')}' vs shypple='{d.get('shypple_value')}'"
                    for d in differences
                )
            else:
                diff_summary = reason
            log_job(job, f"'{t}' is uploaded but content DIFFERS from the email's version: {diff_summary}")
            needs_upload.append({"type": t, "attachment_index": idx, "reason": f"content differs: {reason}", "differences": differences})

    with STATE_LOCK:
        job["uploaded_doc_types"] = uploaded_types_raw
        job["missing_doc_types"] = [n["type"] for n in needs_upload]
        job["upload_reasons"] = needs_upload

    if not needs_upload:
        # Nothing needed uploading - every extracted type was already present and
        # verified to match content-for-content - mark read + yellow star
        _star_source_email(job, "yellow")
        _mark_source_email_read(job)
        set_job_status(job, "up_to_date", phase="Done - documents present and verified")
        log_job(job, "All extracted document types are present on Shypple and verified as matching. Marked email read + yellow star.")
        return

    # Per operator request: Single manual verification workflow for CMR process.
    # Proceed directly to preparing and filling document form, pausing ONLY at awaiting_submit_confirmation.
    set_job_status(job, "processing", phase="Preparing document upload...")

    # Read the customer name once (only if some type actually needs it) rather than
    # per-document - it's the same shipment throughout this job.
    customer_name = None
    if any(n["type"] not in ORG_SKIP_TYPES for n in needs_upload):
        customer_name = read_shipment_customer(page)
        if not customer_name:
            log_job(job, "Could not read the Customer name from the Info tab - uploads that need it will skip organization.")
        if not _ensure_org_before_documents_tab(page, job, expected_org=job.get("organization_switched_to")):
            return
        open_documents_tab(page)

    uploaded_count, upload_failures = 0, []
    for n in needs_upload:
        t = n["type"]
        file_bytes, file_mime, filename, fetch_error = _fetch_email_document(
            job["message_id"], t, subject=job.get("subject", ""), attachment_index=n.get("attachment_index")
        )
        if file_bytes is None:
            reason = fetch_error or "unknown reason"
            upload_failures.append({"type": t, "error": f"Could not fetch the email's own file to upload: {reason}"})
            log_job(job, f"Failed to upload '{t}': could not fetch the email's own file ({reason}).")
            continue

        type_override = (job.get("type_overrides") or {}).get(t)
        fill_result = fill_shipment_document_form(
            page, t, file_bytes, file_mime, filename, containers, customer_name, type_override=type_override
        )
        if not fill_result.get("success"):
            upload_failures.append({"type": t, "error": fill_result.get("error")})
            log_job(job, f"Failed to prepare upload for '{t}': {fill_result.get('error')}")
            continue

        # Pause here, form filled but NOT submitted yet - per explicit request, a human
        # must verify the file/type/containers/organization before Create Shipment
        # document is actually clicked.
        _open_confirmation_gate()
        set_job_status(
            job, "awaiting_submit_confirmation",
            phase=f"Waiting for confirmation to submit '{t}'",
            submit_preview={
                "type": t,
                "filename": fill_result.get("filename"),
                "matched_type": fill_result.get("matched_type"),
                "containers": fill_result.get("picked_containers"),
                "organization": fill_result.get("picked_org"),
            },
        )
        log_job(job, f"Form filled for '{t}' (file: {filename}, containers: {fill_result.get('picked_containers')}, "
                      f"organization: {fill_result.get('picked_org')}) - waiting for confirmation before submitting.")
        if not _wait_for_confirmation(job):
            set_job_status(job, "skipped_by_operator", phase="Skipped by operator")
            log_job(job, f"Skipped by operator - '{t}' was NOT submitted (form left filled but unsent).")
            return
        set_job_status(job, "processing", phase=f"Submitting '{t}'...")

        result = submit_shipment_document_form(page)
        if result.get("success"):
            uploaded_count += 1
            log_job(job, f"Uploaded '{t}' ({filename}).")
        else:
            upload_failures.append({"type": t, "error": result.get("error")})
            log_job(job, f"Failed to upload '{t}': {result.get('error')}")

    with STATE_LOCK:
        job["uploaded_count"] = uploaded_count
        job["upload_failures"] = upload_failures

    if upload_failures:
        set_job_status(job, "upload_partial" if uploaded_count else "upload_failed",
                        phase="Done - some upload(s) failed")
    else:
        # Per the operator's explicit rule: every document uploaded successfully ->
        # mark the source email with a yellow star, matching this account's existing
        # "Process yellow-starred" -> "Processed - India filing" sweep, so a
        # successfully-processed email is ready to move there - and mark it READ (a
        # completed upload, like the "nothing needed uploading" branch above, ends in
        # read + yellow star). Matches the LCL Arrivals/Release pipeline's terminal
        # rule (see handle_delay_or_devanning/handle_arrival_notice/
        # handle_delivery_order) - both pipelines agree.
        _star_source_email(job, "yellow")
        _mark_source_email_read(job)
        set_job_status(job, "uploaded", phase="Done - all document(s) uploaded")


def run_batch(page, jobs, email, password):
    # batch_state (running/jobs/started_at) is already set synchronously by the
    # /run_batch HTTP handler, BEFORE it responds - so a /status poll can never observe
    # the pre-batch default state (running=False, jobs=[]) and misreport "done, 0
    # processed" for a batch that hasn't actually started yet (queue.Queue hand-off to
    # this thread has an inherent delay of up to the 0.5s poll interval in main()).
    try:
        ensure_shypple_login(page, email, password)
        ensure_admin_access(page)
        ensure_org_is_shypple_bv(page)
    except Exception as e:
        # Login/admin-access setup happens once for the whole batch, BEFORE any
        # per-job try/except - if it fails, every job would otherwise be left stuck at
        # "queued" forever with running flipped to False, which read to the dashboard
        # as a silent, unexplained "Done - N processed" instead of a visible failure.
        reason = str(e)[:300]
        log_system(f"Could not establish Shypple admin access: {reason}")
        with STATE_LOCK:
            for job in jobs:
                job["status"] = "error"
                job["error"] = f"Could not establish Shypple admin access: {reason}"
                job.setdefault("log", []).append(
                    f"[{datetime.now().strftime('%H:%M:%S')}] Blocked before reaching this job: {reason}"
                )
            batch_state["paused_reason"] = None
            batch_state["running"] = False
            batch_state["finished_at"] = datetime.now(timezone.utc).isoformat()
        return

    for idx, job in enumerate(jobs):
        # A job inserted mid-batch by _process_discovered_arrival_notice (a Delivery
        # Order's Arrival Notice cross-verification, found via BL-number search) is
        # already fully processed synchronously by the time this loop's own
        # enumerate() reaches its position - without this guard it would be handed
        # to process_lcl_arrival_job a second time, from scratch, re-searching the
        # shipment and re-running whichever handler already just ran.
        if job.get("_inline_handled"):
            continue
        with STATE_LOCK:
            batch_state["current_index"] = idx
        job_is_lcl = job.get("flow") == "lcl_arrivals"
        # Defensive guard: this instance is dedicated to one role (see main()'s
        # --role) - a job for the other pipeline landing here means a stale or
        # misconfigured caller, not a job this browser/org context is set up for.
        if job_is_lcl != (CURRENT_ROLE == "lcl"):
            set_job_status(
                job, "error",
                error=f"This Shypple browser instance is running as role='{CURRENT_ROLE}' and "
                      f"cannot process a {'LCL' if job_is_lcl else 'CMR'} job - it was routed here "
                      f"by mistake.",
            )
            log_job(job, f"Skipped: wrong role for this instance (role={CURRENT_ROLE}).")
            continue
        try:
            if job_is_lcl:
                process_lcl_arrival_job(page, job)
            else:
                process_one_job(page, job)
        except Exception as e:
            set_job_status(job, "error", error=str(e)[:300])
            log_job(job, f"Unhandled error: {e}")

    with STATE_LOCK:
        batch_state["running"] = False
        batch_state["finished_at"] = datetime.now(timezone.utc).isoformat()
    log_system("Batch finished.")


class ControlServer(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_json(self, obj, status=200):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/status":
            with STATE_LOCK:
                snapshot = json.loads(json.dumps(batch_state))
            self._send_json({"success": True, "state": snapshot})
            return
        if parsed.path == "/document_type_options":
            # Pure file read, no Playwright access needed - safe to answer directly
            # from this handler thread regardless of whether a batch is running.
            cached = _load_document_type_options()
            if cached:
                self._send_json({"success": True, **cached})
            else:
                self._send_json({"success": False, "error": "No document type options captured yet."})
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            payload = {}

        if parsed.path == "/run_batch":
            jobs = payload.get("jobs") or []
            for j in jobs:
                j.setdefault("status", "queued")
                j.setdefault("log", [])

            with STATE_LOCK:
                if batch_state.get("running"):
                    self._send_json({
                        "success": False,
                        "error": "A batch is already running - wait for it to finish before starting another.",
                    }, status=409)
                    return
                # Set the visible state HERE, synchronously, before responding - not in
                # run_batch() on the main thread, which only picks this batch up off the
                # queue after its own poll loop notices it (up to ~0.5s later). Without
                # this, a /status poll that lands in that gap sees the untouched default
                # (running=False, jobs=[]) and misreports "Done - 0 email(s) processed."
                batch_state["running"] = True
                batch_state["jobs"] = jobs
                batch_state["current_index"] = -1
                batch_state["started_at"] = datetime.now(timezone.utc).isoformat()
                batch_state["finished_at"] = None

            incoming_batches.put(jobs)
            self._send_json({"success": True, "queued": len(jobs)})
            return

        if parsed.path == "/proceed":
            with STATE_LOCK:
                is_paused = batch_state.get("paused_reason") is not None or any(
                    j.get("status") in _AWAITING_STATUSES
                    for j in batch_state.get("jobs", [])
                )
            if is_paused:
                proceed_event.set()
                self._send_json({"success": True})
            else:
                self._send_json({"success": False, "error": "Nothing is currently paused."})
            return

        if parsed.path == "/skip":
            # Only meaningful for a job paused on one of its own confirmation gates -
            # NOT the batch-wide google_login pause (there's no single job to skip
            # there, and skipping would just re-hit the same login wall on the very
            # next job). Sets _skip_requested BEFORE waking the wait, so
            # _wait_for_confirmation sees it the instant it wakes.
            global _skip_requested
            with STATE_LOCK:
                job_is_paused = any(
                    j.get("status") in _AWAITING_STATUSES
                    for j in batch_state.get("jobs", [])
                )
                if job_is_paused:
                    _skip_requested = True
            if job_is_paused:
                proceed_event.set()
                self._send_json({"success": True})
            else:
                self._send_json({"success": False, "error": "Nothing is currently awaiting confirmation to skip."})
            return

        if parsed.path == "/refresh_document_type_options":
            with STATE_LOCK:
                busy = batch_state.get("running", False)
            if busy:
                self._send_json({
                    "success": False,
                    "error": "A batch is currently running - the automation browser can't navigate "
                             "away to refresh this right now. Try again once it's done.",
                }, status=409)
                return
            req = _TypeOptionsRequest()
            type_options_requests.put(req)
            fulfilled = req.event.wait(timeout=40)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}
            self._send_json(result)
            return

        self.send_response(404)
        self.end_headers()


def start_http_server(port):
    server = HTTPServer(("127.0.0.1", port), ControlServer)
    print(f"Shypple control server running on http://127.0.0.1:{port}")
    server.serve_forever()


def _seed_lcl_storage_state(role, user_data_dir):
    """First-run convenience only: if this is a brand-new LCL profile (never
    launched before) and the CMR profile already has a saved login
    (_save_shypple_session_tokens writes data/shypple_profile/storage_state.json on
    every successful login/org-switch), seed the new profile with that session."""
    if role != "lcl" or os.path.exists(user_data_dir):
        return
    cmr_storage_state = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "shypple_profile", "storage_state.json")
    )
    if os.path.exists(cmr_storage_state):
        try:
            os.makedirs(user_data_dir, exist_ok=True)
            target = os.path.join(user_data_dir, "storage_state.json")
            shutil.copy2(cmr_storage_state, target)
            print(f"[Shypple] Seeded new LCL profile's login session to: {target}")
        except Exception as e:
            print(f"[Shypple] Could not seed storage state: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=list(ROLE_CONFIG.keys()), default="cmr",
                         help="Which pipeline this instance serves - determines its "
                              "control-server port and Chrome profile so CMR and LCL "
                              "can each run their own independent Shypple browser.")
    args = parser.parse_args()

    global CURRENT_ROLE
    CURRENT_ROLE = args.role
    role_cfg = ROLE_CONFIG[CURRENT_ROLE]
    port = role_cfg["port"]

    email = os.environ.get("SHYPPLE_EMAIL", "")
    password = os.environ.get("SHYPPLE_PASSWORD", "")
    if not email or not password:
        print("[Shypple] WARNING: SHYPPLE_EMAIL / SHYPPLE_PASSWORD not set - login will be skipped.")

    user_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", role_cfg["profile_dir"]))
    _seed_lcl_storage_state(CURRENT_ROLE, user_data_dir)
    os.makedirs(user_data_dir, exist_ok=True)

    threading.Thread(target=start_http_server, args=(port,), daemon=True).start()

    print(f"[Shypple] role={CURRENT_ROLE} - launching Playwright with persistent context in: {user_data_dir}")
    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                channel="chrome",
                args=["--start-maximized"],
                no_viewport=True,
            )
        except Exception as e:
            print(f"Could not launch with Chrome channel: {e}. Falling back to default chromium.")
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                args=["--start-maximized"],
                no_viewport=True,
            )

        page = context.pages[0] if context.pages else context.new_page()
        print("Shypple automation browser window opened. Feel free to log in manually if prompted.")
        try:
            if page.url == "about:blank":
                page.goto(SHYPPLE_LOGIN_URL, timeout=30000)
        except Exception as e:
            print(f"[Shypple] Could not load initial Shypple URL: {e}")

        try:
            while len(context.pages) > 0:
                try:
                    jobs = incoming_batches.get(timeout=0.5)
                except queue.Empty:
                    # Only serviced while idle (no batch waiting) - a type-options
                    # refresh navigates away from wherever `page` currently is, which
                    # would be unsafe to do mid-batch while a job owns that state.
                    while True:
                        try:
                            req = type_options_requests.get_nowait()
                        except queue.Empty:
                            break
                        try:
                            req.result = _refresh_document_type_options(page)
                        except Exception as e:
                            req.result = {"success": False, "error": str(e)}
                        req.event.set()
                    page.wait_for_timeout(200)
                    continue
                try:
                    run_batch(page, jobs, email, password)
                except Exception as e:
                    print(f"[Shypple] Batch failed: {e}")
                    with STATE_LOCK:
                        batch_state["running"] = False
                        batch_state["finished_at"] = datetime.now(timezone.utc).isoformat()
                        batch_state["error"] = str(e)
        except Exception as e:
            print(f"[Shypple main loop] Unexpected error, shutting down: {e}")
        finally:
            context.close()
            print("Shypple browser context closed.")


if __name__ == "__main__":
    main()
