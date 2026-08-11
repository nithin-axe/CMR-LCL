import os
import sys
import json
import time
import queue
import hashlib
import re
from playwright.sync_api import sync_playwright
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from urllib.parse import urlparse, parse_qs, quote

from shared.download_utils import fetch_bytes_robust, get_downloads_dir

# Shared references. Playwright's sync API is greenlet-based and can only be called
# from the thread that created it (the main thread, inside sync_playwright()). The
# HTTP control server runs on its own thread, so it must never touch these directly -
# instead it enqueues a Request and waits for the main thread's loop to fulfil it.
gmail_page_ref = None
# The delegated nl.importsea@shypple.com inbox tab (see RELEASE_ORDERS_INBOX_URL below) -
# same account as gmail_page_ref, but its own separate label set. "a-release-orders"
# lives here, NOT in the a-cmr label gmail_page_ref points at. Stays None (and the tab
# stays closed) until forward-and-relabel actually needs it - see
# _ensure_release_orders_page. This pipeline only ever opens the a-cmr tab at startup.
release_orders_page_ref = None
# A SECOND, permanently-open tab pinned to the "lcl-arrivals---release" label -
# separate from gmail_page_ref (which stays on a-cmr) so both labels' mail lists are
# live and scraped simultaneously, and dashboard actions (star/mark-read/fetch) never
# need to wait for a label switch. Created once at startup in main(), alongside
# gmail_page_ref. See _resolve_action_page for how a per-message action picks between
# the two tabs, and do_scrape_emails' label/output_path params for how each tab writes
# to its own scrape file.
lcl_page_ref = None
LCL_LABEL_KEY = "lcl-arrivals---release"
_LCL_SCRAPED_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "scraped_emails_lcl.json")
)
# Message ids currently known to live under LCL_LABEL_KEY (rebuilt on every scrape of
# lcl_page_ref) - lets _resolve_action_page route a star/mark-read/fetch request to the
# correct tab without threading a label/tab parameter through every caller in
# tracking_api.py/operations_api.py/shypple_process.py. Defaults to gmail_page_ref for
# anything not in this set, which is exactly the existing (pre-dual-tab) behavior.
_lcl_message_ids = set()
_lcl_message_ids_lock = threading.Lock()
request_queue = queue.Queue()

# Per-email cache of attachment-name scrape results, keyed by the same "pw_<legacy_id>"
# id used everywhere else. The periodic list-view scrape below re-visits every row in
# the label every ~2s, and the attachment-name lookup (a full descendant DOM walk per
# row) is by far its most expensive step - but a given email's attachments never
# change, so once a row has been scraped there is no need to pay that cost again.
# Without this, a label that has grown to 100+ rows (this pipeline only moves rows out
# via a separate, occasional "Process yellow-starred" sweep, so a-cmr accumulates
# processed-but-not-yet-swept mail all day) makes the scrape take many seconds, and
# since it's not interruptible (see the request_queue.empty() check below), every
# star/mark-read/fetch request that lands mid-scrape has to wait for the ENTIRE
# scrape to finish first - this is what caused star/read/next-mail to each visibly
# slow down as the label filled up over the day.
_row_attachment_cache = {}

# So /health can prove (not just claim) whether THIS running process reflects the
# current file on disk - recurring source of confusion where a fix is made but the
# already-running script never got restarted, so the old behavior silently persists.
_SCRIPT_PATH = os.path.abspath(__file__)
_SCRIPT_STARTED_AT = time.time()

active_label = "a-cmr"

def normalize_label(label_name):
    lbl = (label_name or "").strip()
    if not lbl or lbl in ("a-cmr", "cmr", "CMR Process"):
        return "a-cmr", "a-cmr"
    if "lcl" in lbl.lower() or "arrival" in lbl.lower() or "release" in lbl.lower():
        return "lcl-arrivals---release", "LCL Arrivals | Release"
    return lbl, lbl

def get_label_url(label_name=None):
    lbl_key, lbl_gmail = normalize_label(label_name or active_label)
    encoded = quote(lbl_gmail)
    return f"https://mail.google.com/mail/u/0/d/AEoRXRTpk-vnl6i_A_JlRYw6MkDiC4PKNiBFMfjDlPZe5HKmE9ML/#label/{encoded}"

# Primary scraped label URL (backwards compatible)
LABEL_URL = get_label_url("a-cmr")


def _list_view_url_for(page):
    """The label list-view URL to fall back to for THIS specific page/tab.

    lcl_page_ref is a dedicated tab permanently pinned to LCL_LABEL_KEY - it never
    changes label - so any "return to the list view" fallback acting on it must always
    go back to the LCL list, never the module-level LABEL_URL constant (frozen to
    a-cmr at import time). Using LABEL_URL unconditionally was the bug: fetch_email_body's
    "Back to list" fallback and _restore_unread's post-search-retry both did
    ``page.goto(LABEL_URL)`` regardless of which page they were actually operating on -
    so reading an LCL mail on lcl_page_ref (via _resolve_action_page) could silently
    navigate the LCL TAB ITSELF over to the a-cmr label view whenever that fallback
    fired. Any other page (gmail_page_ref) uses active_label - which tracks whichever
    label that tab is really showing - rather than the frozen a-cmr default, so this
    also stays correct if the user has since switched it via the label box."""
    if page is lcl_page_ref:
        return get_label_url(LCL_LABEL_KEY)
    return get_label_url(active_label)

# The same delegated mailbox, opened as a second tab ONLY on demand (see
# _ensure_release_orders_page) so the forward-and-relabel flow (org-empty CMR -> forward
# to a-release-orders) can apply the label on the forwarded copy that arrives here.
# "a-release-orders" lives in THIS mailbox, not in the a-cmr label's own picker.
RELEASE_ORDERS_INBOX_URL = "https://mail.google.com/mail/u/0/d/AEoRXRTpk-vnl6i_A_JlRYw6MkDiC4PKNiBFMfjDlPZe5HKmE9ML/#inbox"

# Tracks which message ids have already been forwarded via forward_and_relabel, so a
# re-run of "Process blue-starred" (or a repeated Shypple no-organization job) skips
# mail already sent instead of forwarding a duplicate. Keyed by Gmail's own internal
# legacy message id (same id forward_and_relabel is called with). Persisted to disk so
# it survives a script restart, not just this process's lifetime.
_FORWARDED_TRACKER_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "forwarded_mails.json")
)
_FORWARDED_TRACKER_LOCK = threading.Lock()


def _load_forwarded_ids():
    try:
        if os.path.exists(_FORWARDED_TRACKER_PATH):
            with open(_FORWARDED_TRACKER_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_forwarded_ids(data):
    os.makedirs(os.path.dirname(_FORWARDED_TRACKER_PATH), exist_ok=True)
    tmp = _FORWARDED_TRACKER_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, _FORWARDED_TRACKER_PATH)


def _mark_forwarded(message_id, to_email, add_label, subject="", relabeled=False):
    """Record that message_id was forwarded - subject is kept so a later retry (the
    dashboard's "Label pending release-orders" button, see label_pending_relabel) can
    search the release-orders mailbox for the forwarded copy without re-opening the
    original mail. relabeled reflects whether _label_forwarded_copy already succeeded
    at forward time; _mark_relabeled flips it to True later if a retry succeeds."""
    with _FORWARDED_TRACKER_LOCK:
        data = _load_forwarded_ids()
        data[message_id] = {
            "to": to_email,
            "label": add_label,
            "subject": subject,
            "relabeled": relabeled,
            "forwarded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        _save_forwarded_ids(data)


def _mark_relabeled(message_id, relabeled=True):
    with _FORWARDED_TRACKER_LOCK:
        data = _load_forwarded_ids()
        if message_id not in data:
            return
        data[message_id]["relabeled"] = relabeled
        _save_forwarded_ids(data)


class ActionRequest:
    def __init__(self, action, message_id, **extra):
        self.action = action
        self.message_id = message_id
        self.result = None
        self.event = threading.Event()
        for key, value in extra.items():
            setattr(self, key, value)


# Playwright's page.evaluate() expects the whole string to be a single function or
# expression, so these helpers are embedded *inside* each consumer's function body
# (JS allows nested function declarations) rather than concatenated as sibling
# top-level statements.
#
# Rows are matched by Gmail's own internal message id (the "bqe" span's
# data-legacy-last-message-id attribute), not by subject/sender/date text. This
# label routinely has multiple genuinely duplicate-looking emails (same subject,
# sender, and displayed time - shipping notification systems resend these), so
# fuzzy text matching could silently act on the wrong one of two lookalike rows.
_FIND_ROW_JS = """
    function findRowById(msgId) {
        const rows = document.querySelectorAll("tr.zA");
        for (const row of rows) {
            // Not scoped to .bqe specifically - read vs. unread rows structure this
            // differently, so search the whole row for whichever element carries it.
            const idEl = row.querySelector('[data-legacy-last-message-id]');
            if (idEl && idEl.getAttribute('data-legacy-last-message-id') === msgId) return row;
        }
        return null;
    }
"""

# Toolbar button identification: class tokens (nv/nX/m9/bvt) were captured live from
# this account's Gmail deployment via a one-off DOM inspection; aria-label text (EN +
# NL, since this mailbox's Gmail UI is in Dutch) is the fallback if that ever changes.
_TOOLBAR_HELPERS_JS = """
    function hasClassToken(el, token) {
        return el.className.split(/\\s+/).includes(token);
    }
    function getToolbarButtons() {
        const toolbar = document.querySelector('[gh="mtb"]');
        return toolbar ? Array.from(toolbar.querySelectorAll('[role="button"]')) : [];
    }
    function findArchiveButton(buttons) {
        let btn = buttons.find(b => hasClassToken(b, 'nv'));
        if (btn) return btn;
        btn = buttons.find(b => {
            const label = (b.getAttribute('aria-label') || '').trim();
            return /verwijderen$/i.test(label) && label.split(' ').length > 1;
        });
        if (btn) return btn;
        return buttons.find(b => /archive/i.test(b.getAttribute('aria-label') || ''));
    }
    function findDeleteButton(buttons) {
        let btn = buttons.find(b => hasClassToken(b, 'nX'));
        if (btn) return btn;
        return buttons.find(b => {
            const label = (b.getAttribute('aria-label') || '').trim().toLowerCase();
            return label === 'verwijderen' || label === 'delete';
        });
    }
    function findMarkReadButton(buttons) {
        // Text match FIRST here, unlike findArchiveButton/findDeleteButton above -
        // "mark as read" vs "mark as unread" (EN and NL) are unambiguous phrases that
        // never collide as substrings of each other, so aria-label is the more
        // trustworthy signal for exactly these two, not just a fallback. The 'm9'
        // class token was a one-off capture from this account's build and, if Gmail
        // ever reassigns/changes it (or it was mis-captured against the wrong
        // button to begin with), would confidently match the WRONG toolbar button
        // and click it without ever reaching this text check - which is exactly what
        // made "mark as read" actually mark the mail unread instead.
        let btn = buttons.find(b => /markeren als gelezen|mark as read/i.test(b.getAttribute('aria-label') || ''));
        if (btn) return btn;
        return buttons.find(b => hasClassToken(b, 'm9'));
    }
    function findMarkUnreadButton(buttons) {
        let btn = buttons.find(b => /markeren als ongelezen|mark as unread/i.test(b.getAttribute('aria-label') || ''));
        if (btn) return btn;
        return buttons.find(b => hasClassToken(b, 'bvt'));
    }
"""

# Forward + label-picker identification for the "org empty -> forward & move" action.
# Unlike the toolbar buttons above (whose class tokens were captured from a live DOM
# inspection), these have NOT been inspected directly yet - they're aria-label /
# visible-text matches only (EN + NL). Treat this as a first pass that may need
# correcting against the real DOM once tried live (see the memory note on this repo
# about guessed Gmail selectors failing before).
_FORWARD_AND_LABEL_HELPERS_JS = """
    function findByLabel(els, patterns) {
        return els.find(b => {
            const label = (b.getAttribute('aria-label') || b.getAttribute('data-tooltip') || b.textContent || '').trim();
            return patterns.some(p => p.test(label));
        });
    }
    function findForwardButton() {
        const all = Array.from(document.querySelectorAll('[role="button"], span[role="link"], div[role="link"]'));
        return findByLabel(all, [/^forward$/i, /doorsturen/i]);
    }
    function findSendButton() {
        const all = Array.from(document.querySelectorAll('[role="button"]'));
        // This build's actual Dutch label is "Sturen" (confirmed live) - "Verzenden"
        // was a guess and never matched. Keep both plus English, anchored so a loose
        // substring match elsewhere on the page (this queries the whole document,
        // not just the compose box) can't grab the wrong element.
        return findByLabel(all, [/^send$/i, /^verzenden$/i, /^sturen$/i]);
    }
    function findLabelsButton() {
        const toolbar = document.querySelector('[gh="mtb"]');
        const buttons = toolbar ? Array.from(toolbar.querySelectorAll('[role="button"]')) : [];
        // Dutch Gmail sometimes leaves "Labels" untranslated, but "Etiketten" is the
        // literal translation - check both since this mailbox's UI is Dutch.
        return findByLabel(buttons, [/^label/i, /etiket/i]);
    }
    function findLabelOption(labelName) {
        const items = Array.from(document.querySelectorAll('[role="menuitemcheckbox"], [role="checkbox"], [role="option"]'));
        const needle = labelName.toLowerCase();
        return items.find(el => (el.textContent || '').trim().toLowerCase().includes(needle));
    }
    function findApplyButton() {
        const all = Array.from(document.querySelectorAll('[role="button"]'));
        return findByLabel(all, [/^(apply|toepassen|ok|done|gereed)$/i]);
    }
    function findLabelSearchInput() {
        // Scope to the open popup/menu first - a bare [placeholder] match previously
        // matched Gmail's OWN mail search bar instead (it has a placeholder too), and
        // typing the label name into that corrupted the active search filter instead
        // of searching within the label picker.
        const scoped = document.querySelector('[role="menu"] input, [role="dialog"] input, [role="listbox"] input');
        if (scoped) return scoped;
        return document.querySelector('input[aria-label*="abel" i]');
    }
"""


def _safe_goto(page, url, attempts=3, timeout_ms=45000, wait_until="domcontentloaded"):
    """page.goto() that tolerates a slow/cold-cache load without taking down the whole
    browser automation process. This script wipes the profile's disk cache on every
    launch (see cache-clearing in main()), so Gmail's heavy SPA bundle has to be
    fully re-downloaded on the very next navigation - combined with Gmail keeping
    long-lived connections open (so a strict "load" event can be slow to fire), the
    default 30s goto timeout can be exceeded on a cold start even with nothing wrong.
    domcontentloaded is enough for the DOM queries this script relies on. Retries a
    few times and returns False (instead of raising) if it never succeeds, so a single
    slow first load degrades gracefully instead of crashing the entire script."""
    last_err = None
    for attempt in range(attempts):
        try:
            page.goto(url, timeout=timeout_ms, wait_until=wait_until)
            return True
        except Exception as e:
            last_err = e
            print(f"[Startup] Navigation to {url} timed out/failed (attempt {attempt + 1}/{attempts}): {e}")
    print(f"[Startup] Giving up on {url} after {attempts} attempts, continuing anyway: {last_err}")
    return False


def _ensure_gmail_logged_in(page, max_wait_s=300):
    """Check whether THIS browser profile is actually signed in to Google before ever
    navigating to LABEL_URL - LABEL_URL is a delegated-mailbox deep link that encodes
    an existing session's own internal id (the "/d/<id>/" segment), captured once from
    an already-logged-in session on the machine this project was originally built on.
    It is NOT a portable "go to this label" URL: opened in a browser profile that has
    never logged in at all (a fresh data/playwright_profile - exactly what a new
    machine/office deployment starts with), Google rejects it outright with a 403
    instead of redirecting to a login page the way a normal Gmail URL would.

    Navigates to a plain, generic Gmail URL first instead. If Google redirects that to
    its own accounts.google.com sign-in flow, this prints the REAL login URL Google
    just generated and waits (polling, not blocking any other startup work) for a
    human to complete it manually in this same visible window - credentials are never
    entered by the automation itself, only a real person's own Google login. Once
    that's done, the caller's own navigation to LABEL_URL will work normally, the same
    as it always has on a machine that was already logged in."""
    try:
        page.goto("https://mail.google.com/mail/", timeout=45000, wait_until="domcontentloaded")
    except Exception as e:
        print(f"[Startup] Could not load a plain Gmail URL to check login state: {e}")
        return False

    page.wait_for_timeout(1500)
    if "accounts.google.com" not in (page.url or ""):
        return True  # already signed in in this profile - nothing to do

    login_url = page.url
    print("=" * 78)
    print("[Startup] This browser profile is not signed in to Google yet.")
    print(f"[Startup] Please sign in now at the URL open in the browser window:\n          {login_url}")
    print(f"[Startup] Waiting up to {max_wait_s}s for sign-in to complete...")
    print("=" * 78)

    waited = 0
    while waited < max_wait_s:
        page.wait_for_timeout(3000)
        waited += 3
        if "accounts.google.com" not in (page.url or ""):
            print("[Startup] Google sign-in detected - continuing.")
            return True
    print(f"[Startup] Still not signed in after {max_wait_s}s - continuing anyway "
          "(the label navigation below will likely fail until you sign in).")
    return False


def _safe_click(el, timeout_ms=4000, label="element"):
    """Click an ElementHandle without letting a bad match take the whole calling
    function down with it. findByLabel's generic word matches ("apply"/"ok"/"done")
    can land on some unrelated, invisible element elsewhere on the page (e.g. a hidden
    dialog's own "Done" button) - Playwright's default actionability wait is 30s, and an
    uncaught timeout there previously aborted the ENTIRE action (label already applied
    or not), reporting total failure over one redundant/mismatched click. Returns True
    if the click actually happened."""
    if el is None:
        return False
    try:
        el.click(timeout=timeout_ms)
        return True
    except Exception as e:
        print(f"[Server] _safe_click: click on {label} did not complete ({e}) - continuing anyway.")
        return False


def _click_row_by_id(page, message_id):
    """Find message_id's row in the CURRENTLY rendered list and click it, capturing
    the list-view attachment chip names AND the row's unread state first (before the
    row's context changes on click - opening a message marks it read in Gmail, and the
    caller needs to know whether to undo that afterward). Returns
    {"clicked": bool, "chipNames": [...], "wasUnread": bool}.

    chipNames extraction previously queried a hardcoded '.brd .brc' selector, which
    doesn't match anything in this build's obfuscated markup (same caveat this file
    documents elsewhere: Gmail's CSS class names are obfuscated per build and not a
    durable thing to guess) - it always came back empty, so fetch_email_body's
    filename resolution always fell through past this to its own last-resort generic
    "Attachment {i+1}" (with no extension), which is why classification/upload/the
    dashboard all ended up showing a useless placeholder name instead of anything
    real. Now reuses the SAME class-agnostic, file-extension-regex walk the periodic
    list scrape (in main()'s loop below) already relies on for this exact same data,
    which - unlike a hardcoded class name - actually finds real chip text like
    "Attachment 1.jpg" reliably here."""
    return page.evaluate("(data) => {" + _FIND_ROW_JS + r"""
        const row = findRowById(data.msgId);
        if (!row) return { clicked: false, chipNames: [], wasUnread: false };

        const chipNames = [];
        for (const c of row.querySelectorAll('*')) {
            const txt = (
                c.getAttribute('title') ||
                c.getAttribute('data-tooltip') ||
                c.getAttribute('aria-label') ||
                (c.children.length === 0 ? c.innerText : '') || ''
            ).trim();
            if (txt && /\.(pdf|docx?|xlsx?|png|jpe?g|txt|zip|rar|csv)\b/i.test(txt)) {
                if (!chipNames.includes(txt) && txt.length < 150) {
                    chipNames.push(txt);
                }
            }
        }
        const wasUnread = (row.className || '').split(' ').includes('zE');

        row.click();
        return { clicked: true, chipNames: chipNames, wasUnread: wasUnread };
    }
    """, {"msgId": message_id})


def _row_exists_by_id(page, message_id):
    return page.evaluate("(data) => {" + _FIND_ROW_JS + """
        return !!findRowById(data.msgId);
    }""", {"msgId": message_id})


def fetch_email_body(page, message_id, download_bytes=False, subject=""):
    """Click the matching row and extract the full body + attachments. Must run on
    the main thread that owns the Playwright sync objects.

    When ``download_bytes`` is True, each attachment's raw bytes are also fetched
    (base64-encoded into ``data_b64``) using the logged-in browser session's cookies -
    this is what the deep document-type scan needs to read the actual PDF/image pages.
    Downloading opens the mail (a click), so this is only used by the explicit deep
    scan, never the read-safe bulk sweep.

    If message_id's row isn't in the CURRENTLY rendered list (e.g. it scrolled out of
    the initially-loaded view since it was first classified - the live list only ever
    holds whatever's presently rendered, not a persistent index), this previously just
    failed outright ("could not find row"), permanently breaking any later re-fetch
    (View/Download, a deep-classify retry) for an email that's still very much real,
    just not on screen anymore. If ``subject`` is given, search Gmail for it as a
    fallback and retry once results load."""
    t0 = time.time()

    def _elapsed():
        return f"{time.time() - t0:.1f}s"

    try:
        click_result = _click_row_by_id(page, message_id)

        if not click_result.get("clicked") and subject:
            snippet = subject.strip()[:70]
            if snippet:
                print(f"[Server] fetch_email_body: row not found for '{message_id}' in the "
                      f"current view - searching by subject '{snippet}' instead.")
                base_url = LABEL_URL.split("#", 1)[0]
                search_url = f"{base_url}#search/{quote(chr(34) + snippet + chr(34))}"
                page.goto(search_url)
                waited_ms = 0
                while waited_ms < 8000:
                    page.wait_for_timeout(500)
                    waited_ms += 500
                    if _row_exists_by_id(page, message_id):
                        break
                click_result = _click_row_by_id(page, message_id)

        if not click_result.get("clicked"):
            print(f"[Server] Could not find row matching message id: '{message_id}'"
                  + (f" (searched by subject too)" if subject else ""))
            return None

        chip_names = click_result.get("chipNames", [])
        was_unread = click_result.get("wasUnread", False)
        print(f"[Server] [{_elapsed()}] fetch_email_body: row clicked for '{message_id}'.")

        # Wait for email body element to appear
        page.wait_for_selector(".a3s", timeout=6000)
        page.wait_for_timeout(500)  # attachment cards can render a beat after the body
        print(f"[Server] [{_elapsed()}] fetch_email_body: body element visible.")

        # Extract email body HTML and a structured attachment list. Gmail's CSS class
        # names are obfuscated per build (confirmed by direct DOM inspection - guessing
        # class names is not durable), but the attachment ANCHOR's href is stable: it
        # always carries "attid=" + "view=att", and real attachments use disp=inline or
        # disp=attd - never disp=emb (that's reserved for inline images embedded in the
        # body, e.g. a forwarded signature logo, which are NOT attachments). A separate
        # sibling/ancestor element (class contains "CSS_CV_NEWATTCARDS" in this build,
        # but that's not load-bearing here) carries a `download_url="mime:filename:url"`
        # attribute with the clean real filename - used only to name the file; the
        # anchor's own href is what's actually fetched (proven reliable, the
        # download_url's embedded url is Gmail's internal proxy format and needlessly
        # fragile to parse).
        result = page.evaluate(r"""() => {
            const bodyEl = document.querySelector(".a3s");
            const html = bodyEl ? bodyEl.innerHTML : "";
            const attachments = [];
            const seenAttid = new Set();

            document.querySelectorAll('a[href*="attid="][href*="view=att"]').forEach(a => {
                const href = a.href || '';
                if (/disp=emb/i.test(href)) return; // inline body image, not a real attachment
                const m = href.match(/[?&]attid=([^&]+)/);
                const attid = m ? m[1] : href;
                if (seenAttid.has(attid)) return;
                seenAttid.add(attid);

                let filename = '';
                let mime = '';
                // Look for the download_url enrichment on this anchor or a few ancestors up.
                // Deliberately NO text-regex fallback here anymore if that's absent - a
                // filename containing spaces (very common, e.g. "Bratzler Packing List
                // Invoice MAV BR 012-26.xlsx") got truncated down to just "012-26.xlsx"
                // by an earlier regex fallback here, since [^\s/\\]+ can't cross a space.
                // That wrong-but-non-empty result pre-empted the Python-side chip_names
                // substitution below (which only fires when filename is EMPTY) - leaving
                // filename empty when download_url isn't found lets that more reliable
                // source (the list-view chip's own title attribute, not a fragile
                // locale-dependent text-regex) take over instead.
                let node = a;
                for (let i = 0; i < 4 && node; i++) {
                    const raw = node.getAttribute && node.getAttribute('download_url');
                    if (raw) {
                        const dm = raw.match(/^([^:]+):(.+?):(https?:\/\/.+)$/);
                        if (dm) { mime = dm[1]; filename = dm[2]; }
                        break;
                    }
                    node = node.parentElement;
                }

                attachments.push({ filename: filename, size: "", url: href, mime: mime });
            });

            return { body: html, attachments: attachments };
        }""")

        # Chip names were read from the LIST VIEW before the click - a reliable,
        # independent signal that this message DOES have attachment(s). If the
        # opened-view scan above still came back with none, the fixed 500ms wait
        # above wasn't enough this time (attachment cards can render a beat after the
        # body, per the comment above) - poll a bit longer rather than silently
        # returning zero attachments for a message that clearly has some (this
        # previously produced a "deep" classification cached with NO attachment-
        # sourced document at all, which then made every upload/compare using it fail
        # with "could not fetch the email's own file", permanently, since a "deep"
        # source is treated as already-complete and never retried).
        # Compares COUNT, not just emptiness - attachment cards render progressively,
        # so a 3-attachment message can easily have card #1 already in the DOM (making
        # result["attachments"] non-empty) while #2/#3 haven't rendered yet. The old
        # "not (result and result.get('attachments'))" check only re-scanned when ZERO
        # attachments were found, so any message where at least one card beat the
        # others into the DOM silently kept whatever partial list it caught on the
        # first pass - the rest were permanently missing from that classification (this
        # is what caused "for the CMR it does not fetch all the documents").
        retry_wait_ms = 0
        while chip_names and len((result or {}).get("attachments") or []) < len(chip_names) and retry_wait_ms < 8000:
            page.wait_for_timeout(700)
            retry_wait_ms += 700
            result = page.evaluate(r"""() => {
                const bodyEl = document.querySelector(".a3s");
                const html = bodyEl ? bodyEl.innerHTML : "";
                const attachments = [];
                const seenAttid = new Set();

                document.querySelectorAll('a[href*="attid="][href*="view=att"]').forEach(a => {
                    const href = a.href || '';
                    if (/disp=emb/i.test(href)) return;
                    const m = href.match(/[?&]attid=([^&]+)/);
                    const attid = m ? m[1] : href;
                    if (seenAttid.has(attid)) return;
                    seenAttid.add(attid);

                    let filename = '';
                    let mime = '';
                    // No text-regex fallback here either - see the matching comment in
                    // the initial scan above for why (truncates space-containing names).
                    let node = a;
                    for (let i = 0; i < 4 && node; i++) {
                        const raw = node.getAttribute && node.getAttribute('download_url');
                        if (raw) {
                            const dm = raw.match(/^([^:]+):(.+?):(https?:\/\/.+)$/);
                            if (dm) { mime = dm[1]; filename = dm[2]; }
                            break;
                        }
                        node = node.parentElement;
                    }

                    attachments.push({ filename: filename, size: "", url: href, mime: mime });
                });

                return { body: html, attachments: attachments };
            }""")
        found_count = len((result or {}).get("attachments") or [])
        if chip_names and found_count < len(chip_names):
            print(f"[Server] fetch_email_body: list view showed {len(chip_names)} attachment chip(s) "
                  f"for '{message_id}' but the opened view only found {found_count} after retrying.")

        # Match the pre-captured chip filenames positionally with the attachments
        # found in the opened message (the opened view's own markup doesn't
        # reliably expose a clean name across Gmail UI locales).
        if result and result.get("attachments"):
            for i, att in enumerate(result["attachments"]):
                if not att.get("filename") and i < len(chip_names):
                    att["filename"] = chip_names[i]
                if not att.get("filename"):
                    att["filename"] = f"Attachment {i + 1}"

        # Deep scan: download each attachment's raw bytes via the authenticated
        # browser session (its cookies), BEFORE navigating away from the open mail so
        # the download URLs are still valid. base64 so it survives the JSON hop.
        # fetch_bytes_robust falls back to an in-page JS fetch if the plain
        # request-context GET is rejected, instead of just giving up on that
        # attachment (which previously meant it silently never got classified/used).
        if download_bytes and result and result.get("attachments"):
            import base64
            for att in result["attachments"]:
                url = att.get("url")
                if not url:
                    continue
                dl_start = time.time()
                data_bytes, mime = fetch_bytes_robust(page, url, timeout_ms=20000)
                dl_secs = time.time() - dl_start
                if data_bytes is not None:
                    att["data_b64"] = base64.b64encode(data_bytes).decode("ascii")
                    att["mime"] = mime or ""
                    print(f"[Server] [{_elapsed()}] fetch_email_body: downloaded '{att.get('filename')}' "
                          f"({dl_secs:.1f}s).")
                else:
                    print(f"[Server] [{_elapsed()}] Attachment download failed for '{att.get('filename')}' "
                          f"(spent {dl_secs:.1f}s).")

        # Try to navigate back to list view by clicking the back button. Bounded
        # timeout + a goto(_list_view_url_for(page)) fallback if the click itself
        # doesn't land - an unbounded click() here (Playwright's 30s default) sitting
        # right before _restore_unread was previously able to silently eat most of a
        # "hung" request, since nothing distinguished "navigating back is slow" from
        # "stuck".
        back_btn = page.query_selector('div[title="Back to list"], div[aria-label="Back to list"]')
        try:
            if back_btn:
                back_btn.click(timeout=4000)
            else:
                page.goto(_list_view_url_for(page))
        except Exception as e:
            print(f"[Server] fetch_email_body: 'Back to list' click failed ({e}) - falling back to goto(_list_view_url_for(page)).")
            page.goto(_list_view_url_for(page))

        page.wait_for_timeout(500)
        if was_unread:
            restore_start = time.time()
            undo = _restore_unread(page, message_id, subject=subject)
            print(f"[Server] [{_elapsed()}] fetch_email_body: _restore_unread took "
                  f"{time.time() - restore_start:.1f}s (success={undo.get('success')}).")
            if not undo.get("success"):
                print(f"[Server] Could not restore unread status for '{message_id}': {undo.get('error')}")

        print(f"[Server] [{_elapsed()}] fetch_email_body: done for '{message_id}'.")
        return result
    except Exception as e:
        print(f"[Server] Error retrieving email body: {e}")
        return None


def perform_list_action(page, action, message_id):
    """Toggle star, or select the row and click the matching toolbar action
    (mark read/unread, archive/remove-label, delete). Must run on the main thread.

    Uses Playwright's own ElementHandle.click() (real, trusted mouse events) rather
    than a JS-dispatched .click() - some Gmail toolbar buttons (mark as read, in
    particular) silently no-op on synthetic/untrusted clicks even though the
    element and handler are found correctly.

    Every click below passes an explicit short timeout (4s). Without one, a click on
    an element that isn't IMMEDIATELY actionable (Gmail's virtualized list re-rendering
    a row a beat too early/late, a brief animation) falls back to Playwright's own
    default 30s actionability wait - and _restore_unread retries this whole function up
    to 5 times (then 3 more after its subject-search fallback), so a single flaky click
    could previously burn 8+ minutes before giving up. That's long enough to look like
    the automation hung entirely (no scraping happens while this thread is blocked),
    and if it ran out the caller's own timeout before finishing, the mail was left
    stuck marked read - the retry loop's own backoff already handles a fast failure
    fine, so there is no reason for a single click attempt to wait anywhere near 30s."""
    try:
        row_handle = page.evaluate_handle("(data) => {" + _FIND_ROW_JS + """
            return findRowById(data.msgId);
        }""", {"msgId": message_id})
        row_el = row_handle.as_element()
        if row_el is None:
            return {"success": False, "error": "row_not_found"}

        # The row's checkbox replaces the sender avatar/icon only on HOVER (Gmail's
        # standard list behaviour) - it exists in the DOM but is invisible until then,
        # which is exactly what was causing every click here to fail with "element is
        # not visible" after burning its whole timeout (see the repeated ElementHandle.
        # click timeouts in the logs). A real user's mouse would be over the row before
        # clicking its checkbox; hover() reproduces that so the checkbox actually
        # becomes visible/clickable instead of retrying against a hidden element.
        try:
            row_el.hover(timeout=3000)
        except Exception:
            pass

        if action == "toggle_star":
            star_el = row_el.query_selector(".T-KT")
            if not star_el:
                return {"success": False, "error": "star_not_found"}
            star_el.click(timeout=4000)
            return {"success": True}

        checkbox_el = row_el.query_selector('div[role="checkbox"]')
        if not checkbox_el:
            return {"success": False, "error": "checkbox_not_found"}
        try:
            checkbox_el.click(timeout=1500, force=True)
        except Exception:
            page.evaluate("el => el.click()", checkbox_el)
        page.wait_for_timeout(200)

        btn_handle = page.evaluate_handle("(data) => {" + _TOOLBAR_HELPERS_JS + """
            const buttons = getToolbarButtons();
            if (data.action === 'archive') return findArchiveButton(buttons);
            if (data.action === 'delete') return findDeleteButton(buttons);
            if (data.action === 'mark_read') return findMarkReadButton(buttons);
            if (data.action === 'mark_unread') return findMarkUnreadButton(buttons);
            return null;
        }""", {"action": action})
        btn_el = btn_handle.as_element()
        if btn_el is None:
            try:
                checkbox_el.click(timeout=1500, force=True)  # undo selection
            except Exception:
                page.evaluate("el => el.click()", checkbox_el)
            return {"success": False, "error": "toolbar_button_not_found"}
        try:
            btn_el.click(timeout=1500, force=True)
        except Exception:
            page.evaluate("el => el.click()", btn_el)
        page.wait_for_timeout(200)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


def _row_is_unread(page, message_id):
    """True/False if message_id's row is currently rendered and its unread state can
    be read off it, None if the row isn't in the current view at all (can't verify
    either way - caller should not treat that as a failure)."""
    return page.evaluate("(data) => {" + _FIND_ROW_JS + """
        const row = findRowById(data.msgId);
        if (!row) return null;
        return (row.className || '').split(' ').includes('zE');
    }""", {"msgId": message_id})


def _restore_unread(page, message_id, subject=""):
    """Retried, verified restore of a message's unread flag after a
    classification-only open marked it read as a Gmail side effect.

    A single perform_list_action attempt right after navigating back to the list was
    unreliable in practice - the list is often still re-rendering at that moment (a row
    present in one DOM snapshot has its node swapped out by the next), which either
    throws "Element is not attached to the DOM" mid-click or reports "row_not_found"
    even though the row is about to reappear. perform_list_action re-queries the DOM
    from scratch on every call (no handle carried across calls), so simply retrying it
    with a short backoff rides out that re-render window instead of giving up on the
    first pass. Each apparent success is also verified by re-reading the row's own
    unread class - a click that "succeeds" without error but lands a beat too early can
    still leave the row read.

    If the row genuinely isn't in the currently-rendered list (scrolled out of the
    initially-loaded page - the live list only ever holds whatever's presently
    rendered, not a persistent index), falls back to the same subject-search technique
    fetch_email_body itself uses to relocate a row, then returns to the label's list
    view so the caller's next iteration starts from a clean state."""
    def _attempt(retries):
        outcome = {"success": False, "error": "not attempted"}
        for attempt in range(retries):
            outcome = perform_list_action(page, "mark_unread", message_id)
            if outcome.get("success"):
                page.wait_for_timeout(100)
                still_unread = _row_is_unread(page, message_id)
                if still_unread is not False:  # True, or None (can't verify) - accept
                    return outcome
                outcome = {"success": False, "error": "click reported success but row still shows read"}
            page.wait_for_timeout(200)
        return outcome

    undo = _attempt(2)
    if undo.get("success"):
        return undo
    
    if undo.get("error") != "row_not_found" or not subject:
        return undo

    snippet = subject.strip()[:70]
    if not snippet:
        return undo

    print(f"[Server] _restore_unread: row for '{message_id}' not in the current view - "
          f"searching by subject '{snippet}' instead.")
    base_url = LABEL_URL.split("#", 1)[0]
    search_url = f"{base_url}#search/{quote(chr(34) + snippet + chr(34))}"
    page.goto(search_url)
    page.wait_for_timeout(500)
    waited_ms = 0
    while waited_ms < 8000:
        page.wait_for_timeout(500)
        waited_ms += 500
        if _row_exists_by_id(page, message_id):
            break
    undo = _attempt(3)
    page.goto(_list_view_url_for(page))
    page.wait_for_timeout(300)
    return undo


# Gmail's star icon cycles through whichever marker colors are enabled in this
# account's Settings > General > Stars, in a fixed order, on each click - there's no
# direct "set to X" action. English + Dutch keywords to recognise each color in the
# star's title/aria-label/class after a click.
_STAR_COLOR_KEYWORDS = {
    "blue": ["blue", "blauw"],
    "yellow": ["yellow", "geel"],
    "red": ["red", "rood"],
    "orange": ["orange", "oranje"],
    "purple": ["purple", "paars"],
    "green": ["green", "groen"],
}


def classify_star_descriptor(descriptor, star_class=""):
    """Star color for the live scrape loop, so the dashboard reflects whatever color is
    ACTUALLY set in Gmail (including one set by clicking the star directly in this
    browser, not just via set_star_color).

    _YELLOW_STAR_CLASS_TOKEN (confirmed via direct DOM inspection) is the PRIMARY,
    authoritative signal for yellow/plain-starred - trust it first. Title/aria-label
    text is used ONLY to detect an explicitly-named non-yellow color (blue etc.); it
    must NOT fall back to "contains the word star/ster -> assume yellow" - that
    generic fallback previously marked EVERY row as yellow-starred, because this
    build's NOT-starred tooltip also contains "ster" (Dutch: the action-oriented
    phrasing, e.g. "mark with star", not a state description)."""
    if _YELLOW_STAR_CLASS_TOKEN in (star_class or "").split():
        return "yellow"

    d = (descriptor or "").strip().lower()
    if not d:
        return None
    for color, keywords in _STAR_COLOR_KEYWORDS.items():
        if color == "yellow":
            continue
        if any(kw in d for kw in keywords):
            return color
    return None


def _find_star_element(page, message_id):
    """Fresh lookup of message_id's row + star element. Must be re-run before every
    single click in set_star_color's loop below, NOT queried once and reused - Gmail
    re-renders the star element (replacing its DOM node) after each click to reflect
    its new visual state, so a handle from a previous iteration is stale by the time
    the next click fires, throwing "Element is not attached to the DOM"."""
    row_handle = page.evaluate_handle("(data) => {" + _FIND_ROW_JS + """
        return findRowById(data.msgId);
    }""", {"msgId": message_id})
    row_el = row_handle.as_element()
    if row_el is None:
        return None
    # Same hover-to-reveal issue as the row checkbox (see perform_list_action) - some
    # Gmail "Stars" configurations only render the star target on hover, which
    # previously made set_star_color's click retry against an invisible element until
    # it exhausted max_clicks and reported "timed out" without ever actually landing.
    try:
        row_el.hover(timeout=3000)
    except Exception:
        pass
    return row_el.query_selector(".T-KT")


def set_star_color(page, message_id, color, max_clicks=12):
    """Click the row's star repeatedly until it indicates the requested color, up to
    ``max_clicks`` (Gmail's built-in marker set is small, so this comfortably covers a
    full cycle). If that color isn't in this account's enabled set of markers, this
    exhausts its attempts and reports failure rather than clicking forever - the
    caller should check ``success`` and log accordingly, not assume it silently
    worked.

    Re-queries the star element fresh on every attempt (see _find_star_element) -
    reusing one handle across multiple clicks previously threw "Element is not
    attached to the DOM" on the second+ click, since Gmail replaces the star's DOM
    node after each click.

    "yellow" is checked via _YELLOW_STAR_CLASS_TOKEN (the same confirmed, authoritative
    signal classify_star_descriptor uses for reading), NOT the title/aria-label text
    keywords every other color uses - the plain/default star's title/aria-label does
    NOT reliably contain the word "yellow"/"geel" (per classify_star_descriptor's own
    docstring, this build's generic star tooltip text is ambiguous), so text matching
    alone would exhaust every attempt and always report failure for yellow."""
    keywords = _STAR_COLOR_KEYWORDS.get(color, [color])
    try:
        star_el = _find_star_element(page, message_id)
        if star_el is None:
            return {"success": False, "error": "row_not_found_or_star_not_found"}

        # Fast path: check if the star is already the requested color!
        star_class = (star_el.get_attribute("class") or "")
        if color == "yellow" and _YELLOW_STAR_CLASS_TOKEN in star_class.split():
            return {"success": True, "attempts": 0}
        descriptor = " ".join([
            (star_el.get_attribute("title") or ""),
            (star_el.get_attribute("aria-label") or ""),
            star_class,
        ]).lower()
        if color != "yellow" and any(kw in descriptor for kw in keywords):
            return {"success": True, "attempts": 0}

        for attempt in range(max_clicks):
            try:
                star_el.click(timeout=4000)
            except Exception as e:
                print(f"[Server] set_star_color: click attempt {attempt + 1} did not land ({e}) - retrying.")
                page.wait_for_timeout(250)
                star_el = _find_star_element(page, message_id)
                if star_el is None:
                    return {"success": False, "error": "row_not_found_or_star_not_found"}
                continue
            page.wait_for_timeout(250)

            # Re-query before reading state too - the click just fired may itself
            # have already replaced this handle's underlying DOM node.
            star_el = _find_star_element(page, message_id)
            if star_el is None:
                return {"success": False, "error": "row_not_found_or_star_not_found"}

            star_class = (star_el.get_attribute("class") or "")
            if color == "yellow":
                if _YELLOW_STAR_CLASS_TOKEN in star_class.split():
                    return {"success": True, "attempts": attempt + 1}
                continue
            descriptor = " ".join([
                (star_el.get_attribute("title") or ""),
                (star_el.get_attribute("aria-label") or ""),
                star_class,
            ]).lower()
            if any(kw in descriptor for kw in keywords):
                return {"success": True, "attempts": attempt + 1}

        return {
            "success": False,
            "error": f"'{color}' star not reached after {max_clicks} clicks - this account's "
                     f"Gmail Settings > General > Stars may not have that color enabled.",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def _label_forwarded_copy(page, subject_text, add_label, max_wait_s=25):
    """Search the release-orders inbox view (same delegated account, opened lazily by
    _ensure_release_orders_page) for the just-forwarded copy of ``subject_text``, and
    apply add_label to it there. This label ("a-release-orders") lives in THIS view, not
    the a-cmr label the original message came from - it never existed in the a-cmr
    label picker, which is why looking for it there always failed. Must run on the main
    thread; ``page`` is release_orders_page_ref.

    Returns (success, reason) - reason is "" on success, otherwise a specific
    human-readable point of failure (row not found / button not found / etc.), since a
    bare boolean gave no way to diagnose a failure short of reading this process's own
    terminal output. Surfaced up through forward_and_relabel/label_pending_relabel to
    the dashboard."""
    try:
        page.bring_to_front()
        # Dismiss any stray overlay left open on this tab (e.g. Gmail's account-switcher
        # popup) - a generic word-match click from a previous call (see
        # _FORWARD_AND_LABEL_HELPERS_JS's docstring: these selectors are a first pass,
        # not fully confirmed against this build's real DOM) can land on the wrong
        # element and pop one open, which would otherwise swallow every click below.
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(200)
        except Exception:
            pass

        # A forwarded subject is "Fwd: <original subject>" - search on a snippet of the
        # ORIGINAL subject (stable across mail clients' forward-prefix conventions)
        # rather than assuming an exact "Fwd:" prefix. Quoted so Gmail matches it as one
        # exact phrase, not an AND of loose keywords (which risks landing on some other,
        # unrelated forwarded mail that happens to share a few words).
        snippet = (subject_text or "").strip()[:70]
        if not snippet:
            print("[Server] _label_forwarded_copy: no subject snippet to search with.")
            return False, "no subject snippet to search with"
        base_url = RELEASE_ORDERS_INBOX_URL.split("#", 1)[0]
        search_url = f"{base_url}#search/{quote(chr(34) + snippet + chr(34))}"
        page.goto(search_url)

        row = None
        waited = 0
        while waited < max_wait_s:
            row = page.query_selector("tr.zA")
            if row:
                break
            page.wait_for_timeout(1000)
            waited += 1
        if row is None:
            reason = (f"no message found matching subject \"{snippet}\" after {max_wait_s}s - "
                      f"the forward may still be in transit, or landed outside this mailbox's inbox")
            print(f"[Server] _label_forwarded_copy: {reason}")
            return False, reason

        checkbox = row.query_selector('div[role="checkbox"]')
        if not checkbox or not _safe_click(checkbox, label="row checkbox"):
            print("[Server] _label_forwarded_copy: checkbox not found or click did not complete.")
            return False, "row checkbox not found or click did not complete"
        page.wait_for_timeout(300)

        labels_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelsButton();
        }""")
        labels_el = labels_handle.as_element()
        if not _safe_click(labels_el, label="Labels button"):
            print("[Server] _label_forwarded_copy: Labels button not found or click did not complete.")
            return False, "Labels button not found or click did not complete"
        page.wait_for_timeout(500)

        search_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelSearchInput();
        }""")
        search_box = search_handle.as_element()
        if search_box:
            try:
                search_box.click(timeout=4000)
                page.keyboard.press("Control+A")
                page.keyboard.press("Delete")
                search_box.type(add_label, delay=20)
                page.wait_for_timeout(700)
            except Exception as e:
                print(f"[Server] _label_forwarded_copy: label search box interaction failed: {e}")

        option_handle = page.evaluate_handle(
            "(name) => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelOption(name);
        }""", add_label,
        )
        option_el = option_handle.as_element()
        if option_el is None:
            print(f"[Server] _label_forwarded_copy: label option '{add_label}' not found in this mailbox's picker.")
            return False, f"label option '{add_label}' not found in this mailbox's picker"
        if not _safe_click(option_el, label="label option"):
            print(f"[Server] _label_forwarded_copy: label option '{add_label}' click did not complete.")
            return False, f"label option '{add_label}' click did not complete"
        page.wait_for_timeout(300)

        apply_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findApplyButton();
        }""")
        apply_el = apply_handle.as_element()
        # Many pickers apply on option-click alone; a failed/mismatched Apply click
        # isn't fatal here - the option click above is what actually mattered.
        if apply_el is None or not _safe_click(apply_el, label="Apply button"):
            page.keyboard.press("Escape")
        page.wait_for_timeout(500)
        return True, ""
    except Exception as e:
        print(f"[Server] _label_forwarded_copy error: {e}")
        return False, str(e)


def _ensure_release_orders_page(context):
    """Lazily open the release-orders inbox tab (the same delegated account's #inbox
    view, used only to apply the "a-release-orders" label to a just-forwarded copy) the
    FIRST time the forward-and-relabel flow actually needs it. This pipeline's only mail
    tab opened at startup is the a-cmr label view - the inbox view mixes in every other
    kind of mail this shared account receives, so it must never appear unless a forward
    genuinely happened. Safe to call repeatedly - a no-op once the tab is already open."""
    global release_orders_page_ref
    if release_orders_page_ref is None or release_orders_page_ref.is_closed():
        print(f"Opening release-orders inbox on demand: {RELEASE_ORDERS_INBOX_URL}")
        release_orders_page_ref = context.new_page()
        _safe_goto(release_orders_page_ref, RELEASE_ORDERS_INBOX_URL)
    return release_orders_page_ref


def forward_and_relabel(page, message_id, to_email, add_label):
    """Open the mail on ``page`` (the a-cmr label mailbox) and forward it to
    ``to_email``. The label ("a-release-orders") lives in the SAME mailbox but is
    applied to the forwarded copy via release_orders_page_ref (the inbox tab of the
    same account), not to the original. The original message is deliberately left
    exactly as it was (still under a-cmr) - only forwarded, not moved.

    Skips (without opening anything) if this message_id was already forwarded in a
    previous run - see _FORWARDED_TRACKER_PATH. This makes it safe to run "Process
    blue-starred" repeatedly on the same page: already-handled mail is left alone,
    only genuinely new blue-starred mail gets forwarded.

    Must run on the main thread that owns the Playwright sync objects. Triggered by
    scripts/shypple_process.py when a matched shipment has no organization set - see
    _FORWARD_AND_LABEL_HELPERS_JS's docstring: these selectors are a first pass, not
    fully confirmed against this build's real DOM."""
    already = _load_forwarded_ids().get(message_id)
    if already:
        print(f"[Server] forward_and_relabel: '{message_id}' already forwarded on "
              f"{already.get('forwarded_at')} - skipping.")
        return {"success": True, "forwarded": False, "relabeled": False, "skipped": True}

    try:
        click_result = page.evaluate("(data) => {" + _FIND_ROW_JS + """
            const row = findRowById(data.msgId);
            if (!row) return { clicked: false, subject: '' };
            const subjectEl = row.querySelector('span.bog');
            const subject = subjectEl ? subjectEl.innerText.trim() : '';
            row.click();
            return { clicked: true, subject: subject };
        }""", {"msgId": message_id})
        if not click_result.get("clicked"):
            print(f"[Server] forward_and_relabel: could not find row for message id '{message_id}'")
            return {"success": False, "error": "row_not_found"}
        subject_text = click_result.get("subject", "")

        page.wait_for_selector(".a3s", timeout=6000)
        page.wait_for_timeout(500)

        forward_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findForwardButton();
        }""")
        forward_el = forward_handle.as_element()
        if forward_el is None:
            print("[Server] forward_and_relabel: Forward button not found.")
            return {"success": False, "error": "forward_button_not_found"}
        forward_el.click()

        # This mailbox's Gmail UI is Dutch (see memory note) - the recipient field's
        # aria-label is "Aan", not "To". The first attempt only checked English and
        # failed with recipient_field_not_found; this checks both, case-insensitively,
        # and matches on a contained substring rather than a strict prefix.
        to_selector = (
            'div[aria-label*="To" i][contenteditable="true"], '
            'div[aria-label*="Aan" i][contenteditable="true"], '
            'input[aria-label*="To" i], input[aria-label*="Aan" i], '
            'textarea[aria-label*="To" i], textarea[aria-label*="Aan" i], '
            'textarea[name="to"]'
        )
        try:
            to_field = page.wait_for_selector(to_selector, timeout=8000)
        except Exception:
            print("[Server] forward_and_relabel: recipient field not found.")
            return {"success": False, "error": "recipient_field_not_found"}
        to_field.click()
        to_field.type(to_email, delay=20)
        page.keyboard.press("Tab")
        page.wait_for_timeout(300)

        send_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findSendButton();
        }""")
        send_el = send_handle.as_element()
        if send_el is None:
            print("[Server] forward_and_relabel: Send button not found.")
            return {"success": False, "error": "send_button_not_found"}
        send_el.click()
        page.wait_for_timeout(1500)
        forwarded = True

        # Record this BEFORE the relabel step (a separate, independently-retriable
        # operation) - what must never repeat is the actual send, not the labeling.
        _mark_forwarded(message_id, to_email, add_label, subject=subject_text, relabeled=False)

        # Apply the label in the OTHER mailbox, on the just-arrived forwarded copy.
        # Best-effort: if this doesn't land (selector guess miss, copy not arrived yet),
        # relabeled stays False in the tracker so the dashboard's "Label pending
        # release-orders" button can retry it later without re-forwarding.
        relabeled = False
        relabel_error = ""
        if release_orders_page_ref is not None and not release_orders_page_ref.is_closed():
            relabeled, relabel_error = _label_forwarded_copy(release_orders_page_ref, subject_text, add_label)
            if relabeled:
                _mark_relabeled(message_id, True)
        else:
            relabel_error = "release-orders tab unavailable"
            print("[Server] forward_and_relabel: release-orders tab unavailable - skipping remote relabel.")

        # Back to the source label's list - the original is intentionally left
        # untouched (no archive/remove-label here; only forwarded).
        page.bring_to_front()
        back_btn = page.query_selector('div[title="Back to list"], div[aria-label="Back to list"]')
        try:
            if back_btn:
                back_btn.click(timeout=4000)
            else:
                page.goto(_list_view_url_for(page))
        except Exception as e:
            print(f"[Server] forward_and_relabel: 'Back to list' click failed ({e}) - falling back to goto(_list_view_url_for(page)).")
            page.goto(_list_view_url_for(page))

        return {"success": True, "forwarded": forwarded, "relabeled": relabeled,
                "relabel_error": relabel_error, "skipped": False}
    except Exception as e:
        print(f"[Server] forward_and_relabel error: {e}")
        try:
            page.bring_to_front()
            page.goto(_list_view_url_for(page))
        except Exception:
            pass
        return {"success": False, "error": str(e)}


def pending_relabel_summary():
    """Forwarded-but-not-yet-labeled mails from the tracker, grouped by the UTC date
    they were forwarded on - powers the dashboard's "Label pending release-orders"
    button (shown before it retries them). Pure tracker read, no Playwright access
    needed, so this can run directly on the HTTP handler thread."""
    data = _load_forwarded_ids()
    by_day = {}
    for rec in data.values():
        if rec.get("relabeled"):
            continue
        day = (rec.get("forwarded_at") or "")[:10] or "unknown"
        by_day[day] = by_day.get(day, 0) + 1
    return {"success": True, "pending_count": sum(by_day.values()), "by_day": by_day}


def label_pending_relabel(page):
    """Retry _label_forwarded_copy for every forwarded mail in the tracker that isn't
    yet confirmed labeled - the manual catch-up for forward_and_relabel's best-effort
    immediate attempt (which can miss, e.g. if the forwarded copy hadn't arrived in the
    release-orders mailbox yet). Must run on the main thread (release_orders_page_ref)."""
    data = _load_forwarded_ids()
    pending = [(mid, rec) for mid, rec in data.items() if not rec.get("relabeled")]
    if not pending:
        return {"success": True, "found_count": 0, "labeled_count": 0, "failures": []}
    if page is None or page.is_closed():
        return {"success": False, "found_count": len(pending), "labeled_count": 0,
                 "error": "release-orders tab unavailable", "failures": []}

    labeled_count = 0
    failures = []
    for mid, rec in pending:
        subject = rec.get("subject") or ""
        add_label = rec.get("label") or "a-release-orders"
        if not subject:
            failures.append({"message_id": mid, "subject": subject, "error": "no stored subject to search with"})
            continue
        ok, reason = _label_forwarded_copy(page, subject, add_label)
        if ok:
            _mark_relabeled(mid, True)
            labeled_count += 1
        else:
            failures.append({"message_id": mid, "subject": subject, "error": reason or "label option/forwarded copy not found"})

    return {
        "success": labeled_count == len(pending),
        "found_count": len(pending),
        "labeled_count": labeled_count,
        "failures": failures,
    }


def forwarded_mails_list():
    """Every tracked forwarded mail (both labeled and still-pending), most recent
    first - powers the dashboard's forwarded-mails list view, so the user can see
    exactly what was forwarded/labeled and remove stale entries. Pure tracker read, no
    Playwright access needed."""
    data = _load_forwarded_ids()
    items = [{"id": mid, **rec} for mid, rec in data.items()]
    items.sort(key=lambda r: r.get("forwarded_at") or "", reverse=True)
    return {"success": True, "items": items, "count": len(items)}


def delete_forwarded_mail(message_id):
    """Forget one message_id from the forwarded-mail tracker. Does NOT touch Gmail
    itself - the mail that was already forwarded stays exactly as it is - it only
    removes the tracking record, e.g. to clean up a stale/mistaken entry, or to let
    that message be forwarded again on a future run (its "already forwarded" guard is
    gone once the record is gone). Pure tracker write, no Playwright access needed."""
    with _FORWARDED_TRACKER_LOCK:
        data = _load_forwarded_ids()
        if message_id not in data:
            return {"success": False, "error": "not found"}
        del data[message_id]
        _save_forwarded_ids(data)
    return {"success": True}


# The class token confirmed (via direct DOM inspection, before the "blue star" marker
# was ever introduced) to mean this account's plain/default star - i.e. yellow. Unlike
# the Forward/Send/Labels selectors, this one is NOT a guess: it's the same check
# fetch_email_body's scrape loop already uses for the boolean "starred" flag.
_YELLOW_STAR_CLASS_TOKEN = "T-KT-Jp"


_ROW_COLOR_MATCH_JS = """
    function countMatchingRows(yellowToken, keywords) {
        const rows = Array.from(document.querySelectorAll('tr.zA'));
        let count = 0;
        for (const row of rows) {
            const starEl = row.querySelector('.T-KT');
            if (!starEl) continue;
            let matches = false;
            if (yellowToken) {
                const cls = (starEl.className || '').split(/\\s+/);
                matches = cls.includes(yellowToken);
            }
            if (!matches && keywords.length) {
                const descriptor = ((starEl.getAttribute('title') || '') + ' ' + (starEl.getAttribute('aria-label') || '')).toLowerCase();
                matches = keywords.some(kw => descriptor.includes(kw));
            }
            if (matches) count++;
        }
        return count;
    }
"""


def move_starred_to_label(page, target_label, color="yellow"):
    """Select every ``color``-starred row on the CURRENTLY LOADED page of the list
    view, apply target_label to all of them in one go, then remove them from the
    current label (true "Move to" semantics, reusing the same remove-label button as
    forward_and_relabel). Must run on the main thread.

    Yellow is matched via the confirmed class token (_YELLOW_STAR_CLASS_TOKEN); any
    other color is matched via title/aria-label text (_STAR_COLOR_KEYWORDS) - same
    split used by classify_star_descriptor, for the same reason (no confirmed class
    token exists for colors other than yellow).

    Success is verified by OUTCOME, not by trusting each intermediate button-find: it
    re-counts matching rows still in THIS view after the attempt and reports however
    many actually disappeared as moved_count. This is deliberate - an earlier version
    returned "label option not found" and stopped there, even though the label picker's
    search-filter can still apply correctly a beat after that check runs, and the mails
    end up moved despite the reported failure. Every step below still tries its best
    (doesn't bail out early), so a slow/missed detection at one step doesn't block the
    steps after it.

    Only processes what's already rendered - Gmail paginates the list (default 50 per
    page), so if there are more matching mails than fit on one page, either increase
    Settings > General > "Max page size" in Gmail, or run this again after paging
    manually. Pagination-clicking wasn't automated here to avoid stacking yet another
    unconfirmed selector on top of the ones already guessed for Forward/Labels."""
    found_count = 0
    yellow_token = _YELLOW_STAR_CLASS_TOKEN if color == "yellow" else ""
    keywords = [] if color == "yellow" else _STAR_COLOR_KEYWORDS.get(color, [color])
    match_args = {"yellowToken": yellow_token, "keywords": keywords}
    notes = []
    try:
        found_count = page.evaluate(
            """(args) => {
                const rows = Array.from(document.querySelectorAll('tr.zA'));
                let selected = 0;
                for (const row of rows) {
                    const starEl = row.querySelector('.T-KT');
                    if (!starEl) continue;
                    let matches = false;
                    if (args.yellowToken) {
                        const cls = (starEl.className || '').split(/\\s+/);
                        matches = cls.includes(args.yellowToken);
                    }
                    if (!matches && args.keywords.length) {
                        const descriptor = ((starEl.getAttribute('title') || '') + ' ' + (starEl.getAttribute('aria-label') || '')).toLowerCase();
                        matches = args.keywords.some(kw => descriptor.includes(kw));
                    }
                    if (!matches) continue;
                    const checkbox = row.querySelector('div[role="checkbox"]');
                    if (!checkbox) continue;
                    checkbox.click();
                    selected++;
                }
                return selected;
            }""",
            match_args,
        )
        if found_count == 0:
            return {"success": True, "found_count": 0, "moved_count": 0,
                    "message": f"No {color}-starred mails found on the currently loaded page."}

        page.wait_for_timeout(300)

        labels_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelsButton();
        }""")
        labels_el = labels_handle.as_element()
        if labels_el is None:
            notes.append("Labels button not found")
        elif not _safe_click(labels_el, label="Labels button"):
            notes.append("Labels button click did not complete")
        else:
            page.wait_for_timeout(500)

            search_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
                return findLabelSearchInput();
            }""")
            search_box = search_handle.as_element()
            if search_box:
                try:
                    search_box.click(timeout=4000)
                    page.keyboard.press("Control+A")
                    page.keyboard.press("Delete")
                    search_box.type(target_label, delay=20)
                    page.wait_for_timeout(700)
                except Exception as e:
                    notes.append(f"label search box interaction failed: {e}")

            option_handle = page.evaluate_handle(
                "(name) => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
                return findLabelOption(name);
            }""", target_label,
            )
            option_el = option_handle.as_element()
            if option_el is None:
                notes.append(f"label option '{target_label}' not found in picker at check time")
            elif not _safe_click(option_el, label="label option"):
                notes.append(f"label option '{target_label}' click did not complete")
            else:
                page.wait_for_timeout(300)
                apply_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
                    return findApplyButton();
                }""")
                apply_el = apply_handle.as_element()
                if apply_el is not None and not _safe_click(apply_el, label="Apply button"):
                    # Many Gmail label pickers apply on option-click alone, with no
                    # separate Apply step - a generic word match ("apply"/"ok"/"done")
                    # can land on an unrelated, invisible button elsewhere on the page.
                    # Don't treat a failed click here as fatal; Escape just closes
                    # whatever's left open.
                    page.keyboard.press("Escape")
                elif apply_el is None:
                    page.keyboard.press("Escape")
                page.wait_for_timeout(500)

        # Remove the current label regardless of whether the steps above were all
        # detected - reuses the same button perform_list_action's "archive" action
        # already uses, which in a custom-label view removes just that label.
        btn_handle = page.evaluate_handle("() => {" + _TOOLBAR_HELPERS_JS + """
            return findArchiveButton(getToolbarButtons());
        }""")
        btn_el = btn_handle.as_element()
        if btn_el is None:
            notes.append("remove-current-label button not found")
        elif not _safe_click(btn_el, label="remove-current-label button"):
            notes.append("remove-current-label button click did not complete")
        else:
            page.wait_for_timeout(800)

        # Verify by outcome: how many color-starred rows are STILL in this view now?
        # (Starring itself is untouched by a label move, so any that are still here
        # genuinely weren't moved - this isn't confused by the star surviving.)
        remaining = page.evaluate("(args) => {" + _ROW_COLOR_MATCH_JS + """
            return countMatchingRows(args.yellowToken, args.keywords);
        }""", match_args)
        moved_count = max(0, found_count - remaining)

        if moved_count >= found_count:
            return {"success": True, "found_count": found_count, "moved_count": found_count}

        note_str = "; ".join(notes) if notes else "no specific step failed, but some mail(s) are still in this view"
        return {
            "success": False, "found_count": found_count, "moved_count": moved_count,
            "error": f"Only {moved_count}/{found_count} confirmed moved (re-checked the list afterward). {note_str}",
        }
    except Exception as e:
        print(f"[Server] move_starred_to_label error: {e}")
        return {"success": False, "found_count": found_count, "moved_count": 0, "error": str(e)}


def process_color_starred_forward(page, color, to_email, add_label):
    """Find every ``color``-starred mail on the CURRENTLY LOADED page of the list view,
    and for each one forward it to to_email then move it to add_label - one call to
    forward_and_relabel per mail (unlike move_starred_to_label's single bulk action,
    this can't be batched: Gmail's Forward requires the message to be open). This is
    exactly the same action the Shypple automation takes automatically for an empty-
    organization shipment (see shypple_process.py's _forward_and_relabel_source_email),
    exposed here as a standalone bulk "catch up on any mail already starred this color"
    button. Must run on the main thread."""
    keywords = _STAR_COLOR_KEYWORDS.get(color, [color])
    try:
        message_ids = page.evaluate(
            """(keywords) => {
                const rows = Array.from(document.querySelectorAll('tr.zA'));
                const ids = [];
                for (const row of rows) {
                    const starEl = row.querySelector('.T-KT');
                    if (!starEl) continue;
                    const descriptor = ((starEl.getAttribute('title') || '') + ' ' + (starEl.getAttribute('aria-label') || '')).toLowerCase();
                    if (!keywords.some(kw => descriptor.includes(kw))) continue;
                    const idEl = row.querySelector('[data-legacy-last-message-id]');
                    const legacyId = idEl ? idEl.getAttribute('data-legacy-last-message-id') : null;
                    if (legacyId) ids.push(legacyId);
                }
                return ids;
            }""",
            keywords,
        )
    except Exception as e:
        print(f"[Server] process_color_starred_forward scan error: {e}")
        return {"success": False, "found_count": 0, "moved_count": 0, "error": str(e), "failures": []}

    found_count = len(message_ids)
    if found_count == 0:
        return {"success": True, "found_count": 0, "moved_count": 0,
                "message": f"No {color}-starred mails found on the currently loaded page."}

    moved_count = 0
    skipped_count = 0
    failures = []
    for mid in message_ids:
        result = forward_and_relabel(page, mid, to_email, add_label)
        if result.get("skipped"):
            skipped_count += 1
        elif result.get("success"):
            moved_count += 1
        else:
            failures.append({"message_id": mid, "error": result.get("error", "unknown error")})

    return {
        "success": (moved_count + skipped_count) == found_count,
        "found_count": found_count,
        "moved_count": moved_count,
        "skipped_count": skipped_count,
        "failures": failures,
    }


def mark_unread_and_move_to_label(page, message_id, target_label):
    """Mark one specific email unread and move it to target_label (true "Move to" -
    removes the current label too). Triggered by the Shypple automation when a
    container search comes back with a genuinely empty results table (not just an
    org/ETA mismatch) - the shipment isn't in Shypple at all, so the email gets
    flagged unread and filed under target_label for manual follow-up. Must run on the
    main thread."""
    try:
        unread_result = perform_list_action(page, "mark_unread", message_id)
        if not unread_result.get("success"):
            print(f"[Server] mark_unread_and_move_to_label: mark_unread failed: {unread_result.get('error')}")

        # perform_list_action's checkbox selection doesn't persist after its own
        # toolbar click - re-select the row for the label move.
        row_handle = page.evaluate_handle("(data) => {" + _FIND_ROW_JS + """
            return findRowById(data.msgId);
        }""", {"msgId": message_id})
        row_el = row_handle.as_element()
        if row_el is None:
            return {"success": False, "error": "row_not_found"}
        checkbox_el = row_el.query_selector('div[role="checkbox"]')
        if not checkbox_el or not _safe_click(checkbox_el, label="row checkbox"):
            return {"success": False, "error": "checkbox_not_found_or_click_failed"}
        page.wait_for_timeout(300)

        labels_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelsButton();
        }""")
        labels_el = labels_handle.as_element()
        if not _safe_click(labels_el, label="Labels button"):
            return {"success": False, "error": "labels_button_not_found_or_click_failed"}
        page.wait_for_timeout(500)

        search_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelSearchInput();
        }""")
        search_box = search_handle.as_element()
        if search_box:
            try:
                search_box.click(timeout=4000)
                page.keyboard.press("Control+A")
                page.keyboard.press("Delete")
                search_box.type(target_label, delay=20)
                page.wait_for_timeout(700)
            except Exception as e:
                print(f"[Server] mark_unread_and_move_to_label: search box interaction failed: {e}")

        option_handle = page.evaluate_handle(
            "(name) => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findLabelOption(name);
        }""", target_label,
        )
        option_el = option_handle.as_element()
        if option_el is None:
            return {"success": False, "error": f"label option '{target_label}' not found in picker"}
        if not _safe_click(option_el, label="label option"):
            return {"success": False, "error": f"label option '{target_label}' click did not complete"}
        page.wait_for_timeout(300)

        apply_handle = page.evaluate_handle("() => {" + _FORWARD_AND_LABEL_HELPERS_JS + """
            return findApplyButton();
        }""")
        apply_el = apply_handle.as_element()
        if apply_el is None or not _safe_click(apply_el, label="Apply button"):
            page.keyboard.press("Escape")  # many pickers apply on option-click alone
        page.wait_for_timeout(500)

        btn_handle = page.evaluate_handle("() => {" + _TOOLBAR_HELPERS_JS + """
            return findArchiveButton(getToolbarButtons());
        }""")
        btn_el = btn_handle.as_element()
        if btn_el is None or not _safe_click(btn_el, label="remove-current-label button"):
            return {"success": False, "error": "remove-current-label button not found or click did not complete"}
        page.wait_for_timeout(800)

        return {"success": True}
    except Exception as e:
        print(f"[Server] mark_unread_and_move_to_label error: {e}")
        return {"success": False, "error": str(e)}


class PlaywrightControlServer(BaseHTTPRequestHandler):
    def do_GET(self):
        # Disable logging for cleaner terminal output
        self.log_message = lambda format, *args: None

        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        message_id = query.get("id", [""])[0].strip()

        if parsed.path == "/health":
            try:
                file_mtime = os.path.getmtime(_SCRIPT_PATH)
            except OSError:
                file_mtime = None
            stale = file_mtime is not None and _SCRIPT_STARTED_AT < file_mtime
            result = {
                "success": True,
                "pid": os.getpid(),
                "started_at": _SCRIPT_STARTED_AT,
                "script_mtime": file_mtime,
                "stale": stale,
                "active_label": active_label,
                "current_url": gmail_page_ref.url if gmail_page_ref else None,
                "lcl_url": lcl_page_ref.url if lcl_page_ref else None,
                "lcl_message_count": len(_lcl_message_ids),
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/get_labels":
            req = ActionRequest("get_labels", "")
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=10)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/switch_label":
            target_lbl = query.get("label", ["a-cmr"])[0].strip() or "a-cmr"
            print(f"[Server] Request to switch label -> '{target_lbl}'")
            req = ActionRequest("switch_label", "", label_name=target_lbl)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=20)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/get_body":
            print(f"[Server] Request to get body for message id: '{message_id}'")
            req = ActionRequest("get_body", message_id)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=25)
            result = req.result if fulfilled else None

            if result is not None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode("utf-8"))
                return

            self.send_response(500)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"Failed to retrieve email body.")
            return

        if parsed.path == "/get_documents":
            doc_subject = query.get("subject", [""])[0]
            print(f"[Server] Request to get body + attachment bytes for message id: '{message_id}'")
            req = ActionRequest("get_documents", message_id, subject=doc_subject)
            request_queue.put(req)
            # A real (non-attachment-heavy) deep classification measured at ~48s
            # end-to-end, right at the old 45s ceiling here - this widens the margin so
            # a normal-but-slow request doesn't get cut off at the edge; the caller's
            # own urlopen timeout (document_classifier.py) is widened to match.
            #
            # A 2-attachment email adds a second sequential fetch_bytes_robust download
            # (up to 20s each) plus, if the row scrolled out of the loaded list or
            # _restore_unread needed its subject-search fallback, another ~20-30s on
            # top of that - measured live at ~90s total, right at the old 90s ceiling
            # here, which cut the request off and threw away an otherwise-successful
            # multi-attachment read (silently falling back to the single-attachment
            # metadata guess upstream). Widened again, same reasoning as above.
            fulfilled = req.event.wait(timeout=150)
            result = req.result if fulfilled else None

            if result is not None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode("utf-8"))
                return

            self.send_response(500)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"Failed to retrieve email documents.")
            return

        if parsed.path == "/action":
            action = query.get("type", [""])[0].strip()
            print(f"[Server] Request to perform action '{action}' for message id: '{message_id}'")
            req = ActionRequest(action, message_id)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=25)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/forward_and_relabel":
            to_email = query.get("to", [""])[0].strip()
            add_label = query.get("label", [""])[0].strip()
            print(f"[Server] Request to forward+relabel message id: '{message_id}' -> {to_email}, label={add_label}")
            req = ActionRequest("forward_and_relabel", message_id, to_email=to_email, add_label=add_label)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=40)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/star_color":
            color = query.get("color", [""])[0].strip()
            print(f"[Server] Request to star (color='{color}') message id: '{message_id}'")
            req = ActionRequest("star_color", message_id, color=color)
            request_queue.put(req)
            # set_star_color retries up to 12 clicks (each bounded at 4s) to cycle
            # through Gmail's marker colors - worst case ~50s+, well past the old 20s
            # wait here, which is exactly why shypple_process.py logged "Could not
            # reach the Gmail automation to set the star: timed out" even though the
            # click loop was very likely still legitimately working.
            fulfilled = req.event.wait(timeout=70)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/move_starred_to_label":
            target_label = query.get("label", [""])[0].strip()
            color = query.get("color", ["yellow"])[0].strip() or "yellow"
            print(f"[Server] Request to move {color}-starred mails to label:{target_label}")
            req = ActionRequest("move_starred_to_label", "", target_label=target_label, color=color)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=45)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/process_color_starred_forward":
            color = query.get("color", [""])[0].strip()
            to_email = query.get("to", [""])[0].strip()
            add_label = query.get("label", [""])[0].strip()
            print(f"[Server] Request to forward+relabel all {color}-starred mails -> {to_email}, label={add_label}")
            req = ActionRequest("process_color_starred_forward", "", color=color, to_email=to_email, add_label=add_label)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=120)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/mark_unread_and_move_to_label":
            target_label = query.get("label", [""])[0].strip()
            print(f"[Server] Request to mark unread + move message id '{message_id}' to label:{target_label}")
            req = ActionRequest("mark_unread_and_move_to_label", message_id, target_label=target_label)
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=30)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/pending_relabel_summary":
            # Pure tracker read - no Playwright access needed, so answer directly on
            # this handler thread rather than routing through request_queue.
            result = pending_relabel_summary()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/label_pending_relabel":
            print("[Server] Request to label pending (forwarded but not yet labeled) release-order mails")
            req = ActionRequest("label_pending_relabel", "")
            request_queue.put(req)
            fulfilled = req.event.wait(timeout=120)
            result = req.result if fulfilled else {"success": False, "error": "timeout"}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/forwarded_mails_list":
            # Pure tracker read - no Playwright access needed.
            result = forwarded_mails_list()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        if parsed.path == "/delete_forwarded_mail":
            # Pure tracker write - no Playwright access needed.
            result = delete_forwarded_mail(message_id) if message_id else {"success": False, "error": "id is required"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


def _resolve_action_page(message_id):
    """Which tab a per-message action (star, mark read/unread, fetch body/documents,
    forward) should run against - lcl_page_ref if this id was last seen scraped under
    LCL_LABEL_KEY, otherwise gmail_page_ref (covers a-cmr and anything not yet
    classified either way, i.e. the exact pre-dual-tab default). message_id here is the
    RAW id (no "pw_" prefix) as used by ActionRequest; _lcl_message_ids stores the full
    "pw_<raw>" form written by do_scrape_emails, so it's re-prefixed for the lookup."""
    full_id = f"pw_{message_id}" if not message_id.startswith("pw_") else message_id
    with _lcl_message_ids_lock:
        is_lcl = full_id in _lcl_message_ids
    if is_lcl and lcl_page_ref is not None and not lcl_page_ref.is_closed():
        return lcl_page_ref
    return gmail_page_ref


def do_scrape_emails(page, force_write=False, label=None, output_path=None):
    """Scrape ``page``'s currently-visible mail list. ``label``/``output_path`` default
    to the global active_label / the primary data/scraped_emails.json (unchanged
    behavior for every pre-existing call site) - the LCL tab's periodic scrape passes
    both explicitly (LCL_LABEL_KEY / _LCL_SCRAPED_PATH) so it tags its own rows and
    writes its own file without touching active_label at all."""
    if page is None or page.is_closed():
        return []
    effective_label = label or active_label
    effective_path = output_path or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "scraped_emails.json")
    )
    try:
        js_scrape = """() => {
            const rows = Array.from(document.querySelectorAll('tr.zA'));
            return rows.map(row => {
                const senderEl = row.querySelector('span.yP, span.zF, td.yX, span.bA4');
                const sender = senderEl ? senderEl.innerText.trim() : 'Unknown';

                const subjectEl = row.querySelector('span.bog');
                const subject = subjectEl ? subjectEl.innerText.trim() : 'No Subject';

                const snippetEl = row.querySelector('span.y2');
                let snippet = snippetEl ? snippetEl.innerText.trim() : '';
                if (snippet.startsWith('- ') || snippet.startsWith('— ')) snippet = snippet.slice(2).trim();

                const dateEl = row.querySelector('td.xW span');
                const date = dateEl ? dateEl.innerText.trim() : '';

                const isUnread = row.classList.contains('zE');

                // Yellow is matched via the confirmed class token ('T-KT-Jp', same as
                // _YELLOW_STAR_CLASS_TOKEN/classify_star_descriptor elsewhere in this
                // file) - NOT text, since the plain star's title/aria-label doesn't
                // reliably say "yellow". Other colors are matched via word-boundary-safe
                // regex against the title/aria-label text, NOT plain .includes() - a
                // naive substring check previously matched "red" against every row's
                // generic "(Not) starred" tooltip (the word "starred" itself contains
                // "red" - "star-RED"), which is why every row was showing as red
                // regardless of its actual color or even whether it was starred at all.
                const starEl = row.querySelector('.T-KT');
                let starColor = null;
                if (starEl) {
                    const clsTokens = (starEl.getAttribute('class') || '').split(/\\s+/);
                    const descriptor = ((starEl.getAttribute('title') || '') + ' ' + (starEl.getAttribute('aria-label') || '')).toLowerCase();
                    if (clsTokens.includes('T-KT-Jp')) starColor = 'yellow';
                    else if (/\\bblue\\b|\\bblauw\\b/.test(descriptor)) starColor = 'blue';
                    else if (/\\bred\\b|\\brood\\b/.test(descriptor)) starColor = 'red';
                    else if (/\\borange\\b|\\boranje\\b/.test(descriptor)) starColor = 'orange';
                    else if (/\\bgreen\\b|\\bgroen\\b/.test(descriptor)) starColor = 'green';
                    else if (/\\bpurple\\b|\\bpaars\\b/.test(descriptor)) starColor = 'purple';
                }

                const idEl = row.querySelector('[data-legacy-last-message-id]');
                const legacyId = idEl ? idEl.getAttribute('data-legacy-last-message-id') : null;

                const names = [];
                const children = Array.from(row.querySelectorAll('*'));
                for (const c of children) {
                    const txt = (
                        c.getAttribute('title') ||
                        c.getAttribute('data-tooltip') ||
                        c.getAttribute('aria-label') ||
                        (c.children.length === 0 ? c.innerText : '') || ''
                    ).trim();
                    if (txt && /\\.(pdf|docx?|xlsx?|png|jpe?g|txt|zip|rar|csv)\\b/i.test(txt)) {
                        if (!names.includes(txt) && txt.length < 150) names.push(txt);
                    }
                }
                const hasAtt = names.length > 0 || !!row.querySelector('img.yE[title="Has attachment"], img.yE[alt="Has attachment"]');

                return {
                    legacyId,
                    sender,
                    subject,
                    snippet,
                    date,
                    unread: isUnread,
                    starred: starColor !== null,
                    starColor,
                    hasAttachment: hasAtt,
                    attachmentNames: names
                };
            });
        }"""
        
        raw_rows = page.evaluate(js_scrape)
        if not raw_rows:
            curr_target = get_label_url(effective_label)
            lbl_key, lbl_gmail = normalize_label(effective_label)
            url_lower = (page.url or "").lower()
            match_ok = ("cmr" in url_lower) if lbl_key == "a-cmr" else ("lcl" in url_lower or "arrival" in url_lower or "release" in url_lower)
            if not match_ok:
                page.goto(curr_target)
            page.wait_for_timeout(800)
            raw_rows = page.evaluate(js_scrape)

        emails = []
        for item in (raw_rows or []):
            legacy_id = item.get("legacyId")
            if legacy_id:
                email_id = f"pw_{legacy_id}"
            else:
                unique_str = f"{item['sender']}-{item['subject']}-{item['date']}"
                email_id = f"pw_{hashlib.md5(unique_str.encode('utf-8')).hexdigest()}"

            emails.append({
                "id": email_id,
                "subject": item["subject"],
                "from": item["sender"],
                "date": item["date"],
                "snippet": item["snippet"],
                "unread": item["unread"],
                "starred": item["starred"],
                "starColor": item["starColor"],
                "hasAttachment": item["hasAttachment"],
                "attachmentNames": item["attachmentNames"],
                "label": effective_label
            })

        if effective_label == LCL_LABEL_KEY:
            with _lcl_message_ids_lock:
                _lcl_message_ids.clear()
                _lcl_message_ids.update(e["id"] for e in emails)

        if emails or force_write:
            with open(effective_path, "w", encoding="utf-8") as f:
                json.dump(emails, f)
            print(f"[Scrape] Saved {len(emails)} emails for label '{effective_label}'")
        return emails
    except Exception as e:
        print(f"[Scrape] Error in do_scrape_emails: {e}")
        return []


def start_http_server():
    server = HTTPServer(("127.0.0.1", 40005), PlaywrightControlServer)
    print("Playwright control server running on http://127.0.0.1:40005")
    server.serve_forever()


def main():
    global gmail_page_ref, release_orders_page_ref, lcl_page_ref, active_label
    user_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "playwright_profile"))
    os.makedirs(user_data_dir, exist_ok=True)

    # Purge Chrome's HTTP caches for this persistent profile before launch. Otherwise a
    # previously cached copy of the dashboard *shell* is served on navigation - the mail
    # list refreshes via AJAX and masks it, so UI changes to the static page (e.g. new
    # toolbar buttons) appear "missing" until the cache is cleared. Safe: only cached
    # responses are removed, never cookies/login. Runs while Chrome is not yet started,
    # so the files aren't locked.
    #
    # Deliberately does NOT touch "Service Worker" (unlike an earlier version). This is
    # one shared profile directory for every origin in it, including mail.google.com -
    # wiping Gmail's own service-worker/session state right as two tabs simultaneously
    # re-establish Gmail sessions on the very next launch is a plausible trigger for
    # Gmail's "Temporary Error (403)" page (reproduced: both Gmail tabs 403'd
    # immediately on a fresh launch right after this wipe). The dashboard_url's own
    # "?t=<timestamp>" cache-buster (below) already solves the original stale-shell
    # problem non-destructively, so this wipe is now redundant for that purpose and not
    # worth the collateral risk to Gmail's session state.
    import shutil
    for sub in ("Cache", "Code Cache", "GPUCache"):
        try:
            shutil.rmtree(os.path.join(user_data_dir, "Default", sub), ignore_errors=True)
        except Exception:
            pass

    server_thread = threading.Thread(target=start_http_server, daemon=True)
    server_thread.start()

    print(f"Launching Playwright with persistent context in: {user_data_dir}")

    downloads_dir = get_downloads_dir()

    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                channel="chrome",
                args=["--start-maximized"],
                no_viewport=True,
                accept_downloads=True,
                downloads_path=downloads_dir
            )
        except Exception as e:
            print(f"Could not launch with Chrome channel: {e}. Falling back to default chromium.")
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,
                args=["--start-maximized"],
                no_viewport=True,
                accept_downloads=True,
                downloads_path=downloads_dir
            )

        def _handle_browser_download(download):
            try:
                orig_name = download.suggested_filename or "shypple_document.pdf"
                if not os.path.splitext(orig_name)[1]:
                    orig_name = orig_name + ".pdf"
                target_path = os.path.join(downloads_dir, orig_name)
                download.save_as(target_path)
                print(f"[Browser Download] Saved downloaded PDF to: {target_path}")
            except Exception as dl_err:
                print(f"[Browser Download] Error saving download: {dl_err}")

        context.on("download", _handle_browser_download)

        page = context.pages[0] if context.pages else context.new_page()

        # Cache-buster query so Chrome's persistent-profile disk cache can never serve a
        # stale copy of the dashboard after a UI change (buttons/layout "missing" bug).
        dashboard_url = f"http://localhost:40000/?t={int(time.time())}"
        print(f"Navigating to local UI: {dashboard_url}")
        _safe_goto(page, dashboard_url)

        gmail_page_ref = context.new_page()
        _ensure_gmail_logged_in(gmail_page_ref)
        print(f"Navigating to Gmail a-cmr label: {LABEL_URL}")
        _safe_goto(gmail_page_ref, LABEL_URL)

        # A second, permanently-open tab pinned to LCL Arrivals/Release - same logged-in
        # session (shared cookies via the same persistent context), so no separate login
        # check is needed here. Both tabs now stay live simultaneously; no per-request
        # label switch is needed for either one - see do_scrape_emails' label/output_path
        # params and _resolve_action_page above.
        lcl_page_ref = context.new_page()
        lcl_label_url = get_label_url(LCL_LABEL_KEY)
        print(f"Navigating to Gmail LCL Arrivals/Release label: {lcl_label_url}")
        _safe_goto(lcl_page_ref, lcl_label_url)

        # The release-orders inbox tab is intentionally NOT opened here - it opens
        # lazily, only once a forward-and-relabel actually happens - see
        # _ensure_release_orders_page.

        print("Playwright browser window opened with Flask UI, Gmail (a-cmr label), and "
              "Gmail (LCL Arrivals/Release label). Feel free to log in. Close the browser window to finish.")

        # Main loop: services requests from the HTTP thread (must run here, on the
        # thread that owns the Playwright sync objects) and periodically re-scrapes
        # the visible email list for the dashboard's live view.
        last_scrape = 0
        try:
            while len(context.pages) > 0:
                # Drain any pending requests immediately
                while True:
                    try:
                        req = request_queue.get_nowait()
                    except queue.Empty:
                        break
                    # A single bad request (e.g. a click that triggers navigation)
                    # must never take down the whole automation loop.
                    try:
                        if gmail_page_ref and not gmail_page_ref.is_closed():
                            if req.action == "get_body":
                                req.result = fetch_email_body(_resolve_action_page(req.message_id), req.message_id)
                            elif req.action == "get_documents":
                                req.result = fetch_email_body(
                                    _resolve_action_page(req.message_id), req.message_id, download_bytes=True,
                                    subject=getattr(req, "subject", ""),
                                )
                            elif req.action == "forward_and_relabel":
                                _ensure_release_orders_page(context)
                                req.result = forward_and_relabel(
                                    _resolve_action_page(req.message_id), req.message_id, req.to_email, req.add_label
                                )
                            elif req.action == "star_color":
                                req.result = set_star_color(_resolve_action_page(req.message_id), req.message_id, req.color)
                            elif req.action == "move_starred_to_label":
                                req.result = move_starred_to_label(gmail_page_ref, req.target_label, req.color)
                            elif req.action == "process_color_starred_forward":
                                _ensure_release_orders_page(context)
                                req.result = process_color_starred_forward(
                                    gmail_page_ref, req.color, req.to_email, req.add_label
                                )
                            elif req.action == "mark_unread_and_move_to_label":
                                req.result = mark_unread_and_move_to_label(
                                    _resolve_action_page(req.message_id), req.message_id, req.target_label
                                )
                            elif req.action == "get_labels":
                                 js = """() => {
                                     const links = Array.from(document.querySelectorAll('a[title], a[aria-label], div.n2, span.n4'));
                                     return links.map(el => (el.getAttribute('title') || el.getAttribute('aria-label') || el.innerText || '').trim()).filter(Boolean);
                                 }"""
                                 labels = gmail_page_ref.evaluate(js) if gmail_page_ref else []
                                 req.result = {"success": True, "url": gmail_page_ref.url if gmail_page_ref else "", "active_label": active_label, "labels": labels}
                            elif req.action == "switch_label":
                                raw_lbl = getattr(req, "label_name", "a-cmr")
                                lbl_key, lbl_gmail = normalize_label(raw_lbl)
                                active_label = lbl_key
                                target_url = get_label_url(active_label)
                                print(f"[Browser Navigation] Switching to label: key='{lbl_key}', gmail='{lbl_gmail}', url='{target_url}'")
                                
                                if gmail_page_ref and not gmail_page_ref.is_closed():
                                    try:
                                        # Try JS click on matching sidebar item first
                                        js_click = """(targetText) => {
                                            const allEls = Array.from(document.querySelectorAll('a[href*="#label/"], a[title], a[aria-label], [role="navigation"] a, [role="navigation"] div, div.n2, span.n4'));
                                            const needle = targetText.toLowerCase();
                                            for (const el of allEls) {
                                                const txt = (el.getAttribute('title') || el.getAttribute('aria-label') || el.getAttribute('href') || el.innerText || '').toLowerCase();
                                                if (txt.includes(needle)) {
                                                    const clickable = el.closest('a') || el.closest('[role="link"]') || el;
                                                    clickable.click();
                                                    return true;
                                                }
                                            }
                                            return false;
                                        }"""
                                        search_needle = "lcl" if "lcl" in lbl_key.lower() else ("cmr" if "cmr" in lbl_key.lower() else lbl_gmail)
                                        gmail_page_ref.evaluate(js_click, search_needle)
                                    except Exception as e:
                                        print(f"[Browser Navigation] Sidebar click failed: {e}")

                                    # Always ensure browser page URL matches target_url
                                    if target_url not in gmail_page_ref.url:
                                        _safe_goto(gmail_page_ref, target_url)

                                gmail_page_ref.wait_for_timeout(1500)
                                _row_attachment_cache.clear()
                                scraped_list = do_scrape_emails(gmail_page_ref, force_write=True)
                                print(f"[Browser Navigation] Current URL after navigation: {gmail_page_ref.url}, scraped {len(scraped_list)} mails")
                                req.result = {"success": True, "label": active_label, "count": len(scraped_list), "url": gmail_page_ref.url}
                            elif req.action == "label_pending_relabel":
                                req.result = label_pending_relabel(_ensure_release_orders_page(context))
                            else:
                                # Catch-all: toggle_star, mark_read, mark_unread, archive,
                                # delete - exactly the actions
                                # scripts/shypple_process.py's _star_source_email (toggle
                                # path)/_mark_source_email_read/_mark_source_email_unread
                                # use, for EITHER pipeline's messages.
                                req.result = perform_list_action(
                                    _resolve_action_page(req.message_id), req.action, req.message_id
                                )
                    except Exception as e:
                        print(f"[Server] Unhandled error processing request: {e}")
                        req.result = None
                    req.event.set()
                    try:
                        scraped_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "scraped_emails.json"))
                        if os.path.exists(scraped_path):
                            os.utime(scraped_path, None)
                    except Exception:
                        pass

                now = time.time()
                if now - last_scrape >= 2:
                    last_scrape = now
                    if gmail_page_ref is None or gmail_page_ref.is_closed():
                        print("[Scrape] Skipped a-cmr: the Gmail tab is missing or closed.")
                    else:
                        do_scrape_emails(gmail_page_ref)
                    if lcl_page_ref is None or lcl_page_ref.is_closed():
                        print("[Scrape] Skipped LCL Arrivals/Release: the tab is missing or closed.")
                    else:
                        do_scrape_emails(
                            lcl_page_ref, force_write=True, label=LCL_LABEL_KEY, output_path=_LCL_SCRAPED_PATH
                        )

                page.wait_for_timeout(200)
        except Exception as e:
            print(f"[Main loop] Unexpected error, shutting down: {e}")
        finally:
            context.close()
            print("Playwright browser context closed.")


if __name__ == "__main__":
    main()
