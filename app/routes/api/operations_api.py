import io
import os
import re
import sys
import json
import time
import base64
import mimetypes
import threading
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from flask import Blueprint, jsonify, request, current_app, send_file, send_from_directory

from app.services.parsing.document_classifier import (
    resolve_deep_classification, fetch_document_bytes, compare_document_versions, DOCUMENT_TYPES,
    _load_cache, classify_email_meta, extract_sf_number,
)
from app.utils.system_paths import get_downloads_dir

operations_api_bp = Blueprint("operations_api", __name__)

# The Shypple automation browser (scripts/shypple_process.py) runs its own HTTP control
# server on this port, mirroring how scripts/open_gmail.py exposes one on 40005.
_CONTROL_SERVER = "http://127.0.0.1:40006"

# scripts/open_gmail.py's own control server (a separate automation browser).
_GMAIL_CONTROL_SERVER = "http://127.0.0.1:40005"
# The Labels picker shows a label's real display name ("Processed - India filing", a
# sub-label of "_0 India filing"), NOT the hyphenated "label:" search-box slug
# ("_0-india-filing-processed---india-filing") - using the slug here is what made the
# picker search never find a match, even though the label already exists in Gmail.
_DEFAULT_YELLOW_STAR_LABEL = "Processed - India filing"
_DEFAULT_PURPLE_STAR_LABEL = "_0 India shipments"
# Same target this project's Shypple automation already forwards no-organization
# shipments to (shypple_process.py's FORWARD_NO_ORG_TO / FORWARD_NO_ORG_LABEL) - kept
# in sync here since this button is the standalone/manual equivalent of that action.
_DEFAULT_BLUE_FORWARD_TO = "nl.importsea@shypple.com"
_DEFAULT_BLUE_FORWARD_LABEL = "a-release-orders"

_SCRAPED_EMAILS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails.json")
)
# scripts/open_gmail.py's dedicated, permanently-open second tab for this label writes
# here (see lcl_page_ref/LCL_LABEL_KEY there) - separate from _SCRAPED_EMAILS_PATH,
# which only ever holds a-cmr mail.
_LCL_SCRAPED_EMAILS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails_lcl.json")
)
# Matches tracking_api.py's _LCL_LABEL_KEY / document_classifier.py's
# _LCL_ARRIVALS_LABEL - selects the lcl-arrivals label override (Arrival notice/
# Delivery order/Other only) in classify_email_meta/resolve_deep_classification/
# fetch_document_bytes, instead of their generic full-CMR-taxonomy branch.
_LCL_LABEL_KEY = "lcl-arrivals---release"
# scripts/shypple_process.py's save_document_locally() saves already-uploaded Shypple
# documents here (under their real filename) - the reliable local alternative to
# clicking a document link directly in Shypple's own admin UI, which produces an
# extension-less UUID-named file Chrome can't open. The current machine/user's real
# Downloads folder (see get_downloads_dir) - never hardcoded, since this project runs
# on different company machines/accounts.
_DOWNLOADS_DIR = get_downloads_dir()

# Cold-starting the persistent Chrome profile (first launch, or after a machine reboot)
# can take a while - guards against two near-simultaneous requests (e.g. a double click
# on Process) both spawning their own browser instance.
_LAUNCH_LOCK = threading.Lock()


def _control_server_reachable(timeout=2):
    try:
        with urllib.request.urlopen(f"{_CONTROL_SERVER}/status", timeout=timeout):
            return True
    except Exception:
        return False


def _ensure_shypple_browser_running(timeout_s=45):
    """Launch scripts/shypple_process.py if its control server isn't reachable yet, and
    wait (polling) for it to come up - Playwright's first Chrome launch on a persistent
    profile can take several seconds, and this is what previously caused "connection
    actively refused" errors when Process was clicked before/without a separate manual
    Launch step. Returns True once reachable, False on timeout."""
    if _control_server_reachable():
        return True

    with _LAUNCH_LOCK:
        if _control_server_reachable():  # re-check - another request may have launched it
            return True
        python_exe = sys.executable
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts", "shypple_process.py")
        )
        subprocess.Popen([python_exe, script_path])

    waited = 0
    while waited < timeout_s:
        time.sleep(1)
        waited += 1
        if _control_server_reachable():
            return True
    return False


def _save_document_copy(data_bytes, filename, doc_type):
    """Save a document's bytes to this machine's real Downloads folder (_DOWNLOADS_DIR)
    under its real filename (falling back to '<doc_type>.pdf'). Used by the manual
    Download/View link (operations_document_file). Returns the saved filename, or None
    if the write failed (logged, never raised - a save failure here must not blow up
    the Download link)."""
    safe_name = (filename or "").strip() or f"{doc_type}.pdf"
    if not os.path.splitext(safe_name)[1]:
        safe_name = safe_name + ".pdf"
    try:
        os.makedirs(_DOWNLOADS_DIR, exist_ok=True)
        dest_path = os.path.join(_DOWNLOADS_DIR, safe_name)
        with open(dest_path, "wb") as f:
            f.write(data_bytes)
        current_app.logger.info(f"Saved document copy to: {dest_path}")
        return safe_name
    except Exception as e:
        current_app.logger.warning(f"Could not save copy to {_DOWNLOADS_DIR}: {e}")
        return None


def _subject_by_id():
    """Best-effort subject lookup from the cached mail list, purely for labeling jobs
    in the status panel/logs."""
    try:
        if os.path.exists(_SCRAPED_EMAILS_PATH):
            with open(_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                emails = json.load(f)
            return {e.get("id"): e.get("subject", "") for e in emails if e.get("id")}
    except Exception:
        pass
    return {}


@operations_api_bp.route("/operations/launch", methods=["POST"])
def launch_operations():
    try:
        if _control_server_reachable():
            return jsonify({"success": True, "message": "Shypple automation browser is already running."})
        if _ensure_shypple_browser_running(timeout_s=45):
            return jsonify({"success": True, "message": "Shypple automation browser launched."})
        return jsonify({
            "success": False,
            "error": "Timed out waiting for the Shypple automation browser to start (45s).",
        }), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


def _extract_attachment_docs(rec, scraped_email=None):
    docs = [
        d for d in rec.get("documents", [])
        if d.get("source") == "attachment" and d.get("type") != "No DOC"
    ]
    if not docs:
        raw_atts = rec.get("attachments") or []
        docs = [
            {"type": a.get("type", "Other"), "attachment_index": i, "filename": a.get("filename", "")}
            for i, a in enumerate(raw_atts)
            if a.get("type") != "No DOC"
        ]

    names = (scraped_email.get("attachmentNames") or []) if scraped_email else []
    if not docs and names:
        docs = [
            {"type": "Other", "attachment_index": i, "filename": name}
            for i, name in enumerate(names)
        ]

    if not docs:
        fallback_type = rec.get("email_type") or (rec.get("doc_types")[0] if rec.get("doc_types") else "Cmr")
        if fallback_type and fallback_type != "No DOC":
            docs = [{"type": fallback_type, "attachment_index": 0}]

    # If there are 2 or more document attachments:
    # Ensure one CMR document retains 'Cmr' type, while non-CMR attachments retain their specific
    # classified type (e.g. Phytosanitary certificate, Packing List, Commercial invoice, etc.)
    # or default to 'Other' if their type is unspecified or duplicate 'Cmr'.
    if len(docs) >= 2:
        cmr_idx = None
        for i, d in enumerate(docs):
            fn = (d.get("filename") or "").lower()
            if "cmr" in fn or "vrachtbrief" in fn:
                cmr_idx = i
                break
        if cmr_idx is None:
            for i, d in enumerate(docs):
                if d.get("type") == "Cmr":
                    cmr_idx = i
                    break
        if cmr_idx is None:
            cmr_idx = 0

        for i, d in enumerate(docs):
            if i == cmr_idx:
                d["type"] = "Cmr"
            else:
                current_type = d.get("type") or "Other"
                d["type"] = "Other" if current_type == "Cmr" else current_type

    return docs


@operations_api_bp.route("/operations/extract", methods=["POST"])
def extract_operations():
    """Classify the selected emails (containers + document types) and hand them back for
    review. Deliberately does NOT touch the Shypple browser at all - it should only ever
    open once the user has looked at these results and confirmed via /operations/start."""
    data = request.get_json() or {}
    message_ids = data.get("message_ids") or []
    if not message_ids:
        return jsonify({"success": False, "error": "message_ids is required."}), 400

    scraped_map = {}
    if os.path.exists(_SCRAPED_EMAILS_PATH):
        try:
            with open(_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                for e in json.load(f):
                    scraped_map[e.get("id")] = e
        except Exception:
            pass

    subjects = _subject_by_id()
    jobs, errors = [], []
    cache = _load_cache()
    for mid in message_ids:
        subject = subjects.get(mid, "")
        scraped_email = scraped_map.get(mid)
        try:
            # Only trust the cache here if it's a DEEP classification (source == "deep") -
            # that's the only path that actually opens the message and reads every real
            # attachment. A "metadata" record (built from list-view scraping alone, e.g. by
            # the background classify_new_documents_cmr auto-classifier that runs on every
            # mail as it arrives) is created for virtually every message before the user
            # ever gets here, and it can look "complete" (a non-empty attachments list)
            # even when it only saw ONE of several real attachments - a mail with 2+
            # attachments only ever showed its primary "Cmr" document (and no filename
            # for the rest) here whenever this accepted ANY cache hit instead of checking
            # source == "deep", since the list-view scrape never reliably captures every
            # attachment's real filename. Re-using a cached record still makes repeat
            # clicks on Process instant for mail that already went through a real deep
            # read (e.g. via "Find Document Types") - it's only the shallow metadata
            # records that must be re-resolved here.
            if mid in cache and cache[mid].get("source") == "deep":
                rec = cache[mid]
            else:
                try:
                    rec = resolve_deep_classification(mid, subject=subject, label="a-cmr")
                except Exception as ex:
                    current_app.logger.warning(f"resolve_deep_classification failed/timed out for {mid}: {ex}. Falling back to meta classification.")
                    email_obj = {"id": mid, "subject": subject, "hasAttachment": True}
                    rec = classify_email_meta(email_obj, force=False, label="a-cmr")

            attachment_docs = _extract_attachment_docs(rec, scraped_email=scraped_email)

            # Just report what classification already found - filename and type are
            # already sitting on each entry (classify_documents_cmr/classify_email_meta
            # both record "filename" per attachment). Per explicit request, this step
            # must NOT re-open the mail / re-fetch attachment bytes to "pre-download" a
            # local copy (that previous behaviour cost one extra full browser round-trip
            # PER document, on top of the round-trip classification itself already paid
            # for) - the actual bytes are still fetched exactly once, later, when
            # shypple_process.py needs them for the real upload/compare.
            snippet = (scraped_email or {}).get("snippet", "")
            jobs.append({
                "message_id": mid,
                "subject": subject or mid,
                "containers": rec.get("containers") or [],
                # Fallback for process_one_job when containers comes back empty (e.g. a
                # scanned, HANDWRITTEN CMR that's genuinely too hard to OCR/LLM-read
                # reliably) - a machine-printed SF reference in the subject line, so the
                # shipment can still be found by SF number and its real container(s)
                # read straight off Shypple's own Containers tab instead.
                "sf_number": extract_sf_number(f"{subject} {snippet}"),
                "doc_types": [d.get("type") for d in attachment_docs],
                "doc_attachment_indices": [d.get("attachment_index") for d in attachment_docs],
                "doc_filenames": [d.get("filename") or "" for d in attachment_docs],
            })
        except Exception as e:
            current_app.logger.warning(f"[Operations] Classification failed for {mid} ({subject}): {e}")
            errors.append({"message_id": mid, "subject": subject, "error": str(e)})

    if not jobs:
        detail = "; ".join(f"{e['subject'] or e['message_id']}: {e['error']}" for e in errors)
    return jsonify({"success": True, "jobs": jobs, "errors": errors})


def _lcl_star_yellow_and_unread(raw_id):
    """Star yellow + mark unread via scripts/open_gmail.py's control server, matching
    the _PLAYWRIGHT_ACTION_MAP/_star_source_email pattern used everywhere else in this
    file - ``raw_id`` must already have the "pw_" prefix stripped. Raises on failure
    (callers decide how to report that)."""
    params_star = urllib.parse.urlencode({"id": raw_id, "color": "yellow"})
    with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/star_color?{params_star}", timeout=15) as r:
        star_result = json.loads(r.read().decode("utf-8"))
    params_unread = urllib.parse.urlencode({"type": "mark_unread", "id": raw_id})
    with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/action?{params_unread}", timeout=20) as r:
        unread_result = json.loads(r.read().decode("utf-8"))
    if not star_result.get("success") or not unread_result.get("success"):
        raise RuntimeError(
            f"star={star_result.get('error') or 'ok'}, mark_unread={unread_result.get('error') or 'ok'}"
        )


def _lcl_fetch_body_text(raw_id, subject=""):
    """Plain-text-ish version of a scraped mail's body, for regex extraction on mail
    types (delay/devanning) that may have no PDF attachment at all - proxies to
    scripts/open_gmail.py's /get_body the same way tracking_api.py's get_email_body
    does. Best-effort: returns "" on any failure rather than raising, since callers
    always have subject/snippet as a fallback extraction source."""
    try:
        params = urllib.parse.urlencode({"id": raw_id})
        with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/get_body?{params}", timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
        html = data.get("body", "") or ""
        return re.sub(r"<[^>]+>", " ", html)
    except Exception as e:
        current_app.logger.warning(f"[LCL Arrivals] Could not fetch body text for '{subject[:40]}': {e}")
        return ""


_LCL_MAIL_TYPE_DOC_TYPE = {"arrival_notice": "Arrival notice", "delivery_order": "Delivery order"}


@operations_api_bp.route("/operations/lcl_arrivals_process", methods=["POST"])
def lcl_arrivals_process():
    """Process LCL Arrivals / Release emails: classify into the 4 email types, extract
    the fields each type needs, and either resolve it immediately (Shipment Not
    Released) or hand back a "flow": "lcl_arrivals" job for the operator to review
    before /operations/start opens the Shypple browser to act on it.

    1. Shipment Not Released -> mark email unread + add yellow star, resolved here.
    2. Delay or Devanning -> extract date; Shypple date-compare/update happens in
       scripts/shypple_process.py's process_lcl_arrival_job.
    3. Arrival Notice -> extract SF number/container/date/customs/CFS from the actual
       attached document (deep classification + PDF text), for the Shypple side to fill.
    4. Delivery Order -> extract SF number/container from the actual attached document.
    """
    from app.services.parsing.document_classifier import (
        classify_lcl_arrival_email, extract_sf_number, extract_lcl_arrival_data,
        extract_lcl_fields_via_llm, _extract_pdf_page_texts, find_all_document_sources,
    )
    from app.services.llm.llm_client import GeminiClient

    data = request.get_json() or {}
    message_ids = data.get("message_ids") or []
    if not message_ids:
        return jsonify({"success": False, "error": "message_ids is required."}), 400

    # scripts/open_gmail.py keeps this label on its own permanently-open tab, scraped
    # into its own file (_LCL_SCRAPED_EMAILS_PATH) - NOT the shared _SCRAPED_EMAILS_PATH,
    # which only ever contains a-cmr mail now that no per-request label switch happens
    # for this label anymore (see tracking_api.py's get_emails).
    scraped_map = {}
    if os.path.exists(_LCL_SCRAPED_EMAILS_PATH):
        try:
            with open(_LCL_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                for e in json.load(f):
                    scraped_map[e.get("id")] = e
        except Exception:
            pass

    results = []
    for mid in message_ids:
        scraped_email = scraped_map.get(mid, {"id": mid})
        subject = scraped_email.get("subject", "")
        snippet = scraped_email.get("snippet", "")
        mail_type = classify_lcl_arrival_email(scraped_email)
        if mail_type == "unknown" and mid.startswith("pw_") and scraped_email.get("hasAttachment"):
            # classify_lcl_arrival_email only looked at subject/snippet/attachment
            # filenames - a mail worded differently in its subject (e.g. "Arrival
            # message" instead of "Arrival notice") lands here as "unknown" even
            # though the ACTUALLY ATTACHED document plainly says "ARRIVAL NOTICE"/
            # "DELIVERY ORDER" in its own top section (confirmed by the operator).
            # Falling through as "unknown" skips the whole deep-extraction block
            # below, so container/devanning date/customs number/CFS address all come
            # back empty even though the document has them. classify_documents_lcl's
            # deep read already checks the real PDF text for exactly this reason (see
            # its docstring) - reuse whatever it resolves instead of giving up here.
            try:
                doc_rec = resolve_deep_classification(mid, subject=subject, label=_LCL_LABEL_KEY)
                doc_type = doc_rec.get("email_type") if doc_rec else None
                if doc_type == "Arrival notice":
                    mail_type = "arrival_notice"
                elif doc_type == "Delivery order":
                    mail_type = "delivery_order"
            except Exception as e:
                current_app.logger.warning(f"[LCL Arrivals] Deep type fallback failed for '{subject[:40]}': {e}")
        sf_number = extract_sf_number(f"{subject} {snippet}")

        res_entry = {
            "message_id": mid,
            "subject": subject,
            "mail_type": mail_type,
            "sf_number": sf_number,
        }

        if not mid.startswith("pw_"):
            res_entry["status"] = "error"
            res_entry["note"] = "This message isn't from the delegated mailbox this pipeline can act on."
            results.append(res_entry)
            continue
        raw_id = mid[len("pw_"):]

        # Rule 1: Shipment Not Released -> immediately mark unread + yellow star, no
        # Shypple job needed at all.
        if mail_type == "shipment_not_released":
            try:
                _lcl_star_yellow_and_unread(raw_id)
                res_entry["status"] = "processed_unread_yellow_starred"
                res_entry["note"] = "Shipment not released - marked unread and yellow starred."
            except Exception as e:
                res_entry["status"] = "error"
                res_entry["note"] = f"Failed to mark unread/star: {e}"
            results.append(res_entry)
            continue

        # Every other type needs real text to extract fields from - prefer the actual
        # attached document's text (arrival_notice/delivery_order) over the email's own
        # subject/snippet, which rarely carries the container/date/customs/CFS detail.
        text_for_extraction = f"{subject} {snippet}"
        doc_type_label = _LCL_MAIL_TYPE_DOC_TYPE.get(mail_type)
        data_bytes, doc_mime, doc_filename = None, None, None
        if doc_type_label:
            try:
                # label=_LCL_LABEL_KEY (not None/"a-cmr"): restricts classification to
                # Arrival notice/Delivery order/Other so this always finds a document
                # typed exactly "doc_type_label" below - the old label=None generic
                # branch could (and did) type the same attachment as an unrelated CMR
                # type (e.g. "Final master bill of lading"), which made
                # fetch_document_bytes find NOTHING for "Delivery order" and silently
                # fall through to the no-document path a few lines down.
                resolve_deep_classification(mid, subject=subject, label=_LCL_LABEL_KEY)
                data_bytes, doc_mime, doc_filename = fetch_document_bytes(
                    mid, doc_type_label, subject=subject, label=_LCL_LABEL_KEY
                )
                if data_bytes:
                    page_texts = _extract_pdf_page_texts(data_bytes)
                    if page_texts:
                        text_for_extraction = "\n".join(page_texts) + "\n" + text_for_extraction
                    # Per the operator's request: a local copy of the mail's own
                    # document belongs on disk at Process time, before the Shypple
                    # browser is ever opened - not only later, on-demand, via the
                    # Download/View link (_save_document_copy's original caller).
                    saved_name = _save_document_copy(data_bytes, doc_filename, doc_type_label)
                    if saved_name:
                        res_entry["saved_document_filename"] = saved_name

                # Per the operator's explicit example (a Delivery Order mail carrying
                # BOTH a carrier "Delivery order" PDF and a separate "Release" document
                # that also resolves to "Delivery order" on its own merit - see
                # classify_documents_lcl's per-attachment classification): field
                # extraction above is driven entirely by the FIRST such document, but
                # any additional one(s) still need to reach Shypple - just uploaded
                # as-is, no extraction re-run on them. Record which attachment_index(es)
                # those are so scripts/shypple_process.py's handle_delivery_order can
                # upload them after the primary document.
                all_docs = find_all_document_sources(mid, doc_type_label)
                res_entry["extra_doc_attachment_indices"] = [
                    d.get("attachment_index") for d in all_docs[1:]
                    if d.get("attachment_index") is not None
                ]
            except Exception as e:
                current_app.logger.warning(
                    f"[LCL Arrivals] Deep extraction failed for '{subject[:40]}' ({mail_type}): {e}"
                )

            # Rule (per operator's explicit request): a mail type that's SUPPOSED to
            # carry an actual document (arrival_notice/delivery_order) but doesn't -
            # nothing found even after fetch_document_bytes' own force-reclassify retry,
            # or the fetch itself failed - has nothing for the automation to act on.
            # Resolve it the same way as Rule 1 (mark unread + yellow star) instead of
            # silently falling through to a Shypple review built on nothing but a
            # subject/snippet guess: this is this pipeline's own "done" marker, so it
            # also drops the mail out of future "not yellow-starred" sweeps and
            # surfaces it for manual handling.
            if not data_bytes:
                try:
                    _lcl_star_yellow_and_unread(raw_id)
                    res_entry["status"] = "processed_unread_yellow_starred"
                    res_entry["note"] = f"No {doc_type_label} document found on this email - marked unread and yellow starred."
                except Exception as e:
                    res_entry["status"] = "error"
                    res_entry["note"] = f"No {doc_type_label} document found, and failed to mark unread/star: {e}"
                results.append(res_entry)
                continue
        elif mail_type == "delay_or_devanning":
            body_text = _lcl_fetch_body_text(raw_id, subject=subject)
            if body_text:
                text_for_extraction = f"{text_for_extraction}\n{body_text}"

            # This mail type's whole PURPOSE is a (possibly new) devanning date, but
            # that date is almost never in the email body/subject in a machine-
            # parseable form (real example: the body just says "delivering tomorrow" -
            # no digits at all for the date regexes to match) - it lives in the mail's
            # own attached document (the actual delay/devanning notice PDF), which was
            # never read here before (delay_or_devanning has no entry in
            # _LCL_MAIL_TYPE_DOC_TYPE, so it never went through the deep-document-read
            # block above at all). Originally this was gated on "no SF number found
            # yet", on the assumption it was purely an SF-number/container-number
            # fallback - but a real mail with SF170758 plainly in its subject STILL
            # needed its attachment read, since the devanning date itself was only in
            # the PDF; gating on SF-number presence skipped that read even though the
            # date was still missing. Now unconditional whenever there's a real
            # attachment, matching how arrival_notice/delivery_order always read
            # theirs. Read every real attachment's own text regardless of its
            # classified type - a delay/devanning attachment rarely matches "Arrival
            # notice"/"Delivery order" wording, so it'll usually just be "Other"; the
            # type doesn't matter here, only the text does.
            if scraped_email.get("hasAttachment"):
                try:
                    doc_rec = resolve_deep_classification(mid, subject=subject, label=_LCL_LABEL_KEY)
                    att_docs = [
                        d for d in ((doc_rec or {}).get("documents") or [])
                        if d.get("source") == "attachment" and d.get("attachment_index") is not None
                    ]
                    for doc in att_docs:
                        try:
                            att_bytes, _mime, _fn = fetch_document_bytes(
                                mid, doc.get("type"), subject=subject,
                                attachment_index=doc.get("attachment_index"), label=_LCL_LABEL_KEY,
                            )
                            if att_bytes:
                                page_texts = _extract_pdf_page_texts(att_bytes)
                                if page_texts:
                                    text_for_extraction = "\n".join(page_texts) + "\n" + text_for_extraction
                        except Exception as e:
                            current_app.logger.warning(
                                f"[LCL Arrivals] Delay/devanning attachment read failed for "
                                f"'{subject[:40]}' (attachment_index={doc.get('attachment_index')}): {e}"
                            )
                except Exception as e:
                    current_app.logger.warning(
                        f"[LCL Arrivals] Delay/devanning deep classification failed for '{subject[:40]}': {e}"
                    )

        # SF number was only ever looked up from the subject/snippet above - a mail
        # whose SF reference is printed INSIDE the attached document rather than the
        # subject line always came back "not found" even once that document's own text
        # was available. Re-derive it from the FULL text (now including the PDF's
        # content, if any) before falling back to the LLM read below.
        full_text_sf = extract_sf_number(text_for_extraction)
        if full_text_sf:
            sf_number = full_text_sf
            res_entry["sf_number"] = sf_number

        extracted = extract_lcl_arrival_data(text_for_extraction)

        # Deterministic regex-over-PDF-text-layer extraction (above) has three blind
        # spots: a scanned/image-only document with no text layer at all (same root
        # cause as CMR's handwritten container numbers), a real text layer whose actual
        # label wording just doesn't match extract_lcl_arrival_data's patterns, and -
        # confirmed on a real Arrival Notice - a 2-column table whose PDF text
        # extraction doesn't preserve visual reading order, so a label can end up
        # immediately next to an UNRELATED neighbouring cell's value (e.g. "Expected
        # Devanning date" picking up the Bill of Lading Date instead; "Customs number"
        # - genuinely blank on that document - picking up a stray "ETA" from a nearby
        # cell). That third case produces a WRONG-BUT-PRESENT value, which a plain
        # "only fill in what's missing" check can't catch. So whenever we have the
        # document's real bytes, always ask Gemini to read the fields directly off the
        # rendered page (immune to text-flattening order issues) and, for each field,
        # prefer the LLM's answer whenever it disagrees with the regex's - trusting the
        # page-aware read over blind proximity matching. A field the LLM couldn't find
        # keeps the regex's value (LLM silence isn't evidence the regex was wrong).
        if data_bytes:
            llm_fields = extract_lcl_fields_via_llm(GeminiClient(), data_bytes, doc_mime, doc_filename or subject)
            for k in ("container_number", "devanning_date", "customs_number", "cfs_address"):
                llm_val = llm_fields.get(k)
                if not llm_val:
                    continue
                regex_val = extracted.get(k)
                if not regex_val:
                    extracted[k] = llm_val
                elif str(regex_val).strip().casefold() != str(llm_val).strip().casefold():
                    current_app.logger.info(
                        f"[LCL Arrivals] '{k}' regex/LLM disagreement for '{subject[:40]}': "
                        f"regex='{regex_val}' llm='{llm_val}' - using the LLM's value."
                    )
                    extracted[k] = llm_val
            if not sf_number and llm_fields.get("sf_number"):
                sf_number = llm_fields["sf_number"]
                res_entry["sf_number"] = sf_number

        res_entry["flow"] = "lcl_arrivals"
        res_entry["extracted"] = extracted
        res_entry["status"] = "pending_preview"
        results.append(res_entry)

    return jsonify({"success": True, "items": results})



@operations_api_bp.route("/operations/document_type_options", methods=["GET"])
def operations_document_type_options():
    """The document-type dropdown options for the Operations Process review UI's
    type-override control. Prefers the REAL, live-scraped list of Shypple's own
    #shipment_document_document_type_ids options (captured passively during real
    uploads, or on demand via /operations/refresh_document_type_options) - whatever
    the user picks from that list is guaranteed to exist as a real Shypple option,
    unlike our internal canonical DOCUMENT_TYPES list, which doesn't map 1:1 onto
    Shypple's exact wording (that mismatch is what caused select2 match failures).
    Falls back to our own canonical list, clearly flagged as such, only if nothing has
    been captured from Shypple yet."""
    try:
        with urllib.request.urlopen(f"{_CONTROL_SERVER}/document_type_options", timeout=8) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("success") and result.get("options"):
            return jsonify({"success": True, "options": result["options"], "source": "shypple",
                             "captured_at": result.get("captured_at")})
    except Exception:
        pass
    return jsonify({"success": True, "options": DOCUMENT_TYPES, "source": "internal"})


@operations_api_bp.route("/operations/refresh_document_type_options", methods=["POST"])
def operations_refresh_document_type_options():
    """Force a fresh live scrape of Shypple's document-type dropdown (see
    shypple_process.py's _refresh_document_type_options) rather than waiting for the
    next real upload to passively capture it. Requires the Shypple browser to already
    be running and idle (not mid-batch)."""
    try:
        req = urllib.request.Request(f"{_CONTROL_SERVER}/refresh_document_type_options", data=b"{}", method="POST")
        with urllib.request.urlopen(req, timeout=45) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except urllib.error.HTTPError as e:
        try:
            result = json.loads(e.read().decode("utf-8"))
        except Exception:
            result = {"success": False, "error": str(e)}
        return jsonify(result), e.code
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Shypple automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/start", methods=["POST"])
def start_operations():
    """Open (or reuse) the Shypple browser and hand it the job list the user already
    reviewed and confirmed in the UI - this is the only path that ever opens Shypple."""
    data = request.get_json() or {}
    jobs = data.get("jobs") or []
    if not jobs:
        return jsonify({"success": False, "error": "jobs is required."}), 400

    # Auto-launch (and wait for) the Shypple automation browser if it isn't already
    # running, instead of requiring a separate manual "Launch" click first - Playwright's
    # cold Chrome start can take a while, hence the generous timeout here.
    if not _ensure_shypple_browser_running(timeout_s=60):
        return jsonify({
            "success": False,
            "error": "The Shypple automation browser didn't start within 60s. It may still be "
                     "opening Chrome for the first time - try again in a few seconds.",
        }), 500

    try:
        body = json.dumps({"jobs": jobs}).encode("utf-8")
        req = urllib.request.Request(
            f"{_CONTROL_SERVER}/run_batch", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            result = json.loads(e.read().decode("utf-8"))
        except Exception:
            result = {"success": False, "error": str(e)}
        return jsonify(result), e.code
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"The Shypple automation browser is running but rejected the batch: {e}",
        }), 500

    return jsonify({"success": True, "queued": result.get("queued", len(jobs))})


@operations_api_bp.route("/operations/status", methods=["GET"])
def operations_status():
    try:
        with urllib.request.urlopen(f"{_CONTROL_SERVER}/status", timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Shypple automation browser is not running: {e}",
        }), 500


@operations_api_bp.route("/operations/proceed", methods=["POST"])
def operations_proceed():
    try:
        req = urllib.request.Request(
            f"{_CONTROL_SERVER}/proceed", data=b"{}",
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Shypple automation browser: {e}",
        }), 500


@operations_api_bp.route("/operations/skip", methods=["POST"])
def operations_skip():
    """Drop the currently-paused job (whichever confirmation gate it's stuck at)
    without performing its pending action, so the batch moves on to the next job
    instead of the operator being forced to either confirm it or relaunch Shypple."""
    try:
        req = urllib.request.Request(
            f"{_CONTROL_SERVER}/skip", data=b"{}",
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Shypple automation browser: {e}",
        }), 500


@operations_api_bp.route("/operations/move_yellow_starred", methods=["POST"])
def move_yellow_starred():
    """Bulk-move every yellow-starred mail on the Gmail automation's currently loaded
    list page to a target label (true "Move to" - removes the current label too).
    Standalone action, independent of the Shypple verification batch above - talks
    directly to scripts/open_gmail.py's control server, not the Shypple one."""
    data = request.get_json(silent=True) or {}
    target_label = (data.get("label") or _DEFAULT_YELLOW_STAR_LABEL).strip()
    try:
        params = urllib.parse.urlencode({"label": target_label, "color": "yellow"})
        url = f"{_GMAIL_CONTROL_SERVER}/move_starred_to_label?{params}"
        with urllib.request.urlopen(url, timeout=45) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/move_purple_starred", methods=["POST"])
def move_purple_starred():
    """Bulk-move every purple-starred mail on the Gmail automation's currently loaded
    list page to a target label. Same mechanics as move_yellow_starred, different
    color/label."""
    data = request.get_json(silent=True) or {}
    target_label = (data.get("label") or _DEFAULT_PURPLE_STAR_LABEL).strip()
    try:
        params = urllib.parse.urlencode({"label": target_label, "color": "purple"})
        url = f"{_GMAIL_CONTROL_SERVER}/move_starred_to_label?{params}"
        with urllib.request.urlopen(url, timeout=45) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/process_blue_starred", methods=["POST"])
def process_blue_starred():
    """Bulk-forward every blue-starred mail on the Gmail automation's currently loaded
    list page to nl.importsea@shypple.com, labeling the forwarded copy there as
    a-release-orders (that label lives in that mailbox, not this one - the original
    here is left untouched) - the exact same action the Shypple automation already
    takes automatically when a matched shipment has no organization set, exposed here
    as a standalone "catch up on anything already starred blue" button. Can take a
    while (one open+forward+relabel per mail, not a single bulk action - Gmail's Forward needs the message
    open), hence the long timeout."""
    data = request.get_json(silent=True) or {}
    to_email = (data.get("to") or _DEFAULT_BLUE_FORWARD_TO).strip()
    target_label = (data.get("label") or _DEFAULT_BLUE_FORWARD_LABEL).strip()
    try:
        params = urllib.parse.urlencode({"color": "blue", "to": to_email, "label": target_label})
        url = f"{_GMAIL_CONTROL_SERVER}/process_color_starred_forward?{params}"
        with urllib.request.urlopen(url, timeout=150) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/pending_relabel_summary", methods=["GET"])
def pending_relabel_summary():
    """How many previously-forwarded mails (see process_blue_starred /
    shypple_process.py's no-organization forward) are still missing their
    a-release-orders label in the nl.importsea@shypple.com mailbox, grouped by the day
    they were forwarded - shown by the dashboard's "Label pending release-orders"
    button before it retries them."""
    try:
        with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/pending_relabel_summary", timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/label_pending_relabel", methods=["POST"])
def label_pending_relabel():
    """Retry applying the a-release-orders label to every forwarded mail that isn't
    yet confirmed labeled in the nl.importsea@shypple.com mailbox - the manual catch-up
    for forward_and_relabel's best-effort immediate attempt."""
    try:
        with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/label_pending_relabel", timeout=130) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/forwarded_mails", methods=["GET"])
def forwarded_mails_list():
    """Every mail tracked in the forwarded-mail tracker (both labeled and still-
    pending), most recent first - powers the dashboard's forwarded-mails list view."""
    try:
        with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/forwarded_mails_list", timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/forwarded_mails/delete", methods=["POST"])
def delete_forwarded_mail():
    """Remove one entry from the forwarded-mail tracker. Does not touch Gmail itself -
    only forgets the tracking record (so, e.g., that message could be forwarded again
    on a future run)."""
    data = request.get_json(silent=True) or {}
    message_id = (data.get("message_id") or "").strip()
    if not message_id:
        return jsonify({"success": False, "error": "message_id is required."}), 400
    try:
        params = urllib.parse.urlencode({"id": message_id})
        with urllib.request.urlopen(f"{_GMAIL_CONTROL_SERVER}/delete_forwarded_mail?{params}", timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Could not reach the Gmail automation browser: {e}. Make sure it's running.",
        }), 500


@operations_api_bp.route("/operations/document_file", methods=["GET"])
def operations_document_file():
    """Raw bytes of the attachment classified as ``doc_type`` for ``message_id`` -
    used by scripts/shypple_process.py (via a plain urllib GET, unaffected by the
    headers below) to fetch the email's own version of a document, both to compare it
    against an already-uploaded Shypple document and to supply the bytes for an actual
    upload. Also backs the dashboard's "Download"/"View" links on each Operations
    Process job's document(s): as_attachment=True (the default) forces a browser
    navigating straight to this URL to save it to the local Downloads folder under a
    real filename/extension, rather than mishandling it inline (unlike Shypple's own
    raw document links, which were producing extension-less, unopenable downloads).
    Pass ?inline=1 to instead render it in the browser (e.g. a new tab, for a PDF) -
    for viewing without saving a copy to disk; a plain text editor can't open a PDF
    directly, hence "View" as a distinct option from "Download". Pass
    ?attachment_index=N when the caller already knows exactly which attachment it wants
    (see extract_operations' doc_attachment_indices) - required to disambiguate a mail
    with two or more attachments of the SAME type (e.g. two "Other" documents), where
    doc_type alone would always resolve to the first one. Pass ?label=... to control
    which label override fetch_document_bytes' own force-reclassify retry uses if this
    specific attachment isn't found in the cache yet - defaults to "a-cmr" (matching
    fetch_document_bytes' own default) for backward compatibility with existing CMR
    callers that don't pass it; scripts/shypple_process.py's LCL handlers pass
    label=lcl-arrivals---release explicitly so a retry can't corrupt an LCL mail's
    classification under the CMR Cmr/Other override rule."""
    message_id = request.args.get("message_id", "")
    doc_type = request.args.get("doc_type", "")
    subject = request.args.get("subject", "")
    label = request.args.get("label", "") or "a-cmr"
    attachment_index_raw = request.args.get("attachment_index", "")
    attachment_index = int(attachment_index_raw) if attachment_index_raw.strip().lstrip("-").isdigit() else None
    if not message_id or not doc_type:
        return jsonify({"success": False, "error": "message_id and doc_type are required."}), 400

    data_bytes, mime, filename = fetch_document_bytes(
        message_id, doc_type, subject=subject, attachment_index=attachment_index, label=label
    )
    if data_bytes is None:
        return jsonify({
            "success": False,
            "error": f"Could not find/fetch an attachment classified as '{doc_type}' for this email.",
        }), 404

    safe_name = _save_document_copy(data_bytes, filename, doc_type) or (filename or "").strip() or f"{doc_type}.pdf"
    if not os.path.splitext(safe_name)[1]:
        safe_name = safe_name + ".pdf"

    as_attachment = request.args.get("inline", "") != "1"
    response = send_file(
        io.BytesIO(data_bytes),
        mimetype=mime or mimetypes.guess_type(safe_name)[0] or "application/pdf",
        as_attachment=as_attachment,
        download_name=safe_name,
    )
    response.headers["X-Document-Filename"] = filename or ""
    return response


@operations_api_bp.route("/operations/local_document/<path:filename>", methods=["GET"])
def operations_local_document(filename):
    """Serve a locally-saved copy of an already-uploaded Shypple document (see
    scripts/shypple_process.py's save_document_locally, called during the deep-compare
    step of Operations Process) from data/downloads. as_attachment=True (the default)
    downloads it under its real filename/extension, unlike clicking the same
    document's link directly in Shypple's own admin UI. Pass ?inline=1 to render it in
    the browser instead (e.g. a new tab, for a PDF), for viewing without saving a
    fresh copy. send_from_directory validates filename against directory traversal on
    its own."""
    if not os.path.isdir(_DOWNLOADS_DIR):
        return jsonify({"success": False, "error": "No documents have been saved locally yet."}), 404
    # send_from_directory raises a bare werkzeug NotFound for a missing file, which
    # renders Flask's generic unstyled "Not Found" page - no different from a typo'd
    # URL, so a link left pointing at a file that's since been moved/deleted (e.g. by
    # something outside this app entirely - Explorer, antivirus, manual cleanup of the
    # downloads/ folder) looked exactly like a broken route instead of a missing file.
    # Checking existence first lets us say specifically which file is gone.
    target_path = os.path.join(_DOWNLOADS_DIR, filename)
    if not os.path.isfile(target_path):
        return jsonify({
            "success": False,
            "error": f"'{filename}' is no longer in the downloads folder (it may have been moved or "
                     f"deleted outside the app). The document is still on Shypple itself - re-run "
                     f"verification for this job to save a fresh local copy.",
        }), 404
    as_attachment = request.args.get("inline", "") != "1"
    return send_from_directory(_DOWNLOADS_DIR, filename, as_attachment=as_attachment)


@operations_api_bp.route("/operations/compare_document", methods=["POST"])
def operations_compare_document():
    """Compare the email's version of ``doc_type`` for ``message_id`` against a file the
    caller already downloaded from Shypple (base64), using Gemini to judge whether
    they're the same document or materially different - see
    document_classifier.compare_document_versions for the actual logic."""
    data = request.get_json(silent=True) or {}
    message_id = data.get("message_id", "")
    doc_type = data.get("doc_type", "")
    subject = data.get("subject", "")
    attachment_index = data.get("attachment_index")
    other_b64 = data.get("other_file_base64", "")
    other_mime = data.get("other_mime", "") or "application/pdf"
    if not message_id or not doc_type or not other_b64:
        return jsonify({
            "success": False,
            "error": "message_id, doc_type and other_file_base64 are required.",
        }), 400

    try:
        other_bytes = base64.b64decode(other_b64)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not decode other_file_base64: {e}"}), 400

    email_bytes, email_mime, _ = fetch_document_bytes(
        message_id, doc_type, subject=subject, attachment_index=attachment_index
    )
    if email_bytes is None:
        return jsonify({
            "success": False,
            "error": f"Could not find/fetch the email's own attachment classified as '{doc_type}'.",
        }), 404

    result = compare_document_versions(email_bytes, email_mime, other_bytes, other_mime, doc_type)
    return jsonify({"success": True, **result})
