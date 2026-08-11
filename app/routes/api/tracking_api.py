import os
import sys
import json
import time
import subprocess
import mimetypes
import urllib.request
import urllib.parse
from flask import Blueprint, jsonify, request, make_response
from app.services.common.email_service import EmailService
from app.services.common.sheets_service import SheetsService
from app.services.llm.llm_client import GeminiClient
from app.services.parsing.document_classifier import (
    classify_email, classify_email_meta, classify_documents, classify_documents_cmr, classify_documents_lcl,
    get_cached_type_map, resolve_deep_classification, suppress_redundant_mrn, clear_classification_cache,
    delete_classification, clear_classifications, _load_cache,
)

# Playwright-scraped IDs (from a delegated mailbox the Gmail API can't reach
# directly) are prefixed "pw_" and carry Gmail's own internal legacy message id;
# real Gmail API message IDs never have this prefix.
_SCRAPED_ID_PREFIX = "pw_"

tracking_api_bp = Blueprint("tracking_api", __name__)

@tracking_api_bp.route("/status", methods=["GET"])
def get_status():
    email_service = EmailService()
    email_ok, email_msg = email_service.check_connection()
    
    sheets_service = SheetsService()
    sheets_ok, sheets_msg = sheets_service.check_connection()
    
    gemini_client = GeminiClient()
    try:
        test_resp = gemini_client.generate("Hi")
        gemini_ok = True
        gemini_msg = "Connected"
    except Exception as e:
        gemini_ok = False
        gemini_msg = str(e)
        
    return jsonify({
        "email": {"ok": email_ok, "message": email_msg},
        "sheets": {"ok": sheets_ok, "message": sheets_msg},
        "gemini": {"ok": gemini_ok, "message": gemini_msg}
    })

# The delegated-mailbox browser automation (scripts/open_gmail.py) rewrites this cache
# every ~2s while it runs. If it was touched very recently we treat it as the live
# source and skip the Gmail API / IMAP chain entirely - that chain can't see this
# delegated label and only adds latency + IMAP auth-failure log spam to every poll.
_SCRAPED_EMAILS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails.json")
)
# open_gmail.py now keeps a SECOND, permanently-open tab pinned to this label (see
# lcl_page_ref/LCL_LABEL_KEY there), scraped independently every ~2s into its own file -
# no per-request /switch_label call is needed (or made) for it anymore, which is also
# what fixed both labels showing identical stale content: the old single-tab switch
# had a client-side timeout shorter than the server's own wait, so the switch request
# was frequently abandoned before it actually completed.
_LCL_LABEL_KEY = "lcl-arrivals---release"
_LCL_SCRAPED_EMAILS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails_lcl.json")
)
_SCRAPED_FRESH_SECONDS = 90


@tracking_api_bp.route("/emails", methods=["GET"])
def get_emails():
    target_label = request.args.get("label", "").strip()

    if target_label == _LCL_LABEL_KEY:
        scraped_path = _LCL_SCRAPED_EMAILS_PATH
    else:
        scraped_path = _SCRAPED_EMAILS_PATH
        if target_label:
            try:
                # Tell open_gmail control server to switch the PRIMARY tab's label if
                # running - only relevant for a-cmr (the default) or any other ad-hoc
                # label typed into the label box; the LCL label bypasses this entirely
                # (see above). Timeout must exceed open_gmail.py's own internal wait
                # for this request (req.event.wait(timeout=20) in its /switch_label
                # handler - navigating to a new label and rescraping it can legitimately
                # take close to that).
                url = f"http://127.0.0.1:40005/switch_label?label={urllib.parse.quote(target_label)}"
                urllib.request.urlopen(url, timeout=30)
                time.sleep(0.5)
            except Exception:
                pass

    if os.path.exists(scraped_path):
        try:
            with open(scraped_path, "r", encoding="utf-8") as f:
                emails = json.load(f)
        except Exception as e:
            return jsonify({"success": False, "error": f"Could not read the scraped mail cache: {e}"}), 500
        age = time.time() - os.path.getmtime(scraped_path)
        return jsonify({
            "success": True,
            "emails": emails,
            "source": "live-scrape",
            "stale": age >= _SCRAPED_FRESH_SECONDS,
            "age_seconds": int(age),
        })

    return jsonify({
        "success": False,
        "error": "No scraped mail yet - make sure the 'Open Gmail' automation is running.",
    }), 503

@tracking_api_bp.route("/automation/open-gmail", methods=["POST"])
def open_gmail():
    try:
        python_exe = sys.executable
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts", "open_gmail.py"))
        
        # Launch open_gmail.py in the background
        subprocess.Popen([python_exe, script_path])
        return jsonify({"success": True, "message": "Playwright browser launched successfully."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@tracking_api_bp.route("/emails/body", methods=["GET"])
def get_email_body():
    message_id = request.args.get("id", "")

    # Real Gmail message ID: fetch directly via the Gmail API (fast, reliable).
    if message_id and not message_id.startswith(_SCRAPED_ID_PREFIX):
        email_service = EmailService()
        try:
            result = email_service.get_email_full(message_id)
            if result is None:
                return jsonify({"success": False, "error": "No Gmail credentials available to retrieve this message."}), 500
            return jsonify({"success": True, "body": result["body"], "attachments": result["attachments"]})
        except Exception as e:
            return jsonify({"success": False, "error": f"Failed to retrieve email body: {str(e)}"}), 500

    # Scraped ID: this message lives in a delegated mailbox the Gmail API can't reach,
    # so proxy the request to the Playwright control server driving the logged-in
    # browser tab (see scripts/open_gmail.py), matching by Gmail's own internal
    # message id rather than subject/sender/date (this label has many genuine
    # duplicate-looking emails that fuzzy text matching can't tell apart).
    if not message_id:
        return jsonify({"success": False, "error": "Message id is required."}), 400

    raw_id = message_id[len(_SCRAPED_ID_PREFIX):]
    try:
        params = urllib.parse.urlencode({"id": raw_id})
        url = f"http://127.0.0.1:40005/get_body?{params}"
        with urllib.request.urlopen(url, timeout=20) as response:
            data = json.loads(response.read().decode("utf-8"))
            return jsonify({"success": True, "body": data.get("body", ""), "attachments": data.get("attachments", [])})
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Failed to retrieve body from the delegated-mailbox browser: {str(e)}. "
                     f"Make sure the 'Open Gmail' automation window is running and logged into the mailbox that owns this label."
        }), 500


@tracking_api_bp.route("/emails/attachment/<message_id>/<attachment_id>", methods=["GET"])
def get_email_attachment(message_id, attachment_id):
    email_service = EmailService()
    try:
        data = email_service.get_attachment(message_id, attachment_id)
        if data is None:
            return jsonify({"success": False, "error": "No Gmail credentials available to retrieve this attachment."}), 500

        filename = request.args.get("filename", "attachment")
        mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        disposition = "attachment" if request.args.get("download") else "inline"

        response = make_response(data)
        response.headers["Content-Type"] = mimetype
        response.headers["Content-Disposition"] = f'{disposition}; filename="{filename}"'
        return response
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to retrieve attachment: {str(e)}"}), 500

@tracking_api_bp.route("/emails/classify", methods=["POST"])
def classify_email_documents():
    """Identify the document type of an email's body and attachments.

    Read-only: this goes through the Gmail API readonly path only, so it never changes
    the mail's read/unread state. Scraped ('pw_') ids are rejected for that reason.
    """
    data = request.get_json() or {}
    message_id = data.get("id") or data.get("message_id")
    force = bool(data.get("force"))
    if not message_id:
        return jsonify({"success": False, "error": "Message id is required."}), 400

    if message_id.startswith(_SCRAPED_ID_PREFIX):
        return jsonify({
            "success": False,
            "error": "This message is in the delegated mailbox and can't be classified "
                     "via the read-only Gmail API without risking its read state.",
        }), 400

    try:
        result = classify_email(message_id, force=force)
        return jsonify({"success": True, **result})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": f"Classification failed: {str(e)}"}), 500


@tracking_api_bp.route("/emails/classify_new", methods=["POST"])
def classify_new_documents_cmr():
    """Auto-classify only the a-cmr mails that don't yet have a cached type.

    CMR-only - see classify_new_documents_lcl below for the LCL Arrivals/Release
    equivalent. Split apart for the same reason as classify_all_documents_cmr/_lcl:
    CMR and LCL are separate processes end to end, so their live auto-classify sweeps
    no longer share one function's control flow either.

    Called by the live view on every refresh so newly-arrived mails get a document type
    automatically. Escalates to a real, deep, per-attachment read (resolve_deep_
    classification, the same deep path "Deep analyze"/"Find Document Types" uses)
    whenever the mail has a real attachment - the list-view metadata scrape can't
    reliably see every attachment on a multi-document mail (it might only surface the
    primary "Cmr" file's name and miss a second "Other" one), so trusting it alone here
    is exactly how a 2-attachment mail showed only a single document type live, even
    though "Find Document Types" (which always escalates) found both. This does mark
    the mail read in Gmail as a side effect, but fetch_email_body (see
    scripts/open_gmail.py) restores the original unread status afterward. Already-
    classified mails are skipped entirely (no LLM call, no re-open), so - like "Find
    Document Types" - only a mail's first sighting ever pays this cost; this is cheap
    to run repeatedly. Returns the full id->type map plus the mails it newly classified
    this pass.
    """
    from flask import current_app

    emails = []
    try:
        if os.path.exists(_SCRAPED_EMAILS_PATH):
            with open(_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                emails = json.load(f)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the mail list: {e}"}), 500

    ids = [e.get("id") for e in emails if e.get("id")]
    types, types_all, containers, type_counts = get_cached_type_map(ids)  # loads the cache once

    new_results = []
    for email in emails:
        mid = email.get("id")
        if not mid or mid in types:
            continue
        try:
            rec = classify_email_meta(email, label="a-cmr")  # classifies + caches the new mail
        except Exception as e:
            current_app.logger.warning(f"[DocType][CMR] auto-classify failed for {mid}: {e}")
            continue

        doc_type = rec.get("email_type", "Other")
        doc_types_list = rec.get("doc_types") or [doc_type]
        rec_containers = rec.get("containers") or []
        rec_type_counts = rec.get("type_counts") or {}
        has_attachment = bool(email.get("hasAttachment"))

        if has_attachment:
            try:
                deep_rec = resolve_deep_classification(mid, subject=email.get("subject", ""), label="a-cmr")
                if deep_rec:
                    doc_type = deep_rec.get("email_type", doc_type)
                    doc_types_list = deep_rec.get("doc_types") or doc_types_list
                    rec_containers = deep_rec.get("containers") or rec_containers
                    rec_type_counts = deep_rec.get("type_counts") or rec_type_counts
            except Exception as e:
                current_app.logger.warning(f"[DocType][CMR] deep container read failed for {mid}: {e}")
                # Fall back to the metadata-only result already computed above.

        doc_types_list, doc_type = suppress_redundant_mrn(doc_types_list, doc_type)

        types[mid] = doc_type
        types_all[mid] = doc_types_list
        if rec_containers:
            containers[mid] = rec_containers
        if rec_type_counts:
            type_counts[mid] = rec_type_counts
        new_results.append({
            "id": mid,
            "sender": rec.get("sender", ""),
            "subject": rec.get("subject", ""),
            "type": doc_type,
            "types": doc_types_list,
            "type_counts": rec_type_counts,
            "containers": rec_containers,
            "confidence": rec.get("confidence", 0),
        })

    if new_results:
        current_app.logger.info(
            f"[DocType][CMR] Auto-classified {len(new_results)} new mail(s): "
            + ", ".join(f"{r['type']} <- {(r['subject'] or '')[:40]}" for r in new_results)
        )

    return jsonify({
        "success": True,
        "new_count": len(new_results),
        "types": types,
        "types_all": types_all,
        "containers": containers,
        "type_counts": type_counts,
        "results": new_results,
    })


@tracking_api_bp.route("/emails/classify_new_lcl", methods=["POST"])
def classify_new_documents_lcl():
    """Auto-classify only the lcl-arrivals---release mails that don't yet have a cached
    type - the LCL Arrivals/Release equivalent of classify_new_documents_cmr above (see
    its docstring for why these are two separate functions rather than one shared,
    branching one).

    Otherwise identical in shape to the CMR version: escalates to a real, deep,
    per-attachment read whenever a mail has a real attachment (a shallow metadata
    record can't prove it saw every attachment), restores the original unread status
    afterward, and skips already-classified mail entirely so repeat calls are cheap.
    label=_LCL_LABEL_KEY is passed through to classify_email_meta/resolve_deep_
    classification so they take their lcl-arrivals label override branch - restricted
    to "Arrival notice"/"Delivery order"/"Other" only, not the CMR label's Cmr/Other
    rule and not the full CMR document-type taxonomy either (see document_classifier.py's
    _LCL_ARRIVALS_LABEL branches for why the generic multi-type guesser was wrong here -
    it was surfacing unrelated CMR types like "Packing List"/"Final master bill of
    lading" for LCL mail)."""
    from flask import current_app

    emails = []
    try:
        if os.path.exists(_LCL_SCRAPED_EMAILS_PATH):
            with open(_LCL_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                emails = json.load(f)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the mail list: {e}"}), 500

    ids = [e.get("id") for e in emails if e.get("id")]
    types, types_all, containers, type_counts = get_cached_type_map(ids)  # loads the cache once

    new_results = []
    for email in emails:
        mid = email.get("id")
        if not mid or mid in types:
            continue
        try:
            rec = classify_email_meta(email, label=_LCL_LABEL_KEY)  # classifies + caches the new mail
        except Exception as e:
            current_app.logger.warning(f"[DocType][LCL] auto-classify failed for {mid}: {e}")
            continue

        doc_type = rec.get("email_type", "Other")
        doc_types_list = rec.get("doc_types") or [doc_type]
        rec_containers = rec.get("containers") or []
        rec_type_counts = rec.get("type_counts") or {}
        has_attachment = bool(email.get("hasAttachment"))

        if has_attachment:
            try:
                deep_rec = resolve_deep_classification(mid, subject=email.get("subject", ""), label=_LCL_LABEL_KEY)
                if deep_rec:
                    doc_type = deep_rec.get("email_type", doc_type)
                    doc_types_list = deep_rec.get("doc_types") or doc_types_list
                    rec_containers = deep_rec.get("containers") or rec_containers
                    rec_type_counts = deep_rec.get("type_counts") or rec_type_counts
            except Exception as e:
                current_app.logger.warning(f"[DocType][LCL] deep container read failed for {mid}: {e}")
                # Fall back to the metadata-only result already computed above.

        doc_types_list, doc_type = suppress_redundant_mrn(doc_types_list, doc_type)

        types[mid] = doc_type
        types_all[mid] = doc_types_list
        if rec_containers:
            containers[mid] = rec_containers
        if rec_type_counts:
            type_counts[mid] = rec_type_counts
        new_results.append({
            "id": mid,
            "sender": rec.get("sender", ""),
            "subject": rec.get("subject", ""),
            "type": doc_type,
            "types": doc_types_list,
            "type_counts": rec_type_counts,
            "containers": rec_containers,
            "confidence": rec.get("confidence", 0),
        })

    if new_results:
        current_app.logger.info(
            f"[DocType][LCL] Auto-classified {len(new_results)} new mail(s): "
            + ", ".join(f"{r['type']} <- {(r['subject'] or '')[:40]}" for r in new_results)
        )

    return jsonify({
        "success": True,
        "new_count": len(new_results),
        "types": types,
        "types_all": types_all,
        "containers": containers,
        "type_counts": type_counts,
        "results": new_results,
    })


@tracking_api_bp.route("/emails/deep_classify", methods=["POST"])
def deep_classify_documents():
    """DEEP document-type scan: reads the ACTUAL document bytes (all pages) so it can
    use a keyword printed inside the document and detect a multi-document PDF.

    Real Gmail ids go through the read-only API (read state preserved). Scraped ('pw_')
    ids are proxied to the delegated-mailbox browser to download the attachment bytes -
    this opens the mail and CAN mark it read, which is why it is a separate, explicit
    action rather than part of the read-safe bulk sweep.

    ``label`` (optional, from the dashboard's "Deep analyze" button - whichever label
    tab is currently open) picks the same override classify_all_documents_lcl/_cmr use -
    this endpoint used to always call classify_documents_cmr for scraped mail regardless
    of label, which would force the Cmr/Other split onto lcl-arrivals mail too.
    """
    import base64

    data = request.get_json() or {}
    message_id = data.get("id") or data.get("message_id")
    subject = data.get("subject", "") or ""
    label = data.get("label") or ""
    force = bool(data.get("force"))
    if not message_id:
        return jsonify({"success": False, "error": "Message id is required."}), 400

    # Real Gmail message: deep-classify via the read-only Gmail API.
    if not message_id.startswith(_SCRAPED_ID_PREFIX):
        try:
            result = classify_email(message_id, force=force)
            return jsonify({"success": True, **result})
        except ValueError as e:
            return jsonify({"success": False, "error": str(e)}), 400
        except Exception as e:
            return jsonify({"success": False, "error": f"Deep classification failed: {str(e)}"}), 500

    # Scraped mailbox: fetch body + attachment bytes from the Playwright control server.
    raw_id = message_id[len(_SCRAPED_ID_PREFIX):]
    try:
        params = urllib.parse.urlencode({"id": raw_id})
        url = f"http://127.0.0.1:40005/get_documents?{params}"
        # Matches open_gmail.py's /get_documents internal wait (150s) - this was
        # previously a mismatched 50s, so a genuinely successful 2+ attachment read
        # (which can take up to ~90s) hit THIS client's own socket timeout well before
        # the server ever got a chance to respond, reporting a false failure.
        with urllib.request.urlopen(url, timeout=160) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Failed to retrieve documents from the delegated-mailbox browser: {str(e)}. "
                     f"Make sure the 'Open Gmail' automation window is running and logged in.",
        }), 500

    attachments = []
    for att in payload.get("attachments", []):
        b64 = att.get("data_b64")
        data_bytes = None
        if b64:
            try:
                data_bytes = base64.b64decode(b64)
            except Exception:
                data_bytes = None
        attachments.append({
            "filename": att.get("filename", ""),
            "mime": att.get("mime", ""),
            "data_bytes": data_bytes,
        })

    try:
        if label == _LCL_LABEL_KEY:
            result = classify_documents_lcl(message_id, subject, payload.get("body", ""), attachments, force=force)
        else:
            result = classify_documents_cmr(message_id, subject, payload.get("body", ""), attachments, force=force)
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": f"Deep classification failed: {str(e)}"}), 500


@tracking_api_bp.route("/emails/classify_all", methods=["POST"])
def classify_all_documents_cmr():
    """Identify the document type of every UNREAD mail currently in the a-cmr label.

    CMR-only - see classify_all_documents_lcl below for the LCL Arrivals/Release
    equivalent. The two used to be one shared, branching function; they were split
    apart deliberately so a change made for one process's rules (scope, label
    handling, logging) can never silently affect the other's code path - CMR and LCL
    are separate processes end to end in this pipeline.

    Scoped to unread mail only, on purpose: already-read mail was already looked at in
    an earlier sweep (or read for some other reason), so re-running the expensive deep
    escalation below on it every time would multiply the mail-opening cost - and the
    403 risk - for no benefit. New mail arrives unread, gets classified once here, and
    naturally drops out of scope for the next sweep. Deliberately NO fallback to
    classifying read mail when the label happens to have zero unread messages - the
    caller asked for unread-only, so an empty label just returns an empty result
    instead of silently classifying mail the caller didn't ask about.

    Starts from the list-view metadata (subject, snippet, hasAttachment) that the
    browser automation scrapes. For mail with a real attachment, that metadata alone
    can only ever assume a single "Cmr" document - Gmail's list view exposes NO
    reliable per-file names or attachment count, so a second document (which should
    show as "Other") is invisible without actually opening the mail. This escalates to
    a real, deep, per-attachment read for every mail with a real attachment (not just
    ones where the shallow metadata scrape came back empty-handed - a metadata record
    that already "found" one attachment name still can't prove that is the ONLY
    attachment, so trusting it there is exactly how a 2-attachment mail silently showed
    only 1 document). Already deep-classified mail is served from
    resolve_deep_classification's own cache (unless force=True), so only a sweep right
    after a cache reset pays the full per-mail open cost - repeat runs are fast. Opening
    a message for this deep read marks it read in Gmail as a side effect -
    fetch_email_body (see scripts/open_gmail.py) restores the original unread status
    afterward, so this sweep never leaves a mail's read/unread state different from how
    it found it. Progress is logged to the server terminal and returned to the UI.
    """
    from flask import current_app

    data = request.get_json(silent=True) or {}
    force = bool(data.get("force"))

    emails = []
    try:
        if os.path.exists(_SCRAPED_EMAILS_PATH):
            with open(_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                emails = json.load(f)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the mail list: {e}"}), 500

    if not emails:
        return jsonify({
            "success": False,
            "error": "No mails found to classify. Make sure the 'Open Gmail' automation "
                     "window is running and the a-cmr label is loaded.",
        }), 400

    emails = [e for e in emails if e.get("unread")]
    if not emails:
        current_app.logger.info("[DocType][CMR] No unread mail in 'a-cmr' - nothing to classify.")
        return jsonify({
            "success": True,
            "unread_count": 0,
            "count": 0,
            "total": 0,
            "total_documents": 0,
            "summary": {},
            "results": [],
            "label": "a-cmr",
            "scope": "unread",
        })

    total = len(emails)
    current_app.logger.info(f"[DocType][CMR] Starting document-type identification for {total} unread mail(s)...")

    results = []
    summary = {}
    for idx, email in enumerate(emails, 1):
        try:
            rec = classify_email_meta(email, force=force, label="a-cmr")
        except Exception as e:
            current_app.logger.warning(f"[DocType][CMR] {idx}/{total} failed: {e}")
            continue

        if force or email.get("hasAttachment"):
            try:
                deep_rec = resolve_deep_classification(
                    email.get("id"), subject=email.get("subject", ""), force=force, label="a-cmr"
                )
                if deep_rec:
                    rec = deep_rec
            except Exception as e:
                current_app.logger.warning(f"[DocType][CMR] {idx}/{total} deep read failed: {e}")

        doc_type = rec.get("email_type", "Other")
        doc_types_list, doc_type = suppress_redundant_mrn(rec.get("doc_types") or [doc_type], doc_type)
        # Calculate document count for this email
        att_docs = [
            d for d in rec.get("documents", [])
            if d.get("source") == "attachment" and d.get("type") != "No DOC"
        ]
        if not att_docs:
            att_docs = [
                a for a in rec.get("attachments", [])
                if a.get("type") != "No DOC"
            ]
        if att_docs:
            doc_count = len(att_docs)
        elif rec.get("type_counts"):
            doc_count = sum(rec.get("type_counts", {}).values())
        elif doc_types_list and doc_types_list != ["No DOC"]:
            doc_count = len(doc_types_list)
        else:
            doc_count = 1 if email.get("hasAttachment") else 0

        conf = int(round(rec.get("confidence", 0) * 100))
        summary[doc_type] = summary.get(doc_type, 0) + 1
        cache_tag = " (cached)" if rec.get("cached") else ""
        current_app.logger.info(
            f"[DocType][CMR] {idx}/{total} | {(rec.get('sender') or '')[:22]:22} | "
            f"{(rec.get('subject') or '')[:55]:55} -> {doc_type} ({doc_count} doc(s), {conf}%){cache_tag}"
        )
        raw_atts = rec.get("documents") or rec.get("attachments") or []
        real_atts = [a for a in raw_atts if a.get("filename") and a.get("filename") != "Attachment"]
        final_atts = real_atts if real_atts else raw_atts
        if final_atts:
            doc_count = len(final_atts)

        results.append({
            "id": rec.get("message_id"),
            "sender": rec.get("sender", ""),
            "subject": rec.get("subject", ""),
            "type": doc_type,
            "types": doc_types_list,
            "doc_count": doc_count,
            "type_counts": rec.get("type_counts", {}),
            "containers": rec.get("containers", []),
            "confidence": rec.get("confidence", 0),
            "method": rec.get("method", ""),
            "attachments": final_atts,
        })

    summary_str = ", ".join(f"{k}: {v}" for k, v in sorted(summary.items(), key=lambda x: -x[1]))
    total_documents = sum(r.get("doc_count", 0) for r in results)
    current_app.logger.info(f"[DocType][CMR] Done. Classified {len(results)}/{total} unread mail(s) ({total_documents} doc(s)). Breakdown -> {summary_str}")

    return jsonify({
        "success": True,
        "unread_count": total,
        "count": len(results),
        "total": total,
        "total_documents": total_documents,
        "summary": summary,
        "results": results,
        "label": "a-cmr",
        "scope": "unread",
    })


@tracking_api_bp.route("/emails/classify_all_lcl", methods=["POST"])
def classify_all_documents_lcl():
    """Identify the document type of every mail currently in the lcl-arrivals---release
    label that hasn't been yellow-starred yet.

    LCL-only - see classify_all_documents_cmr above for the CMR/a-cmr equivalent. Kept
    as a fully separate function (own scraped-mail source, own scope rule, own logging)
    rather than a branch inside the CMR version, since CMR and LCL are separate
    processes end to end in this pipeline and sharing one function's control flow is
    exactly what previously let an a-cmr-specific assumption silently apply to LCL mail
    (and vice versa).

    Scoped to "not yellow-starred" rather than "unread": this pipeline's own "done"
    marker is UNREAD + yellow star (the opposite of a-cmr's mark-READ-when-done rule -
    see scripts/shypple_process.py's _mark_source_email_unread), so an already-fully-
    processed LCL mail is unread ON PURPOSE. Scoping to unread-only here would keep
    re-classifying mail that's already been handled; yellow star is the real "already
    done" signal for this label. Deliberately no fallback to classifying yellow-starred
    mail when none is unstarred - an empty label just returns an empty result.

    Otherwise behaves the same as the CMR sweep: starts from list-view metadata,
    escalates to a real deep per-attachment read whenever a mail has a real attachment
    (a shallow metadata record can't prove it saw every attachment), and reuses
    resolve_deep_classification's own cache so repeat runs are fast. label=_LCL_LABEL_KEY
    is passed through to classify_email_meta/resolve_deep_classification so they take
    their lcl-arrivals label override branch - restricted to "Arrival notice"/
    "Delivery order"/"Other" only, never the full CMR document-type taxonomy the
    generic branch would otherwise guess from (that's what was surfacing unrelated
    types like "Packing List"/"Final master bill of lading" for LCL mail).
    """
    from flask import current_app

    data = request.get_json(silent=True) or {}
    force = bool(data.get("force"))

    emails = []
    try:
        if os.path.exists(_LCL_SCRAPED_EMAILS_PATH):
            with open(_LCL_SCRAPED_EMAILS_PATH, "r", encoding="utf-8") as f:
                emails = json.load(f)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the mail list: {e}"}), 500

    if not emails:
        return jsonify({
            "success": False,
            "error": "No mails found to classify. Make sure the 'Open Gmail' automation "
                     "window is running and the LCL Arrivals/Release label is loaded.",
        }), 400

    emails = [e for e in emails if e.get("starColor") != "yellow"]
    if not emails:
        current_app.logger.info(f"[DocType][LCL] No non-yellow-starred mail in '{_LCL_LABEL_KEY}' - nothing to classify.")
        return jsonify({
            "success": True,
            "unread_count": 0,
            "count": 0,
            "total": 0,
            "total_documents": 0,
            "summary": {},
            "results": [],
            "label": _LCL_LABEL_KEY,
            "scope": "not yellow-starred",
        })

    total = len(emails)
    current_app.logger.info(f"[DocType][LCL] Starting document-type identification for {total} not-yellow-starred mail(s)...")

    results = []
    summary = {}
    for idx, email in enumerate(emails, 1):
        try:
            rec = classify_email_meta(email, force=force, label=_LCL_LABEL_KEY)
        except Exception as e:
            current_app.logger.warning(f"[DocType][LCL] {idx}/{total} failed: {e}")
            continue

        if force or email.get("hasAttachment"):
            try:
                deep_rec = resolve_deep_classification(
                    email.get("id"), subject=email.get("subject", ""), force=force, label=_LCL_LABEL_KEY
                )
                if deep_rec:
                    rec = deep_rec
            except Exception as e:
                current_app.logger.warning(f"[DocType][LCL] {idx}/{total} deep read failed: {e}")

        doc_type = rec.get("email_type", "Other")
        doc_types_list, doc_type = suppress_redundant_mrn(rec.get("doc_types") or [doc_type], doc_type)
        # Calculate document count for this email
        att_docs = [
            d for d in rec.get("documents", [])
            if d.get("source") == "attachment" and d.get("type") != "No DOC"
        ]
        if not att_docs:
            att_docs = [
                a for a in rec.get("attachments", [])
                if a.get("type") != "No DOC"
            ]
        if att_docs:
            doc_count = len(att_docs)
        elif rec.get("type_counts"):
            doc_count = sum(rec.get("type_counts", {}).values())
        elif doc_types_list and doc_types_list != ["No DOC"]:
            doc_count = len(doc_types_list)
        else:
            doc_count = 1 if email.get("hasAttachment") else 0

        conf = int(round(rec.get("confidence", 0) * 100))
        summary[doc_type] = summary.get(doc_type, 0) + 1
        cache_tag = " (cached)" if rec.get("cached") else ""
        current_app.logger.info(
            f"[DocType][LCL] {idx}/{total} | {(rec.get('sender') or '')[:22]:22} | "
            f"{(rec.get('subject') or '')[:55]:55} -> {doc_type} ({doc_count} doc(s), {conf}%){cache_tag}"
        )
        raw_atts = rec.get("documents") or rec.get("attachments") or []
        real_atts = [a for a in raw_atts if a.get("filename") and a.get("filename") != "Attachment"]
        final_atts = real_atts if real_atts else raw_atts
        if final_atts:
            doc_count = len(final_atts)

        results.append({
            "id": rec.get("message_id"),
            "sender": rec.get("sender", ""),
            "subject": rec.get("subject", ""),
            "type": doc_type,
            "types": doc_types_list,
            "doc_count": doc_count,
            "type_counts": rec.get("type_counts", {}),
            "containers": rec.get("containers", []),
            "confidence": rec.get("confidence", 0),
            "method": rec.get("method", ""),
            "attachments": final_atts,
        })

    summary_str = ", ".join(f"{k}: {v}" for k, v in sorted(summary.items(), key=lambda x: -x[1]))
    total_documents = sum(r.get("doc_count", 0) for r in results)
    current_app.logger.info(f"[DocType][LCL] Done. Classified {len(results)}/{total} not-yellow-starred mail(s) ({total_documents} doc(s)). Breakdown -> {summary_str}")

    return jsonify({
        "success": True,
        "unread_count": total,
        "count": len(results),
        "total": total,
        "total_documents": total_documents,
        "summary": summary,
        "results": results,
        "label": _LCL_LABEL_KEY,
        "scope": "not yellow-starred",
    })


@tracking_api_bp.route("/llm/generate", methods=["POST"])
def generate():
    data = request.get_json() or {}
    prompt = data.get("prompt")
    if not prompt:
        return jsonify({"success": False, "error": "Prompt is required"}), 400
        
    gemini_client = GeminiClient()
    try:
        result = gemini_client.generate(prompt)
        return jsonify({"success": True, "response": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@tracking_api_bp.route("/emails/send", methods=["POST"])
def send_email():
    data = request.get_json() or {}
    to = data.get("to")
    subject = data.get("subject")
    body = data.get("body")
    if not to or not subject or not body:
        return jsonify({"success": False, "error": "To, Subject, and Body are required."}), 400
        
    email_service = EmailService()
    success, message = email_service.send_email(to, subject, body)
    return jsonify({"success": success, "message": message})

@tracking_api_bp.route("/emails/clear_cache", methods=["POST"])
def clear_cache():
    clear_classification_cache()
    return jsonify({"success": True, "message": "All saved document classifications cleared successfully."})


def _label_scraped_ids_and_emails(label):
    """Read one label's own scraped-mail file and return (ids_set, emails_by_id) -
    shared by the History endpoints below to determine which cached classifications
    "belong" to a-cmr vs lcl-arrivals---release. Classification records carry no label
    of their own (see document_classifier.py), so membership is a join against
    whichever mail is CURRENTLY scraped for that label - the same join
    operations_api.py's _subject_by_id already relies on for subjects. A mail that has
    since left the label (archived, moved by a "Process yellow/purple-starred" sweep,
    etc.) drops out of both the history view and "Clear all" as a result - accepted
    as a known limit rather than threading a persistent label tag through every
    classification save site for this."""
    scraped_path = _LCL_SCRAPED_EMAILS_PATH if label == _LCL_LABEL_KEY else _SCRAPED_EMAILS_PATH
    emails_by_id = {}
    if os.path.exists(scraped_path):
        with open(scraped_path, "r", encoding="utf-8") as f:
            for e in json.load(f):
                if e.get("id"):
                    emails_by_id[e["id"]] = e
    return set(emails_by_id.keys()), emails_by_id


@tracking_api_bp.route("/emails/classification_history", methods=["GET"])
def classification_history():
    """History of already-classified mail for one label (a-cmr or
    lcl-arrivals---release, via ?label=) - read from the same persistent cache "Find
    Document Types"/"Update Live"/Operations Process all write to
    (data/doc_classifications.json), most recently classified first."""
    label = (request.args.get("label") or "a-cmr").strip() or "a-cmr"
    try:
        _, emails_by_id = _label_scraped_ids_and_emails(label)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the mail list: {e}"}), 500

    cache = _load_cache()
    results = []
    for mid, rec in cache.items():
        email = emails_by_id.get(mid)
        if not email:
            continue
        doc_type = rec.get("email_type", "Other")
        doc_types_list = rec.get("doc_types") or [doc_type]
        raw_atts = rec.get("documents") or rec.get("attachments") or []
        real_atts = [a for a in raw_atts if a.get("filename") and a.get("filename") != "Attachment"]
        final_atts = real_atts if real_atts else raw_atts
        if final_atts:
            doc_count = len(final_atts)
        elif doc_types_list and doc_types_list != ["No DOC"]:
            doc_count = len(doc_types_list)
        else:
            doc_count = 0
        results.append({
            "id": mid,
            "subject": email.get("subject") or rec.get("subject", ""),
            "sender": email.get("from") or rec.get("sender", ""),
            "type": doc_type,
            "types": doc_types_list,
            "doc_count": doc_count,
            "containers": rec.get("containers", []),
            "confidence": rec.get("confidence", 0),
            "source": rec.get("source", ""),
            "classified_at": rec.get("classified_at", ""),
        })

    results.sort(key=lambda r: r.get("classified_at") or "", reverse=True)
    return jsonify({"success": True, "label": label, "count": len(results), "results": results})


@tracking_api_bp.route("/emails/classification_history/delete", methods=["POST"])
def classification_history_delete():
    """Forget ONE mail's cached classification - see delete_classification."""
    data = request.get_json(silent=True) or {}
    message_id = data.get("message_id")
    if not message_id:
        return jsonify({"success": False, "error": "message_id is required."}), 400
    existed = delete_classification(message_id)
    return jsonify({"success": True, "deleted": existed})


@tracking_api_bp.route("/emails/classification_history/clear", methods=["POST"])
def classification_history_clear():
    """Delete every cached classification belonging to one label (a-cmr or
    lcl-arrivals---release) - scoped per label (unlike the older, blunt /emails/
    clear_cache above, which wipes BOTH labels' entire history at once), so clearing
    CMR's history can never silently also wipe LCL's, or vice versa. Membership is the
    same scraped-mail join classification_history above uses."""
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "a-cmr").strip() or "a-cmr"
    try:
        ids, _ = _label_scraped_ids_and_emails(label)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the mail list: {e}"}), 500

    removed = clear_classifications(ids)
    return jsonify({"success": True, "label": label, "removed": removed})


@tracking_api_bp.route("/emails/mark_unread", methods=["POST"])
def mark_unread():
    data = request.get_json() or {}
    message_id = data.get("message_id")
    unread = data.get("unread", True)
    if not message_id:
        return jsonify({"success": False, "error": "Message ID is required."}), 400

    email_service = EmailService()
    success, message = email_service.modify_email_unread(message_id, unread)
    return jsonify({"success": success, "message": message})


# Actions the delegated (scraped-id) mailbox can perform via the Playwright control
# server. "star"/"unstar" both map to "toggle_star" there since a browser click can
# only toggle, not set an absolute state - the real Gmail API path below sets it
# absolutely.
_PLAYWRIGHT_ACTION_MAP = {
    "star": "toggle_star",
    "unstar": "toggle_star",
    "mark_read": "mark_read",
    "mark_unread": "mark_unread",
    "archive": "archive",
    "delete": "delete",
}


@tracking_api_bp.route("/emails/action", methods=["POST"])
def email_action():
    data = request.get_json() or {}
    action = data.get("action", "")
    message_id = data.get("id", "")

    if action not in _PLAYWRIGHT_ACTION_MAP:
        return jsonify({"success": False, "error": f"Unknown action: {action}"}), 400

    # Real Gmail message ID: perform directly via the Gmail API (fast, reliable, and
    # supports setting an absolute star/read state rather than just toggling).
    if message_id and not message_id.startswith(_SCRAPED_ID_PREFIX):
        email_service = EmailService()
        try:
            if action == "star":
                ok, msg = email_service.set_starred(message_id, True)
            elif action == "unstar":
                ok, msg = email_service.set_starred(message_id, False)
            elif action == "mark_read":
                ok, msg = email_service.modify_email_unread(message_id, False)
            elif action == "mark_unread":
                ok, msg = email_service.modify_email_unread(message_id, True)
            elif action == "archive":
                ok, msg = email_service.archive_email(message_id)
            else:  # delete
                ok, msg = email_service.trash_email(message_id)
            return jsonify({"success": ok, "message": msg})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    # Scraped ID: this message lives in a delegated mailbox the Gmail API can't
    # reach, so proxy the request to the Playwright control server driving the
    # logged-in browser tab, matching by Gmail's own internal message id rather
    # than subject/sender/date (see get_email_body for why).
    if not message_id:
        return jsonify({"success": False, "error": "Message id is required."}), 400

    raw_id = message_id[len(_SCRAPED_ID_PREFIX):]
    try:
        params = urllib.parse.urlencode({"type": _PLAYWRIGHT_ACTION_MAP[action], "id": raw_id})
        url = f"http://127.0.0.1:40005/action?{params}"
        with urllib.request.urlopen(url, timeout=20) as response:
            result = json.loads(response.read().decode("utf-8"))
            return jsonify(result)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Failed to perform action via the delegated-mailbox browser: {str(e)}. "
                     f"Make sure the 'Open Gmail' automation window is running and logged into the mailbox that owns this label."
        }), 500
