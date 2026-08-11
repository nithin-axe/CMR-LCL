"""Reusable file/PDF download techniques for the sync-Playwright automation scripts
(scripts/open_gmail.py, scripts/shypple_process.py). Three distinct techniques, tried
in order of how invasive/risky they are, since different target sites trigger a
download differently and there's no single approach that works everywhere:

1. fetch_bytes_via_request / fetch_bytes_robust - the default for a KNOWN, already-
   authenticated URL (e.g. an <a href> already scraped from the page): a plain
   page.request.get() reusing the browser's own cookies, no click/navigation
   involved. fetch_bytes_robust adds a same-URL in-page JS fetch() as a fallback for
   the (rarer) case a raw request-context GET gets rejected - e.g. a server that
   checks for real-browser fetch headers/referrer - that an in-page fetch, subject to
   the page's own normal browser semantics, can often still get through.

2. save_via_native_download - for a clickable element that makes the BROWSER itself
   prompt a save (Playwright's native "download" event) - captures that event instead
   of guessing a URL at all.

3. save_via_popup_scan - for a clickable element that opens a NEW tab/window (e.g. an
   inline PDF viewer/embed) rather than downloading directly: waits for the popup,
   scans its frames for the real file URL, then fetches those bytes via the
   authenticated session and saves them straight to disk.

Every function here is defensive by design - it returns None (or (None, None)) on
failure rather than raising, so a caller can fall through to another technique or a
"log and continue" path without an unhandled exception taking down a whole batch.
"""

import base64
import os
import sys


def get_downloads_dir():
    """The CURRENT machine/user's real Downloads folder - never a hardcoded path.
    This project gets deployed to different company machines/accounts, and a literal
    'C:\\Users\\<name>\\Downloads' only ever works on the one machine it was typed on.

    On Windows, reads the "User Shell Folders" registry entry for the Downloads GUID
    first - the only reliable way to get the RIGHT answer if a user has relocated
    their Downloads folder (right-click it > Properties > Location), which a plain
    <home>/Downloads guess has no way to know about. Falls back to <home>/Downloads
    (correct for the default, never-moved case, and works on any OS) if that registry
    read fails for any reason - e.g. not running on Windows, or the key is missing.
    Always ensures the resolved directory exists before returning it."""
    downloads = None
    if sys.platform == "win32":
        try:
            import winreg
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders",
            ) as key:
                value, _ = winreg.QueryValueEx(key, "{374DE290-123F-4565-9164-39C4925E467B}")
            downloads = os.path.expandvars(value)
        except Exception:
            downloads = None

    if not downloads:
        downloads = os.path.join(os.path.expanduser("~"), "Downloads")

    os.makedirs(downloads, exist_ok=True)
    return downloads


def fetch_bytes_via_request(page, url, timeout_ms=20000):
    """Plain authenticated GET via Playwright's own request context (reuses the
    browser's cookies - no separate auth needed). Returns (bytes, content_type) or
    (None, None)."""
    try:
        resp = page.request.get(url, timeout=timeout_ms)
        if not resp.ok:
            print(f"[download_utils] fetch_bytes_via_request: HTTP {resp.status} for {url}")
            return None, None
        mime = (resp.headers.get("content-type", "") or "").split(";")[0].strip()
        return resp.body(), mime
    except Exception as e:
        print(f"[download_utils] fetch_bytes_via_request error for {url}: {e}")
        return None, None


def fetch_bytes_via_js(page, url, timeout_ms=20000):
    """Fetch url from INSIDE the page's own JS context, so it carries the page's
    cookies/credentials and normal browser fetch semantics exactly as a real click
    would - base64-round-trips the blob back into Python. Returns (bytes,
    content_type) or (None, None). Last-resort fallback for a URL a raw
    page.request.get() can't reach (e.g. blocked/CORS-guarded).

    timeout_ms bounds the in-browser fetch itself via AbortSignal.timeout - Playwright's
    own page.evaluate() has NO built-in timeout for a script that's just awaiting a JS
    Promise (unlike click()/wait_for_selector(), which do), so an unresponsive fetch()
    here (a stalled connection, a CORS preflight that never resolves) would previously
    hang this call indefinitely - blocking this one attachment forever, and with it every
    other request queued behind it on this same single-threaded automation loop."""
    try:
        result = page.evaluate(
            """async (args) => {
                try {
                    const response = await fetch(args.url, {
                        credentials: 'include',
                        signal: AbortSignal.timeout(args.timeoutMs),
                    });
                    if (!response.ok) return null;
                    const contentType = response.headers.get('content-type') || '';
                    const blob = await response.blob();
                    const reader = new FileReader();
                    const dataUrl = await new Promise((resolve) => {
                        reader.onloadend = () => resolve(reader.result);
                        reader.onerror = () => resolve(null);
                        reader.readAsDataURL(blob);
                    });
                    if (!dataUrl) return null;
                    return { b64: dataUrl.split(',')[1], contentType: contentType };
                } catch (e) {
                    return null;
                }
            }""",
            {"url": url, "timeoutMs": timeout_ms},
        )
        if not result or not result.get("b64"):
            return None, None
        return base64.b64decode(result["b64"]), result.get("contentType") or None
    except Exception as e:
        print(f"[download_utils] fetch_bytes_via_js error for {url}: {e}")
        return None, None


def fetch_bytes_robust(page, url, timeout_ms=20000):
    """fetch_bytes_via_request, falling back to fetch_bytes_via_js if that fails.
    Covers the common case (plain authenticated GET) cheaply, without paying the
    extra page.evaluate() round-trip unless the first attempt genuinely failed.
    Bounded at 2x timeout_ms total (one full attempt each), never unbounded."""
    data, mime = fetch_bytes_via_request(page, url, timeout_ms=timeout_ms)
    if data is not None:
        return data, mime
    print(f"[download_utils] fetch_bytes_robust: falling back to in-page JS fetch for {url}")
    return fetch_bytes_via_js(page, url, timeout_ms=timeout_ms)


def save_via_native_download(page, element, target_path, timeout_ms=20000):
    """Click element and capture Playwright's native download event - the file-save
    the browser itself performs when a link/button directly triggers a download
    (e.g. Content-Disposition: attachment, or a download="" anchor). Saves the result
    to target_path. Returns target_path on success, None on failure - never raises."""
    try:
        try:
            element.evaluate('(el) => el.removeAttribute("target")')
        except Exception:
            pass
        with page.expect_download(timeout=timeout_ms) as download_info:
            try:
                element.click(timeout=5000)
            except Exception:
                element.click(force=True, timeout=3000)
        download = download_info.value
        target_path.parent.mkdir(parents=True, exist_ok=True)
        download.save_as(str(target_path))
        return target_path
    except Exception as e:
        print(f"[download_utils] save_via_native_download failed: {e}")
        return None


def save_via_popup_scan(page, element, target_path, timeout_ms=30000, url_hint=""):
    """Click element expecting it to open a new popup tab (an inline PDF viewer/
    embed rather than a direct download), scan the popup's frames for an
    <embed>/<iframe> pointing at the real file URL, then fetch those bytes via the
    authenticated session (page.request - shares cookies with every tab in this
    browser context) and write them straight to target_path.

    url_hint (e.g. "pdf") narrows which embedded src is treated as the real file, in
    case a viewer page embeds more than one framed resource. Returns target_path on
    success, None on failure - never raises."""
    try:
        with page.expect_popup(timeout=timeout_ms) as popup_info:
            try:
                element.click(timeout=5000)
            except Exception:
                element.click(force=True, timeout=3000)
        viewer_page = popup_info.value
        viewer_page.wait_for_load_state("domcontentloaded")

        file_url = None
        for frame in viewer_page.frames:
            try:
                candidates = frame.locator("embed, iframe").all()
            except Exception:
                candidates = []
            for el in candidates:
                src = el.get_attribute("original-url") or el.get_attribute("src")
                if src and (not url_hint or url_hint.lower() in src.lower()):
                    file_url = src
                    break
            if file_url:
                break
        if not file_url:
            # Some viewers route the popup tab's own URL directly to the file.
            file_url = viewer_page.url

        data, _mime = fetch_bytes_robust(page, file_url)
        viewer_page.close()
        if data is None:
            print(f"[download_utils] save_via_popup_scan: could not fetch bytes for {file_url}")
            return None
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(data)
        return target_path
    except Exception as e:
        print(f"[download_utils] save_via_popup_scan failed: {e}")
        return None
