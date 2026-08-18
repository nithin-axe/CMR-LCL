import os
import json
from flask import Blueprint, render_template, current_app, redirect, url_for, request, session, make_response
from google_auth_oauthlib.flow import Flow
from app.services.common.email_service import EmailService
from app.services.common.sheets_service import SheetsService
from app.services.llm.llm_client import GeminiClient

dashboard_bp = Blueprint("dashboard", __name__)


def _ui_version():
    """A cache-busting fingerprint of the dashboard shell. Bumps whenever the
    template changes, so a long-lived open tab (e.g. the Playwright browser, which
    only re-fetches the mail list via AJAX and never the surrounding shell) can
    detect a UI change and reload itself instead of showing stale toolbar buttons."""
    tpl = os.path.join(current_app.root_path, "templates", "pages", "dashboard.html")
    try:
        return str(int(os.path.getmtime(tpl)))
    except OSError:
        return "0"


@dashboard_bp.route("/api/ui_version")
def ui_version():
    from flask import jsonify
    resp = jsonify({"version": _ui_version()})
    resp.headers["Cache-Control"] = "no-store"
    return resp


@dashboard_bp.route("/")
def index():
    # Check if OAuth flow client_secret.json is configured
    oauth_configured = os.path.exists("config/client_secret.json")
    # Check if authorized token or service account key is present
    token_present = os.path.exists("config/token.json") or os.path.exists("config/service_account.json")

    label = request.args.get("label", "a-cmr")
    recent_emails = []
    
    # Load cached emails for instant initial render
    scraped_path = os.path.abspath(os.path.join(current_app.root_path, "..", "data", "scraped_emails.json"))
    if os.path.exists(scraped_path):
        try:
            with open(scraped_path, "r", encoding="utf-8") as f:
                recent_emails = json.load(f)
        except Exception:
            pass

    resp = make_response(render_template(
        "pages/dashboard.html",
        email_status={"ok": token_present, "msg": "Initialized"},
        sheets_status={"ok": token_present, "msg": "Initialized"},
        gemini_status={"ok": True, "msg": "Initialized"},
        recent_emails=recent_emails,
        logged_in_email=None,
        oauth_configured=oauth_configured,
        token_present=token_present,
        selected_label=label,
        active_page=("lcl" if label == "lcl-arrivals---release" else "cmr"),
        ui_version=_ui_version()
    ))
    # Never let Chrome's persistent-profile disk cache serve a stale copy of the
    # dashboard shell - otherwise UI changes (e.g. new toolbar buttons) appear
    # "missing" even after a reload, because the cached HTML is returned instead.
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@dashboard_bp.route("/cmr-process")
def cmr_process():
    return redirect(url_for("dashboard.index", label="a-cmr"))

@dashboard_bp.route("/lcl-arrivals")
def lcl_arrivals():
    return redirect(url_for("dashboard.index", label="lcl-arrivals---release"))


@dashboard_bp.route("/lcl-my-jewellery")
def lcl_my_jewellery():
    return render_template("pages/my_jewellery.html", active_page="my_jewellery")


@dashboard_bp.route("/login/google")
def login_google():
    if not os.path.exists("config/client_secret.json"):
        return "client_secret.json is missing in config/ folder.", 400
        
    # Configure the flow
    flow = Flow.from_client_secrets_file(
        "config/client_secret.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/gmail.send"
        ],
        redirect_uri="http://localhost:40000/auth/google/callback"
    )
    
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    
    session["oauth_state"] = state
    return redirect(authorization_url)

@dashboard_bp.route("/auth/google/callback")
def auth_google_callback():
    state = session.get("oauth_state") or request.args.get("state")
    if not state:
        return "State parameter missing.", 400
        
    flow = Flow.from_client_secrets_file(
        "config/client_secret.json",
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/gmail.send"
        ],
        state=state,
        redirect_uri="http://localhost:40000/auth/google/callback"
    )
    
    # Allow HTTP for local testing
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    
    flow.fetch_token(authorization_response=request.url)
    
    credentials = flow.credentials
    token_data = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes
    }
    
    os.makedirs("config", exist_ok=True)
    with open("config/token.json", "w") as f:
        json.dump(token_data, f)
        
    return redirect(url_for("dashboard.index"))
