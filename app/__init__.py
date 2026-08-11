from flask import Flask
from app.config import Config
from app.extensions import init_logger

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Always reload templates from disk on each request. Without this, Flask compiles
    # each template once and (with debug=False) keeps serving that cached copy even
    # after the .html file changes - so UI edits appear to "not show up" until a full
    # process restart. Jinja's stat-based check is cheap for an app this size.
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.jinja_env.auto_reload = True

    # Initialize Logger
    init_logger(app)
    
    # Register blueprints/routes
    from app.routes.web.dashboard import dashboard_bp
    from app.routes.api.tracking_api import tracking_api_bp
    from app.routes.api.operations_api import operations_api_bp
    from app.routes.api.realtime_api import realtime_api_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(tracking_api_bp, url_prefix="/api")
    app.register_blueprint(operations_api_bp, url_prefix="/api")
    app.register_blueprint(realtime_api_bp)

    # Never let the browser cache the HTML pages. The dashboard is opened inside a
    # persistent Playwright Chrome profile whose disk cache otherwise serves a stale
    # copy of the page after UI edits (buttons/layout appear "missing" until the cache
    # is cleared). Static assets (CSS/JS) keep their normal caching.
    @app.after_request
    def _no_html_cache(response):
        if response.mimetype == "text/html":
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    # Start email monitoring on app startup
    @app.before_request
    def _start_monitoring():
        if not hasattr(app, '_monitoring_started'):
            from app.services.common.email_monitor import get_email_monitor_service
            monitor = get_email_monitor_service()
            monitor.start_monitoring()
            app._monitoring_started = True

    return app
