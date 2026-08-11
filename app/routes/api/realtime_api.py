import json
import os
from flask import Blueprint, jsonify, request, current_app, Response
from app.services.common.email_monitor import get_email_monitor_service
from app.services.common.websocket_service import get_websocket_service
from app.services.parsing.document_classifier import classify_email_meta

realtime_api_bp = Blueprint("realtime_api", __name__)

@realtime_api_bp.route("/api/emails/stream", methods=["GET"])
def email_stream():
    """Server-Sent Events endpoint for real-time email updates."""
    # Capture client_id BEFORE creating the generator (while request context is active)
    client_id = request.args.get("client_id", "default")
    
    def generate():
        ws_service = get_websocket_service()
        ws_service.subscribe(client_id)
        
        try:
            # Send initial connection message
            yield f"data: {json.dumps({'type': 'connected', 'client_id': client_id})}\n\n"
            
            # Keep connection alive and send updates
            import time
            while True:
                time.sleep(1)
                # In production, this would receive messages from a queue
                # For now, just keep the connection alive
                yield f": heartbeat\n\n"
        except GeneratorExit:
            ws_service.unsubscribe(client_id)
    
    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive"
    })

@realtime_api_bp.route("/api/emails/auto_classify", methods=["POST"])
def auto_classify_emails():
    """Automatically classify new emails without marking them as read.
    Uses metadata-only classification (subject, sender, snippet, attachment names)
    to avoid opening emails and changing their read/unread state.
    """
    data = request.get_json() or {}
    email_ids = data.get("email_ids", [])
    
    if not email_ids:
        return jsonify({"success": False, "error": "email_ids is required"}), 400
    
    # Load current emails from scraped cache to get metadata
    scraped_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails.json")
    )
    
    emails_by_id = {}
    if os.path.exists(scraped_path):
        try:
            with open(scraped_path, "r", encoding="utf-8") as f:
                emails = json.load(f)
                emails_by_id = {e.get("id"): e for e in emails if e.get("id")}
        except Exception as e:
            current_app.logger.warning(f"Could not load scraped emails: {e}")
    
    classifications = {}
    errors = []
    
    for email_id in email_ids:
        try:
            email = emails_by_id.get(email_id)
            if not email:
                errors.append({"email_id": email_id, "error": "Email metadata not found in cache"})
                continue
            
            # Use metadata-only classification (READ-SAFE - doesn't open the email)
            result = classify_email_meta(email, label="a-cmr")
            classifications[email_id] = {
                "type": result.get("email_type", "Unknown"),
                "containers": result.get("containers", []),
                "confidence": result.get("confidence", 0),
                "cached": result.get("cached", False)
            }
        except Exception as e:
            current_app.logger.warning(f"Auto-classification failed for {email_id}: {e}")
            errors.append({"email_id": email_id, "error": str(e)})
    
    # Broadcast classification updates
    if classifications:
        ws_service = get_websocket_service()
        ws_service.broadcast_classification_update(classifications)
    
    return jsonify({
        "success": True,
        "classified": len(classifications),
        "classifications": classifications,
        "errors": errors
    })

@realtime_api_bp.route("/api/emails/monitor/start", methods=["POST"])
def start_email_monitoring():
    """Start real-time email monitoring."""
    monitor = get_email_monitor_service()
    monitor.start_monitoring()
    return jsonify({"success": True, "message": "Email monitoring started"})

@realtime_api_bp.route("/api/emails/monitor/stop", methods=["POST"])
def stop_email_monitoring():
    """Stop real-time email monitoring."""
    monitor = get_email_monitor_service()
    monitor.stop_monitoring()
    return jsonify({"success": True, "message": "Email monitoring stopped"})

@realtime_api_bp.route("/api/emails/monitor/status", methods=["GET"])
def monitor_status():
    """Get email monitoring status."""
    monitor = get_email_monitor_service()
    return jsonify({
        "success": True,
        "monitoring": monitor.monitoring,
        "check_interval": monitor.check_interval
    })
