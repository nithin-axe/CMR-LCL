import json
import os
import threading
import time
from datetime import datetime
from flask import current_app
from app.services.common.websocket_service import get_websocket_service

class EmailMonitorService:
    """Monitors for new emails and triggers automatic classification."""
    
    def __init__(self):
        self.last_email_ids = set()
        self.monitoring = False
        self.monitor_thread = None
        self.check_interval = 5  # seconds
        self.scraped_emails_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails.json")
        )
    
    def start_monitoring(self):
        """Start the background email monitoring thread."""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        if current_app:
            current_app.logger.info("Email monitoring started")
    
    def stop_monitoring(self):
        """Stop the background monitoring thread."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
    
    def _monitor_loop(self):
        """Background loop that checks for new emails."""
        while self.monitoring:
            try:
                self._check_for_new_emails()
            except Exception as e:
                if current_app:
                    current_app.logger.error(f"Email monitoring error: {e}")
            
            time.sleep(self.check_interval)
    
    def _check_for_new_emails(self):
        """Check for new emails and notify subscribers."""
        if not os.path.exists(self.scraped_emails_path):
            return
        
        try:
            with open(self.scraped_emails_path, "r", encoding="utf-8") as f:
                emails = json.load(f)
        except Exception:
            return
        
        current_ids = {e.get("id") for e in emails if e.get("id")}
        new_ids = current_ids - self.last_email_ids
        
        if new_ids:
            self.last_email_ids = current_ids
            new_emails = [e for e in emails if e.get("id") in new_ids]
            
            # Broadcast to all connected clients
            ws_service = get_websocket_service()
            ws_service.broadcast_email_update(new_emails, "new_emails_arrived")
            
            if current_app:
                current_app.logger.info(f"Detected {len(new_emails)} new email(s)")
    
    def get_current_emails(self):
        """Get the current list of emails."""
        if not os.path.exists(self.scraped_emails_path):
            return []
        
        try:
            with open(self.scraped_emails_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

# Global instance
_monitor_service = EmailMonitorService()

def get_email_monitor_service():
    return _monitor_service
