import json
import threading
from datetime import datetime
from flask import current_app

class WebSocketService:
    """Manages real-time email updates via Server-Sent Events (SSE)."""
    
    def __init__(self):
        self.subscribers = set()
        self.lock = threading.Lock()
    
    def subscribe(self, client_id):
        """Register a client for real-time updates."""
        with self.lock:
            self.subscribers.add(client_id)
    
    def unsubscribe(self, client_id):
        """Unregister a client."""
        with self.lock:
            self.subscribers.discard(client_id)
    
    def broadcast_email_update(self, emails, event_type="email_list_updated"):
        """Notify all connected clients of email list changes."""
        message = {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "emails": emails,
            "count": len(emails)
        }
        self._broadcast(message)
    
    def broadcast_classification_update(self, classifications, event_type="classification_updated"):
        """Notify all connected clients of document type classifications."""
        message = {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "classifications": classifications
        }
        self._broadcast(message)
    
    def _broadcast(self, message):
        """Send message to all subscribers."""
        with self.lock:
            # In a production setup, this would use a message queue (Redis, RabbitMQ)
            # For now, we store the latest message for new subscribers
            self.last_message = message

# Global instance
_ws_service = WebSocketService()

def get_websocket_service():
    return _ws_service
