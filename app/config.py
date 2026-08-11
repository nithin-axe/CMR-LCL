import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
    
    # Gemini AI
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
    
    # Email Credentials
    EMAIL_USER = os.getenv("EMAIL_USER", "deepak.indrakumar@shypple.com")
    EMAIL_PASS = os.getenv("EMAIL_PASS")
    IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
    IMAP_PORT = int(os.getenv("IMAP_PORT", 993))
    
    # Google Sheets Configuration
    GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "config/service_account.json")
    SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "India Tracking Sheets")
    
    # Flask options
    PORT = int(os.getenv("PORT", 5000))
    DEBUG = os.getenv("FLASK_ENV") == "development"
