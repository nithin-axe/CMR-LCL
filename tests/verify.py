import os
import sys
from dotenv import load_dotenv

# Ensure the root directory is in the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.llm.llm_client import GeminiClient
from app.services.common.email_service import EmailService
from app.services.common.sheets_service import SheetsService

def main():
    print("--- VERIFICATION START ---")
    load_dotenv()
    
    from app import create_app
    app = create_app()
    
    with app.app_context():
        # 1. Gemini AI
        print("\n[1] Testing Gemini LLM connectivity...")
        api_key = os.getenv("GEMINI_API_KEY")
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
        print(f"Using Model: {model}")
        try:
            client = GeminiClient(api_key=api_key, model_name=model)
            resp = client.generate("Hello from India Filling Hub test script!")
            print("Gemini response:")
            print(resp)
            print("-> SUCCESS")
        except Exception as e:
            print(f"-> FAILED: {str(e)}")

        # 2. Email IMAP
        print("\n[2] Testing Email IMAP connectivity...")
        user = os.getenv("EMAIL_USER")
        pwd = os.getenv("EMAIL_PASS")
        print(f"Attempting login for: {user}")
        try:
            service = EmailService(username=user, password=pwd)
            ok, msg = service.check_connection()
            if ok:
                print(f"-> SUCCESS: {msg}")
                emails = service.fetch_recent_emails(limit=3)
                print(f"Fetched {len(emails)} recent emails:")
                for email in emails:
                    print(f"  - From: {email['from']} | Subj: {email['subject']}")
            else:
                print(f"-> FAILED: {msg}")
        except Exception as e:
            print(f"-> FAILED: {str(e)}")

        # 3. Google Sheets
        print("\n[3] Testing Google Sheets connectivity...")
        sheets_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        print(f"Service account path: {sheets_path}")
        try:
            service = SheetsService(credentials_path=sheets_path)
            ok, msg = service.check_connection()
            if ok:
                print(f"-> SUCCESS: {msg}")
            else:
                print(f"-> FAILED (Expected if credentials.json missing): {msg}")
        except Exception as e:
            print(f"-> FAILED (Expected if credentials.json missing): {str(e)}")


    print("\n--- VERIFICATION COMPLETE ---")

if __name__ == "__main__":
    main()
