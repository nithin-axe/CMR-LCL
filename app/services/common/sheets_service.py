import os
import gspread
from google.oauth2.service_account import Credentials as ServiceCredentials
from google.oauth2.credentials import Credentials as UserCredentials
from flask import current_app

class SheetsService:
    def __init__(self, credentials_path=None, token_path=None, spreadsheet_name=None):
        self.credentials_path = credentials_path
        self.token_path = token_path or "config/token.json"
        self.spreadsheet_name = spreadsheet_name
        self.client = None

    def _get_config(self):
        path = self.credentials_path or current_app.config.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        name = self.spreadsheet_name or current_app.config.get("SPREADSHEET_NAME", "India Tracking Sheets")
        return path, name

    def authenticate(self):
        """Authenticate with Google Sheets API using OAuth2 user token or service account."""
        if self.client:
            return True
            
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Try OAuth2 user token
        if os.path.exists(self.token_path):
            try:
                creds = UserCredentials.from_authorized_user_file(self.token_path, scopes)
                self.client = gspread.authorize(creds)
                return True
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"OAuth2 user token auth failed, trying service account: {e}")
        
        # Fallback to Service Account
        path, _ = self._get_config()
        if path and os.path.exists(path):
            try:
                creds = ServiceCredentials.from_service_account_file(path, scopes=scopes)
                self.client = gspread.authorize(creds)
                return True
            except Exception as e:
                raise e
            
        raise FileNotFoundError(
            "Neither Google service account JSON nor active OAuth2 user token.json was found. "
            "Please log in via the dashboard."
        )

    def check_connection(self):
        """Check if service account or user token is present and valid."""
        try:
            self.authenticate()
            return True, "Authenticated successfully with Google Sheets API."
        except Exception as e:
            return False, f"Authentication failed: {str(e)}"

    def get_sheet_data(self, worksheet_name=None):
        """Read data from the configured spreadsheet."""
        self.authenticate()
        _, name = self._get_config()
        
        try:
            spreadsheet = self.client.open(name)
            if worksheet_name:
                worksheet = spreadsheet.worksheet(worksheet_name)
            else:
                worksheet = spreadsheet.get_worksheet(0)
            return worksheet.get_all_records()
        except Exception as e:
            if 'current_app' in globals() and current_app:
                current_app.logger.error(f"Error reading Google Sheet: {str(e)}")
            raise e

    def append_row(self, row_data, worksheet_name=None):
        """Append a row of data to the spreadsheet."""
        self.authenticate()
        _, name = self._get_config()
        
        try:
            spreadsheet = self.client.open(name)
            if worksheet_name:
                worksheet = spreadsheet.worksheet(worksheet_name)
            else:
                worksheet = spreadsheet.get_worksheet(0)
            worksheet.append_row(row_data)
            return True
        except Exception as e:
            if 'current_app' in globals() and current_app:
                current_app.logger.error(f"Error appending to Google Sheet: {str(e)}")
            raise e
