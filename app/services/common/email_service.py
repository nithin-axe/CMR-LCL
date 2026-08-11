import imaplib
import email
from email.header import decode_header
import base64
import html as html_lib
import os
import urllib.parse
from flask import current_app
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials
from googleapiclient.discovery import build


def _b64url_decode(data):
    """Decode Gmail API's base64url-encoded (and possibly unpadded) data to raw bytes."""
    if not data:
        return b""
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def _format_size(num_bytes):
    """Render a byte count as a short human-readable label, e.g. '482 KB'."""
    try:
        num_bytes = float(num_bytes)
    except (TypeError, ValueError):
        return ""
    if num_bytes < 1024:
        return f"{int(num_bytes)} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.1f} MB"


class EmailService:
    def __init__(self, username=None, password=None, imap_server=None, imap_port=None, token_path=None, service_account_path=None):
        self.username = username
        self.password = password
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.token_path = token_path or "config/token.json"
        self.service_account_path = service_account_path

    def _get_credentials(self):
        username = self.username or current_app.config.get("EMAIL_USER")
        password = self.password or current_app.config.get("EMAIL_PASS")
        server = self.imap_server or current_app.config.get("IMAP_SERVER", "imap.gmail.com")
        port = self.imap_port or current_app.config.get("IMAP_PORT", 993)
        return username, password, server, port

    def _get_service_account_path(self):
        return self.service_account_path or current_app.config.get("GOOGLE_SERVICE_ACCOUNT_JSON", "config/service_account.json")

    def _get_gmail_service(self, scopes):
        """Build an authenticated Gmail API client, trying Service Account impersonation then User OAuth2."""
        sa_path = self._get_service_account_path()
        if os.path.exists(sa_path):
            try:
                creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
                delegated_creds = creds.with_subject("nl.importsea@shypplefresh.com")
                return build('gmail', 'v1', credentials=delegated_creds)
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"Service Account Gmail auth failed: {e}")

        if os.path.exists(self.token_path):
            try:
                creds = UserCredentials.from_authorized_user_file(self.token_path, scopes)
                return build('gmail', 'v1', credentials=creds)
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"User OAuth2 Gmail auth failed: {e}")

        return None

    def check_connection(self):
        """Verify connection using Service Account (delegation) or User OAuth2 or IMAP."""
        # 1. Try Service Account impersonation first if key is present
        sa_path = self._get_service_account_path()
        if os.path.exists(sa_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
                creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
                # Impersonate delegated mailbox
                delegated_creds = creds.with_subject("nl.importsea@shypplefresh.com")
                service = build('gmail', 'v1', credentials=delegated_creds)
                service.users().getProfile(userId='me').execute()
                return True, "Successfully authenticated via Service Account (impersonating nl.importsea@shypplefresh.com)."
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"Service Account Gmail verification failed: {e}")

        # 2. Fallback to User OAuth2
        if os.path.exists(self.token_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
                creds = UserCredentials.from_authorized_user_file(self.token_path, scopes)
                service = build('gmail', 'v1', credentials=creds)
                service.users().getProfile(userId='me').execute()
                return True, "Successfully authenticated with Gmail API (User OAuth2)."
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"User OAuth2 Gmail verification failed: {e}")

        # 3. Fallback to IMAP
        username, password, server, port = self._get_credentials()
        if not username or not password:
            return False, "No valid credentials/keys found to connect to Gmail."
        try:
            mail = imaplib.IMAP4_SSL(server, port)
            mail.login(username, password)
            mail.logout()
            return True, "Successfully connected and authenticated via IMAP."
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def get_user_email(self):
        """Get the email address of the authenticated account."""
        sa_path = self._get_service_account_path()
        if os.path.exists(sa_path):
            return "nl.importsea@shypplefresh.com"
            
        if os.path.exists(self.token_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
                creds = UserCredentials.from_authorized_user_file(self.token_path, scopes)
                service = build('gmail', 'v1', credentials=creds)
                profile = service.users().getProfile(userId='me').execute()
                return profile.get('emailAddress')
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"Failed to fetch Gmail profile email: {e}")
        return None

    def fetch_recent_emails(self, limit=200, label=None):
        """Fetch emails using Service Account impersonation, User OAuth2, or IMAP.

        Always prefers a live Gmail API / IMAP fetch so message IDs are real Gmail
        message IDs (required to later retrieve the full body/attachments). A
        Playwright-scraped cache is only used as a last-resort fallback below if
        every live retrieval method fails, since scraped IDs are synthetic hashes
        that the Gmail API cannot look up.
        """
        import json

        scraped_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "scraped_emails.json"))

        emails_list = []
        
        # 1. Try Service Account impersonation first
        sa_path = self._get_service_account_path()
        if os.path.exists(sa_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
                creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
                delegated_creds = creds.with_subject("nl.importsea@shypplefresh.com")
                service = build('gmail', 'v1', credentials=delegated_creds)
                
                q = f'label:"{label}" OR label:{label.replace(" ", "-")}' if label else None
                results = service.users().messages().list(userId='me', maxResults=limit, q=q).execute()
                messages = results.get('messages', [])
                
                for msg in messages:
                    msg_detail = service.users().messages().get(userId='me', id=msg['id']).execute()
                    payload = msg_detail.get('payload', {})
                    headers = payload.get('headers', [])
                    snippet = msg_detail.get('snippet', '')
                    
                    subject = "No Subject"
                    from_sender = "Unknown"
                    date = ""
                    for h in headers:
                        if h['name'].lower() == 'subject':
                            subject = h['value']
                        elif h['name'].lower() == 'from':
                            from_sender = h['value']
                        elif h['name'].lower() == 'date':
                            date = h['value']
                            
                    label_ids = msg_detail.get('labelIds', [])
                    emails_list.append({
                        "id": msg['id'],
                        "subject": subject,
                        "from": from_sender,
                        "date": date,
                        "snippet": snippet,
                        "unread": "UNREAD" in label_ids,
                        "starred": "STARRED" in label_ids
                    })
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"Service Account Gmail API fetch failed: {e}")

        # 2. Fallback to User OAuth2
        if not emails_list and os.path.exists(self.token_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
                creds = UserCredentials.from_authorized_user_file(self.token_path, scopes)
                service = build('gmail', 'v1', credentials=creds)
                
                q = f'label:"{label}" OR label:{label.replace(" ", "-")}' if label else None
                results = service.users().messages().list(userId='me', maxResults=limit, q=q).execute()
                messages = results.get('messages', [])
                
                for msg in messages:
                    msg_detail = service.users().messages().get(userId='me', id=msg['id']).execute()
                    payload = msg_detail.get('payload', {})
                    headers = payload.get('headers', [])
                    snippet = msg_detail.get('snippet', '')
                    
                    subject = "No Subject"
                    from_sender = "Unknown"
                    date = ""
                    for h in headers:
                        if h['name'].lower() == 'subject':
                            subject = h['value']
                        elif h['name'].lower() == 'from':
                            from_sender = h['value']
                        elif h['name'].lower() == 'date':
                            date = h['value']
                            
                    label_ids = msg_detail.get('labelIds', [])
                    emails_list.append({
                        "id": msg['id'],
                        "subject": subject,
                        "from": from_sender,
                        "date": date,
                        "snippet": snippet,
                        "unread": "UNREAD" in label_ids,
                        "starred": "STARRED" in label_ids
                    })
            except Exception as e:
                if 'current_app' in globals() and current_app:
                    current_app.logger.warning(f"User Gmail API fetch failed: {e}")

        # 3. Prefer the Playwright-scraped cache over IMAP. For a delegated mailbox
        # (the primary use case here) the Gmail API can't see the label and the IMAP
        # credentials don't apply, so hitting IMAP just spams AUTHENTICATIONFAILED
        # errors and adds seconds of latency to every poll. The scraped cache, kept
        # fresh by scripts/open_gmail.py, is the real live source for that mailbox.
        if not emails_list and os.path.exists(scraped_path):
            try:
                with open(scraped_path, "r", encoding="utf-8") as f:
                    emails_list = json.load(f)
            except Exception:
                pass

        # 4. Last resort: IMAP (only when there was no cache at all).
        if not emails_list:
            username, password, server, port = self._get_credentials()
            if username and password:
                try:
                    mail = imaplib.IMAP4_SSL(server, port)
                    mail.login(username, password)
                    
                    folder = label if label else "inbox"
                    try:
                        status, _ = mail.select(folder)
                        if status != "OK":
                            status, _ = mail.select("inbox")
                    except Exception:
                        mail.select("inbox")
                    
                    status, messages = mail.search(None, "ALL")
                    if status == "OK":
                        mail_ids = messages[0].split()
                        recent_ids = mail_ids[-limit:] if len(mail_ids) > limit else mail_ids
                        recent_ids.reverse()
                        
                        for m_id in recent_ids:
                            status, data = mail.fetch(m_id, "(RFC822)")
                            if status != "OK":
                                continue
                            for response_part in data:
                                if isinstance(response_part, tuple):
                                    msg = email.message_from_bytes(response_part[1])
                                    
                                    subject_parts = decode_header(msg.get("Subject") or "")
                                    subject = ""
                                    for part, encoding in subject_parts:
                                        if isinstance(part, bytes):
                                            subject += part.decode(encoding or "utf-8", errors="ignore")
                                        else:
                                            subject += part
                                            
                                    from_parts = decode_header(msg.get("From") or "")
                                    from_sender = ""
                                    for part, encoding in from_parts:
                                        if isinstance(part, bytes):
                                            from_sender += part.decode(encoding or "utf-8", errors="ignore")
                                        else:
                                            from_sender += part
                                    
                                    date = msg.get("Date")
                                    emails_list.append({
                                        "id": m_id.decode(),
                                        "subject": subject,
                                        "from": from_sender,
                                        "date": date,
                                        "snippet": "",
                                        "unread": False,
                                        "starred": False
                                    })
                        mail.logout()
                except Exception as e:
                    if 'current_app' in globals() and current_app:
                        current_app.logger.error(f"Error fetching emails via IMAP: {str(e)}")
                    else:
                        print(f"Error fetching emails via IMAP: {str(e)}")

        # Note: the scraped cache (loaded in step 3 above) is owned by
        # scripts/open_gmail.py - it's the only way to see mail in a delegated mailbox
        # the Gmail API can't reach. fetch_recent_emails must never write to it, or it
        # would clobber that richer scraped data (unread/starred/attachment flags).
        return emails_list[:limit]

    def send_email(self, to, subject, body):
        import base64
        from email.mime.text import MIMEText
        
        # Try service account first
        sa_path = self._get_service_account_path()
        if os.path.exists(sa_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.send"]
                creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
                delegated_creds = creds.with_subject("nl.importsea@shypplefresh.com")
                service = build('gmail', 'v1', credentials=delegated_creds)
                
                message = MIMEText(body)
                message['to'] = to
                message['subject'] = subject
                raw = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
                service.users().messages().send(userId='me', body={'raw': raw}).execute()
                return True, "Email sent successfully via Service Account."
            except Exception as e:
                pass
                
        # Try OAuth2
        if os.path.exists(self.token_path):
            try:
                scopes = ["https://www.googleapis.com/auth/gmail.send"]
                creds = UserCredentials.from_authorized_user_file(self.token_path, scopes)
                service = build('gmail', 'v1', credentials=creds)
                
                message = MIMEText(body)
                message['to'] = to
                message['subject'] = subject
                raw = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
                service.users().messages().send(userId='me', body={'raw': raw}).execute()
                return True, "Email sent successfully via User OAuth2."
            except Exception as e:
                pass
                
        # Try SMTP fallback
        username, password, _, _ = self._get_credentials()
        if username and password:
            try:
                import smtplib
                from email.mime.text import MIMEText
                msg = MIMEText(body)
                msg['Subject'] = subject
                msg['From'] = username
                msg['To'] = to
                
                server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
                server.login(username, password)
                server.sendmail(username, [to], msg.as_string())
                server.quit()
                return True, "Email sent successfully via SMTP."
            except Exception as e:
                return False, f"SMTP send failed: {str(e)}"
                
        return False, "No valid credentials found to send email."

    def modify_email_unread(self, message_id, unread=True):
        service = self._get_gmail_service(["https://www.googleapis.com/auth/gmail.modify"])
        if not service:
            return False, "No Gmail credentials with modify access are available."
        try:
            body = {
                "addLabelIds": ["UNREAD"] if unread else [],
                "removeLabelIds": [] if unread else ["UNREAD"]
            }
            service.users().messages().batchModify(userId='me', ids=[message_id], body=body).execute()
            return True, f"Email marked as {'unread' if unread else 'read'}."
        except Exception as e:
            return False, str(e)

    def set_starred(self, message_id, starred=True):
        service = self._get_gmail_service(["https://www.googleapis.com/auth/gmail.modify"])
        if not service:
            return False, "No Gmail credentials with modify access are available."
        try:
            body = {
                "addLabelIds": ["STARRED"] if starred else [],
                "removeLabelIds": [] if starred else ["STARRED"]
            }
            service.users().messages().modify(userId='me', id=message_id, body=body).execute()
            return True, f"Email {'starred' if starred else 'unstarred'}."
        except Exception as e:
            return False, str(e)

    def archive_email(self, message_id):
        service = self._get_gmail_service(["https://www.googleapis.com/auth/gmail.modify"])
        if not service:
            return False, "No Gmail credentials with modify access are available."
        try:
            service.users().messages().modify(userId='me', id=message_id, body={"removeLabelIds": ["INBOX"]}).execute()
            return True, "Email archived."
        except Exception as e:
            return False, str(e)

    def trash_email(self, message_id):
        service = self._get_gmail_service(["https://www.googleapis.com/auth/gmail.modify"])
        if not service:
            return False, "No Gmail credentials with modify access are available."
        try:
            service.users().messages().trash(userId='me', id=message_id).execute()
            return True, "Email moved to trash."
        except Exception as e:
            return False, str(e)

    def get_email_full(self, message_id):
        """Fetch the complete HTML body and attachment list for a message via the Gmail API.

        Returns None if no Gmail credentials are configured, or raises if the API call fails.
        """
        service = self._get_gmail_service(["https://www.googleapis.com/auth/gmail.readonly"])
        if not service:
            return None

        msg = service.users().messages().get(userId='me', id=message_id, format='full').execute()
        payload = msg.get('payload', {})

        html_parts = []
        plain_parts = []
        attachments = []

        def walk(part):
            mime_type = part.get('mimeType', '')
            body = part.get('body', {}) or {}
            filename = part.get('filename') or ''
            headers = {h['name'].lower(): h['value'] for h in part.get('headers', []) or []}
            content_id = headers.get('content-id', '').strip('<>')

            if filename and body.get('attachmentId'):
                attachments.append({
                    "filename": filename,
                    "size": _format_size(body.get('size', 0)),
                    "attachmentId": body['attachmentId'],
                    "contentId": content_id,
                })
            elif mime_type == 'text/html' and body.get('data'):
                html_parts.append(_b64url_decode(body['data']).decode('utf-8', errors='ignore'))
            elif mime_type == 'text/plain' and body.get('data'):
                plain_parts.append(_b64url_decode(body['data']).decode('utf-8', errors='ignore'))

            for sub_part in part.get('parts', []) or []:
                walk(sub_part)

        walk(payload)

        html_body = "".join(html_parts)
        if not html_body and plain_parts:
            text = "".join(plain_parts)
            html_body = f"<pre style=\"white-space:pre-wrap;font-family:inherit;margin:0;\">{html_lib.escape(text)}</pre>"

        # Build the download/view URL for each attachment, and rewrite cid: references
        # (inline images) in the body to point at the inline (non-forced-download) URL.
        for att in attachments:
            encoded_name = urllib.parse.quote(att['filename'])
            inline_url = f"/api/emails/attachment/{message_id}/{att['attachmentId']}?filename={encoded_name}"
            if att.get('contentId'):
                att['url'] = inline_url
                html_body = html_body.replace(f"cid:{att['contentId']}", inline_url)
            else:
                att['url'] = f"{inline_url}&download=1"

        return {"body": html_body, "attachments": attachments}

    def get_attachment(self, message_id, attachment_id):
        """Fetch the raw bytes of a message attachment via the Gmail API."""
        service = self._get_gmail_service(["https://www.googleapis.com/auth/gmail.readonly"])
        if not service:
            return None

        att = service.users().messages().attachments().get(
            userId='me', messageId=message_id, id=attachment_id
        ).execute()
        return _b64url_decode(att.get('data', ''))
