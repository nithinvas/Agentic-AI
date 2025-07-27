import base64
import json
import os
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.cloud import storage
import datetime
import traceback

from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
BUCKET_NAME = "projectraseedrawdata"
TOKEN_FILE = 'token.json'

def gmail_push(request):
    try:
        envelope = request.get_json(force=True)
        if not envelope.get('message') or not envelope['message'].get('data'):
            return ('Invalid Pub/Sub message format', 400)

        pubsub_message = envelope['message']
        data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        message_data = json.loads(data)

        user_id = message_data.get('emailAddress')
        print(f"Received Gmail Push for user {user_id}")

        service = authenticate()

        # ✅ Fetch latest email instead of using historyId
        results = service.users().messages().list(userId='me', labelIds=['INBOX'], maxResults=1).execute()
        messages = results.get('messages', [])

        if messages:
            msg_id = messages[0]['id']
            msg_detail = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
            upload_body_to_gcs(msg_detail)
            print("Fetched and uploaded the most recent email.")
        else:
            print("No new messages found.")

        return ('OK', 200)

    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return ('Internal Server Error', 500)


def authenticate():
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())



def upload_body_to_gcs(msg_detail):
    msg_id = msg_detail.get('id')
    subject = next((h['value'] for h in msg_detail['payload']['headers'] if h['name'] == 'Subject'), 'no-subject')
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')

    def extract_parts(part):
        body = ""
        if part.get('body', {}).get('data'):
            try:
                body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
            except Exception as e:
                body = f"[Decoding Error: {e}]"
        if 'parts' in part:
            for subpart in part['parts']:
                body += extract_parts(subpart)
        return body

    email_body = extract_parts(msg_detail.get('payload', {}))

    if not email_body:
        print("No readable body found")
        return

    file_name = f"email-{timestamp}-{msg_id}.html"

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    blob.upload_from_string(email_body, content_type='text/html')
    print(f"Uploaded email body to {file_name} in bucket {BUCKET_NAME}")


# def upload_body_to_gcs(msg_detail):
#     parts = msg_detail.get('payload', {}).get('parts', [])
#     msg_id = msg_detail.get('id')
#     subject = next((h['value'] for h in msg_detail['payload']['headers'] if h['name'] == 'Subject'), 'no-subject')
#     timestamp = datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')

#     email_body = ""
#     for part in parts:
#         mime_type = part.get('mimeType')
#         if mime_type in ['text/html', 'text/plain']:
#             data = part['body'].get('data')
#             if data:
#                 email_body = base64.urlsafe_b64decode(data).decode('utf-8')
#                 break

#     if not email_body:
#         print("No readable body found")
#         return

#     file_name = f"email-{timestamp}-{msg_id}.html"

#     storage_client = storage.Client()
#     bucket = storage_client.bucket(BUCKET_NAME)
#     blob = bucket.blob(file_name)
#     blob.upload_from_string(email_body, content_type='text/html')
#     print(f"Uploaded email body to {file_name} in bucket {BUCKET_NAME}")
