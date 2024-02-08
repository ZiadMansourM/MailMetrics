import time
import os.path
import operator
import pickle
from collections import defaultdict

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from rich.console import Console
from rich.progress import Progress

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
TOKEN_FILE = 'token.pickle'
CREDENTIALS_FILE = 'credentials.json'

# Initialize rich console
console = Console()

def load_credentials():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            return pickle.load(token)
    return None

def save_credentials(creds):
    with open(TOKEN_FILE, 'wb') as token:
        pickle.dump(creds, token)

def refresh_credentials(creds):
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        return creds
    return None

def get_new_credentials():
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    return flow.run_local_server(port=0)

def get_gmail_service():
    creds = load_credentials()
    creds = refresh_credentials(creds) if not creds or not creds.valid else creds
    creds = get_new_credentials() if not creds else creds
    save_credentials(creds)
    return build('gmail', 'v1', credentials=creds)

def process_messages(service, request, sender_count, progress):
    i: int = 0
    while request is not None:
        response = request.execute()
        messages = response.get('messages', [])
        task = progress.add_task(f"[green]Processing Page {i}", total=len(messages))
        i += 1
        for message in messages:
            process_message(service, message, sender_count, task, progress)
        request = service.users().messages().list_next(request, response)

def process_message(service, message, sender_count, task, progress):
    try:
        msg = service.users().messages().get(
            userId='me', 
            id=message['id'], 
            format='metadata', 
            metadataHeaders=['From']
        ).execute()
        headers = msg['payload']['headers']
        sender = next(
            (header['value'] for header in headers if header['name'] == 'From'), 
            None
        )
        if sender is None:
            console.print(f"Could not find sender for message {message['id']}")
            return
        sender_count[sender] += 1
        progress.update(task, advance=1)
    except Exception as e:
        console.print(f"Error processing message {message['id']}: {e}")

def main():
    start_time = time.time()

    service = get_gmail_service()
    request = service.users().messages().list(userId='me', q="is:unread")
    sender_count = defaultdict(int)

    profile = service.users().getProfile(userId='me').execute()
    total_messages = profile['messagesTotal']
    console.print(f"Total messages: [bold]{total_messages}[/bold]")

    with Progress() as progress:
        process_messages(service, request, sender_count, progress)

    sorted_senders = sorted(
        sender_count.items(), 
        key=operator.itemgetter(1), 
        reverse=True
    )

    with open('output.txt', 'w') as f:
        f.write(f"Total number of emails: {sum(sender_count.values()):,}\n")
        f.write(f"Number of unique senders: {len(sender_count):,}\n")
        f.write("Number of emails per sender:\n")
        for sender, count in sorted_senders:
            f.write(f"{sender}: {count:,}\n")
    
    end_time = time.time()
    total_time = end_time - start_time
    console.print(f"Total execution time: [bold]{total_time:,.2f}[/bold] seconds")

if __name__ == '__main__':
    main()
