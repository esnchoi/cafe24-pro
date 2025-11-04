import os
import time
import re
from urllib.parse import unquote
import requests
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# 환경변수(.env) 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = os.getenv('CLIENT_SECRET_FILE')
TOKEN_FILE = os.getenv('TOKEN_FILE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
WIKI_URL = os.getenv('WIKI_URL')
WIKI_PAGE_ID = os.getenv('WIKI_PAGE_ID')
WIKI_USERNAME = os.getenv('WIKI_USERNAME')
WIKI_PASSWORD = os.getenv('WIKI_PASSWORD')

def get_credentials():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return creds

def parse_channel(text):
    m = re.match(r'^\(([^)]+)\)\s*(.+)$', text)
    return (m.group(2), m.group(1)) if m else (text, None)

def get_wiki_pr_data():
    url = f"{WIKI_URL}/rest/api/content/{WIKI_PAGE_ID}?expand=body.storage"
    r = requests.get(url, auth=(WIKI_USERNAME, WIKI_PASSWORD))
    html = r.json()['body']['storage']['value']
    soup = BeautifulSoup(html, 'html.parser')
    pr_data = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'utm_campaign=pr' in href:
            keyword = re.search(r'utm_source=([^&]+)', href)
            keyword = unquote(keyword.group(1)) if keyword else None
            channel_text = a.get_text(strip=True)
            if keyword and channel_text:
                a_txt, c_txt = parse_channel(channel_text)
                pr_data.append((keyword, a_txt, c_txt))
    return pr_data

def get_first_empty_row(service, col='B'):
    res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=f"{col}:{col}").execute()
    vals = res.get('values', [])
    for i, v in enumerate(vals[1:], start=2):
        if not v or not v[0].strip():
            return i
    return len(vals) + 1

def keyword_exists(service, keyword):
    res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range="B:B").execute()
    for row in res.get('values', []):
        if row and row[0] == keyword:
            return True
    return False

def add_to_sheet(service, keyword, a_txt, c_txt, row):
    vals = [[a_txt or '', keyword, c_txt or '']]
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"A{row}:C{row}",
        valueInputOption='USER_ENTERED',
        body={'values': vals}
    ).execute()

def main():
    creds = get_credentials()
    sheets = build('sheets', 'v4', credentials=creds)
    pr_data = get_wiki_pr_data()
    for keyword, a_txt, c_txt in pr_data:
        if not keyword_exists(sheets, keyword):
            row = get_first_empty_row(sheets, 'B')
            add_to_sheet(sheets, keyword, a_txt, c_txt, row)
            time.sleep(0.5)
    print("완료")

if __name__ == '__main__':
    main()
