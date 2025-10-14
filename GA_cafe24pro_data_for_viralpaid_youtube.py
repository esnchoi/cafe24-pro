import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly', 'https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = os.getenv("GA_CLIENT_SECRET_PATH", "./client_secret.json")
TOKEN_FILE = os.getenv("GA_TOKEN_PATH", "./ga_token.json")
PROPERTY_ID = "464149233"
SEARCH_TERMS_SHEET_ID = "1vxP7tVII0oWaGtro8puSXy7lDvYDrnppaRPv2qFACm0"

def get_credentials():
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print("기존 토큰 파일에서 인증 정보를 로드했습니다.")
        except Exception as e:
            print(f"토큰 파일 로드 중 오류: {e}")
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
                print("손상된 토큰 파일을 삭제했습니다.")
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("토큰이 만료되었습니다. 갱신을 시도합니다...")
                creds.refresh(Request())
                print("토큰 갱신이 완료되었습니다.")
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                print(f"갱신된 토큰이 {TOKEN_FILE}에 저장되었습니다.")
            except Exception as e:
                print(f"토큰 갱신 실패: {e}")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                    print("만료된 토큰 파일을 삭제했습니다.")
                if os.getenv("GITHUB_ACTIONS") == "true":
                    raise RuntimeError("GA_TOKEN_JSON secret 업데이트 필요: 토큰 갱신에 실패했습니다.")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                print("새로운 인증이 완료되었습니다.")
        else:
            print("새로운 인증을 시작합니다...")
            if os.getenv("GITHUB_ACTIONS") == "true":
                raise RuntimeError("GA_TOKEN_JSON secret 필요: Actions에서는 브라우저 인증(InstalledAppFlow)을 사용할 수 없습니다.")
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            print("인증이 완료되었습니다.")
        try:
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            print(f"토큰이 {TOKEN_FILE}에 저장되었습니다.")
        except Exception as e:
            print(f"토큰 저장 중 오류: {e}")
    return creds

def get_analytics_data_for_search_term(search_term, start_date, end_date):
    creds = get_credentials()
    analytics = build('analyticsdata', 'v1beta', credentials=creds)
    filter_value = search_term
    match_type = "EXACT"
    request_body = {
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": [{"name": "eventCount"}],
        "dimensions": [{"name": "sessionSourceMedium"}, {"name": "eventName"}],
        "dimensionFilter": {
            "andGroup": {
                "expressions": [
                    {"filter": {"fieldName": "eventName", "stringFilter": {"matchType": "EXACT", "value": "click"}}},
                    {"filter": {"fieldName": "sessionSourceMedium", "stringFilter": {"matchType": match_type, "value": filter_value}}}
                ]
            }
        },
        "limit": 1000
    }
    try:
        response = analytics.properties().runReport(property=f"properties/{PROPERTY_ID}", body=request_body).execute()
        return response
    except Exception as e:
        print(f"검색어 '{search_term}'에 대한 데이터 조회 중 오류: {e}")
        return None

def find_today_column(sheets_service):
    result = sheets_service.spreadsheets().values().get(spreadsheetId=SEARCH_TERMS_SHEET_ID, range="1:1").execute()
    values = result.get('values', [[]])
    if not values or not values[0]:
        print("헤더 행을 찾을 수 없습니다.")
        return None
    today = datetime.now().strftime("%Y-%m-%d")
    today_short = datetime.now().strftime("%m/%d")
    today_dot = datetime.now().strftime("%Y.%m.%d")
    date_formats = [today, today_short, today_dot]
    for i, cell in enumerate(values[0]):
        if cell:
            cell_str = str(cell).strip()
            for date_format in date_formats:
                if date_format in cell_str:
                    col_letter = chr(65 + i) if i < 26 else chr(64 + i // 26) + chr(65 + i % 26)
                    print(f"오늘 날짜 열 발견: {col_letter}1 ({cell_str})")
                    return col_letter
    print(f"오늘 날짜({today})에 해당하는 열을 찾을 수 없습니다.")
    return None

def update_single_cell(sheets_service, search_term, click_count, today_column, row_number):
    try:
        cell_range = f"{today_column}{row_number}"
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SEARCH_TERMS_SHEET_ID, range=cell_range,
            valueInputOption='USER_ENTERED', body={'values': [[click_count]]}
        ).execute()
        print(f"✓ '{search_term}': {click_count} → {cell_range}")
        return True
    except Exception as e:
        print(f"✗ '{search_term}' 기록 실패: {e}")
        return False

def main():
    start_date = "2025-02-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Google Analytics 데이터 수집 (viral/paid_youtube): {start_date} ~ {end_date}")
    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)
    today_column = find_today_column(sheets_service)
    if not today_column:
        print("오늘 날짜 열을 찾을 수 없어 업데이트를 중단합니다.")
        return
    search_terms = ["viral / paid_youtube"]
    result = sheets_service.spreadsheets().values().get(spreadsheetId=SEARCH_TERMS_SHEET_ID, range="B:B").execute()
    values = result.get('values', [])
    additional_values = {'sellerocean': 6, 'sba': 22, 'd2c': 1, 'etc': 2, 'closet': 11, 'salecafe': 6}
    print("\n=== viral / paid_youtube 클릭 이벤트 수 ===")
    print("검색어\t\t\t클릭 이벤트 수")
    print("-" * 50)
    success_count = 0
    fail_count = 0
    for search_term in search_terms:
        print(f"'{search_term}' 조회 중...")
        response = get_analytics_data_for_search_term(search_term, start_date, end_date)
        if response and 'rows' in response:
            total_clicks = 0
            for row in response['rows']:
                clicks = int(row['metricValues'][0]['value'])
                total_clicks += clicks
        else:
            total_clicks = 0
        if search_term in additional_values:
            total_clicks += additional_values[search_term]
        print(f"{search_term}\t\t{total_clicks}")
        actual_row_number = None
        for row_idx, row in enumerate(values):
            if row and row[0].strip() == search_term:
                actual_row_number = row_idx + 1
                break
        if actual_row_number:
            if update_single_cell(sheets_service, search_term, total_clicks, today_column, actual_row_number):
                success_count += 1
            else:
                fail_count += 1
    print(f"\n데이터 기록 완료! (성공: {success_count}, 실패: {fail_count})")
    if fail_count > 0:
        exit(1)

if __name__ == "__main__":
    main()
