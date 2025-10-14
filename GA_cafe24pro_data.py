import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

# Google Analytics Data API v1beta 사용
SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

# ✅ 변경된 부분: 윈도우 절대경로 → OS 공통 경로
# GitHub Actions에서는 run-ga.yml이 ./client_secret.json, ./ga_token.json 파일을 만들어줍니다.
CLIENT_SECRET_FILE = os.getenv("GA_CLIENT_SECRET_PATH", "./client_secret.json")
TOKEN_FILE = os.getenv("GA_TOKEN_PATH", "./ga_token.json")

PROPERTY_ID = "464149233"
SEARCH_TERMS_SHEET_ID = "1vxP7tVII0oWaGtro8puSXy7lDvYDrnppaRPv2qFACm0"


def get_credentials():
    creds = None

    # 토큰 파일이 존재하면 로드
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print("기존 토큰 파일에서 인증 정보를 로드했습니다.")
        except Exception as e:
            print(f"토큰 파일 로드 중 오류: {e}")
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
                print("손상된 토큰 파일을 삭제했습니다.")

    # 토큰이 없거나 유효하지 않은 경우
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("토큰이 만료되었습니다. 갱신을 시도합니다...")
                creds.refresh(Request())
                print("토큰 갱신이 완료되었습니다.")
            except Exception as e:
                print(f"토큰 갱신 실패: {e}")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                    print("만료된 토큰 파일을 삭제했습니다.")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                print("새로운 인증이 완료되었습니다.")
        else:
            print("새로운 인증을 시작합니다...")

            # ✅ CI 환경(GitHub Actions)에서는 브라우저 인증 불가
            if os.getenv("GITHUB_ACTIONS") == "true":
                raise RuntimeError(
                    "GA_TOKEN_JSON secret 필요: Actions에서는 브라우저 인증(InstalledAppFlow)을 사용할 수 없습니다."
                )

            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            print("인증이 완료되었습니다.")

        # 토큰 저장
        try:
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            print(f"토큰이 {TOKEN_FILE}에 저장되었습니다.")
        except Exception as e:
            print(f"토큰 저장 중 오류: {e}")

    return creds


def get_search_terms_from_sheet():
    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="B:B"
    ).execute()

    values = result.get('values', [])
    search_terms = []

    for i, row in enumerate(values):
        if i == 0:
            continue
        if row and row[0].strip():
            search_terms.append(row[0].strip())

    print(f"구글 시트에서 {len(search_terms)}개의 검색어를 가져왔습니다.")
    return search_terms


def get_analytics_data_for_search_term(search_term, start_date, end_date):
    creds = get_credentials()
    analytics = build('analyticsdata', 'v1beta', credentials=creds)

    filter_value = search_term
    match_type = "EXACT"

    request_body = {
        "dateRanges": [
            {"startDate": start_date, "endDate": end_date}
        ],
        "metrics": [
            {"name": "eventCount"}
        ],
        "dimensions": [
            {"name": "sessionSource"},
            {"name": "eventName"}
        ],
        "dimensionFilter": {
            "andGroup": {
                "expressions": [
                    {
                        "filter": {
                            "fieldName": "eventName",
                            "stringFilter": {
                                "matchType": "EXACT",
                                "value": "click"
                            }
                        }
                    },
                    {
                        "filter": {
                            "fieldName": "sessionSource",
                            "stringFilter": {
                                "matchType": match_type,
                                "value": filter_value
                            }
                        }
                    }
                ]
            }
        },
        "limit": 1000
    }

    try:
        response = analytics.properties().runReport(
            property=f"properties/{PROPERTY_ID}",
            body=request_body
        ).execute()
        return response
    except Exception as e:
        print(f"검색어 '{search_term}'에 대한 데이터 조회 중 오류: {e}")
        return None


def find_today_column(sheets_service):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="1:1"
    ).execute()

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
            spreadsheetId=SEARCH_TERMS_SHEET_ID,
            range=cell_range,
            valueInputOption='USER_ENTERED',
            body={'values': [[click_count]]}
        ).execute()
        print(f"✓ '{search_term}': {click_count} → {cell_range}")
        return True
    except Exception as e:
        print(f"✗ '{search_term}' 기록 실패: {e}")
        return False


def main():
    start_date = "2025-02-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"Google Analytics 데이터 수집: {start_date} ~ {end_date}")

    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)

    today_column = find_today_column(sheets_service)
    if not today_column:
        print("오늘 날짜 열을 찾을 수 없어 업데이트를 중단합니다.")
        return

    search_terms = get_search_terms_from_sheet()
    if not search_terms:
        print("검색어를 가져올 수 없습니다.")
        return

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="B:B"
    ).execute()
    values = result.get('values', [])

    additional_values = {
        'sellerocean': 6,
        'sba': 22,
        'd2c': 1,
        'etc': 2,
        'closet': 11,
        'salecafe': 6
    }

    print("\n=== 검색어별 클릭 이벤트 수 ===")
    print("검색어\t\t클릭 이벤트 수")
    print("-" * 40)

    for i, search_term in enumerate(search_terms):
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
            update_single_cell(sheets_service, search_term, total_clicks, today_column, actual_row_number)

    print("\n모든 데이터 기록 완료!")


if __name__ == "__main__":
    main()
