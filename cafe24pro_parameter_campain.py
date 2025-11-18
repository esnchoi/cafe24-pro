import os
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Google Analytics Data API v1beta 사용
SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

# === 기존 코드와 동일한 방식 (환경변수 + 기본값) ===
CLIENT_SECRET_FILE = os.getenv("GA_CLIENT_SECRET_PATH", "./client_secret.json")
TOKEN_FILE = os.getenv("GA_TOKEN_PATH", "./ga_token.json")

# === GA4 Property / 시트 ID도 기존 스타일대로 하드코딩 ===
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

                # 갱신된 토큰 저장
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                print(f"갱신된 토큰이 {TOKEN_FILE}에 저장되었습니다.")

            except Exception as e:
                print(f"토큰 갱신 실패: {e}")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                    print("만료된 토큰 파일을 삭제했습니다.")

                # CI 환경(GitHub Actions)에서는 브라우저 인증 불가
                if os.getenv("GITHUB_ACTIONS") == "true":
                    raise RuntimeError(
                        "GA_TOKEN_JSON secret 업데이트 필요: 토큰 갱신에 실패했습니다."
                    )

                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                print("새로운 인증이 완료되었습니다.")
        else:
            print("새로운 인증을 시작합니다...")

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


def get_keyword_utm_pairs_from_sheet():
    """
    구글 시트에서 B열(키워드), E열(UTM 캠페인)을 함께 가져와서
    E열이 비어있지 않은 행만 리스트로 반환
    """
    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="B:E"  # B~E열
    ).execute()

    values = result.get('values', [])
    keyword_utm_pairs = []

    for i, row in enumerate(values):
        if i == 0:
            # 헤더 행 스킵
            continue

        # E열(인덱스 3)에 값이 있는 경우만 사용
        if len(row) > 3 and row[3].strip():
            keyword = row[0].strip() if row[0] else None  # B열
            utm_campaign = row[3].strip()                # E열
            if keyword and utm_campaign:
                keyword_utm_pairs.append({
                    "keyword": keyword,
                    "utm_campaign": utm_campaign,
                    "row_number": i + 1  # 시트 실제 행 번호
                })

    print(f"구글 시트에서 {len(keyword_utm_pairs)}개의 키워드-UTM 쌍을 가져왔습니다.")
    return keyword_utm_pairs


def get_analytics_data_for_campaign(campaign_name, start_date, end_date):
    """
    GA4에서 sessionCampaignName = campaign_name AND eventName = click 인
    이벤트 수를 조회
    """
    creds = get_credentials()
    analytics = build('analyticsdata', 'v1beta', credentials=creds)

    request_body = {
        "dateRanges": [
            {"startDate": start_date, "endDate": end_date}
        ],
        "metrics": [
            {"name": "eventCount"}
        ],
        "dimensions": [
            {"name": "sessionCampaignName"},
            {"name": "eventName"}
        ],
        "dimensionFilter": {
            "andGroup": {
                "expressions": [
                    {
                        "filter": {
                            "fieldName": "sessionCampaignName",
                            "stringFilter": {
                                "matchType": "EXACT",
                                "value": campaign_name
                            }
                        }
                    },
                    {
                        "filter": {
                            "fieldName": "eventName",
                            "stringFilter": {
                                "matchType": "EXACT",
                                "value": "click"
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
        print(f"캠페인 '{campaign_name}'에 대한 데이터 조회 중 오류: {e}")
        return None


def find_today_column(sheets_service):
    """
    1행 헤더에서 오늘 날짜(YYYY-MM-DD / MM/DD / YYYY.MM.DD 형식 포함)를 찾아
    해당 열의 컬럼 문자(A, B, ..., AA 등)를 반환
    """
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


def update_single_cell(sheets_service, label, click_count, today_column, row_number):
    """
    단일 셀에 클릭 수 기록
    """
    try:
        cell_range = f"{today_column}{row_number}"
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SEARCH_TERMS_SHEET_ID,
            range=cell_range,
            valueInputOption='USER_ENTERED',
            body={'values': [[click_count]]}
        ).execute()
        print(f"✓ '{label}': {click_count} → {cell_range}")
        return True
    except Exception as e:
        print(f"✗ '{label}' 기록 실패: {e}")
        return False


def mai
