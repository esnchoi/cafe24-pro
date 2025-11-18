import os
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ===== 설정 영역 (환경변수 사용 권장) ======================================
# 예시 환경변수:
# GA_SCOPES                             -> 공백으로 구분된 스코프 문자열 (선택)
# GA_CLIENT_SECRET_FILE                 -> OAuth client_secret.json 경로
# GA_TOKEN_FILE                         -> 토큰 저장 경로 (예: ./ga_token.json)
# GA_PROPERTY_ID                        -> GA4 Property ID (숫자만, 예: 464149233)
# GA_SEARCH_TERMS_SHEET_ID              -> 검색어/UTM 매핑이 있는 구글 시트 ID

DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

SCOPES = os.environ.get("GA_SCOPES")
if SCOPES:
    SCOPES = SCOPES.split()
else:
    SCOPES = DEFAULT_SCOPES

CLIENT_SECRET_FILE = os.environ.get("GA_CLIENT_SECRET_FILE", "client_secret.json")
TOKEN_FILE = os.environ.get("GA_TOKEN_FILE", "ga_token.json")
PROPERTY_ID = os.environ.get("GA_PROPERTY_ID")  # 필수
SEARCH_TERMS_SHEET_ID = os.environ.get("GA_SEARCH_TERMS_SHEET_ID")  # 필수
# ========================================================================


def get_credentials():
    """Google OAuth2 인증 및 토큰 관리"""
    creds = None

    # 토큰 파일이 존재하면 로드
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print("기존 토큰 파일에서 인증 정보를 로드했습니다.")
        except Exception as e:
            print(f"토큰 파일 로드 중 오류: {e}")
            # 손상된 토큰 파일 삭제
            try:
                os.remove(TOKEN_FILE)
                print("손상된 토큰 파일을 삭제했습니다.")
            except OSError as rm_err:
                print(f"토큰 파일 삭제 중 오류: {rm_err}")

    # 토큰이 없거나 유효하지 않은 경우
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("토큰이 만료되었습니다. 갱신을 시도합니다...")
                creds.refresh(Request())
                print("토큰 갱신이 완료되었습니다.")
            except Exception as e:
                print(f"토큰 갱신 실패: {e}")
                # 리프레시 실패 시 토큰 파일 삭제하고 새로 생성
                if os.path.exists(TOKEN_FILE):
                    try:
                        os.remove(TOKEN_FILE)
                        print("만료된 토큰 파일을 삭제했습니다.")
                    except OSError as rm_err:
                        print(f"토큰 파일 삭제 중 오류: {rm_err}")
                flow = InstalledAppFlow.from_client_secrets_file(
                    CLIENT_SECRET_FILE, SCOPES
                )
                creds = flow.run_local_server(port=0)
                print("새로운 인증이 완료되었습니다.")
        else:
            print("새로운 인증을 시작합니다...")
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRET_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
            print("인증이 완료되었습니다.")

        # 토큰을 파일에 저장
        try:
            with open(TOKEN_FILE, "w", encoding="utf-8") as token:
                token.write(creds.to_json())
            print(f"토큰이 {TOKEN_FILE}에 저장되었습니다.")
        except Exception as e:
            print(f"토큰 저장 중 오류: {e}")

    return creds


def validate_env():
    """필수 환경변수 체크"""
    missing = []
    if not PROPERTY_ID:
        missing.append("GA_PROPERTY_ID")
    if not SEARCH_TERMS_SHEET_ID:
        missing.append("GA_SEARCH_TERMS_SHEET_ID")

    if missing:
        raise RuntimeError(
            f"필수 환경변수 누락: {', '.join(missing)}\n"
            "환경변수를 설정한 후 다시 실행하세요."
        )


def get_search_terms_from_sheet():
    """구글 시트에서 B열(키워드)과 E열(UTM) 매핑을 가져옵니다"""
    creds = get_credentials()
    sheets_service = build("sheets", "v4", credentials=creds)

    # B열부터 E열까지 가져오기
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=SEARCH_TERMS_SHEET_ID, range="B:E")
        .execute()
    )

    values = result.get("values", [])
    keyword_utm_pairs = []

    for i, row in enumerate(values):
        if i == 0:  # 첫 번째 행(헤더)은 건너뛰기
            continue

        # E열(인덱스 3)에 값이 있는 경우만
        if len(row) > 3 and row[3].strip():
            keyword = row[0].strip() if row[0] else None  # B열
            utm_campaign = row[3].strip()  # E열
            if keyword and utm_campaign:
                keyword_utm_pairs.append(
                    {
                        "keyword": keyword,
                        "utm_campaign": utm_campaign,
                        "row_number": i + 1,
                    }
                )

    print(f"구글 시트에서 {len(keyword_utm_pairs)}개의 키워드-UTM 쌍을 가져왔습니다.")
    return keyword_utm_pairs


def get_analytics_data_for_search_term(keyword, start_date, end_date):
    """특정 키워드(sessionCampaignName)에 대한 GA4 데이터를 가져옵니다"""
    creds = get_credentials()
    analytics = build("analyticsdata", "v1beta", credentials=creds)

    match_type = "EXACT"

    request_body = {
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": [{"name": "eventCount"}],
        "dimensions": [
            {"name": "sessionCampaignName"},
            {"name": "eventName"},
        ],
        "dimensionFilter": {
            "andGroup": {
                "expressions": [
                    {
                        "filter": {
                            "fieldName": "sessionCampaignName",
                            "stringFilter": {
                                "matchType": match_type,
                                "value": keyword,
                            },
                        }
                    },
                    {
                        "filter": {
                            "fieldName": "eventName",
                            "stringFilter": {
                                "matchType": "EXACT",
                                "value": "click",
                            },
                        }
                    },
                ]
            }
        },
        "limit": 1000,
    }

    try:
        response = (
            analytics.properties()
            .runReport(property=f"properties/{PROPERTY_ID}", body=request_body)
            .execute()
        )
        return response
    except Exception as e:
        print(f"키워드 '{keyword}'에 대한 데이터 조회 중 오류: {e}")
        return None


def find_specific_date_column(sheets_service, target_date):
    """특정 날짜에 해당하는 열을 찾습니다 (헤더 1행 기준)"""
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=SEARCH_TERMS_SHEET_ID, range="1:1")
        .execute()
    )

    values = result.get("values", [[]])
    if not values or not values[0]:
        print("헤더 행을 찾을 수 없습니다.")
        return None

    # 다양한 날짜 형식 확인
    date_str = target_date  # YYYY-MM-DD
    date_short = target_date[5:7] + "/" + target_date[8:10]  # MM/DD
    date_dot = target_date.replace("-", ".")  # YYYY.MM.DD

    date_formats = [date_str, date_short, date_dot]

    for i, cell in enumerate(values[0]):
        if cell:
            cell_str = str(cell).strip()
            for date_format in date_formats:
                if date_format in cell_str:
                    # 열 인덱스를 열 문자로 변환 (A=1, B=2, ..., Z=26, AA=27, ...)
                    if i < 26:
                        col_letter = chr(65 + i)
                    else:
                        # 간단한 2자리 컬럼 변환 (AA, AB, ...)
                        col_letter = chr(64 + i // 26) + chr(65 + i % 26)
                    print(f"날짜 열 발견: {col_letter}1 ({cell_str})")
                    return col_letter

    print(f"날짜({target_date})에 해당하는 열을 찾을 수 없습니다.")
    return None


def update_single_cell(sheets_service, search_term, click_count, today_column, row_number):
    """단일 셀을 업데이트합니다"""
    try:
        cell_range = f"{today_column}{row_number}"
        (
            sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=SEARCH_TERMS_SHEET_ID,
                range=cell_range,
                valueInputOption="USER_ENTERED",
                body={"values": [[click_count]]},
            )
            .execute()
        )
        print(f"OK '{search_term}': {click_count} -> {cell_range}")
        return True
    except Exception as e:
        print(f"FAIL '{search_term}' 기록 실패: {e}")
        return False


def main():
    # 환경변수 확인
    validate_env()

    # 조회 기간: 2025-02-01 ~ 오늘
    start_date = "2025-02-01"
    today = datetime.now()
    end_date = today.strftime("%Y-%m-%d")

    print(f"Google Analytics 데이터 수집: {start_date} ~ {end_date}")

    # 구글 시트 서비스 초기화
    creds = get_credentials()
    sheets_service = build("sheets", "v4", credentials=creds)

    # 오늘 날짜 열 찾기
    target_date = today.strftime("%Y-%m-%d")
    today_column = find_specific_date_column(sheets_service, target_date)
    if not today_column:
        print(f"{target_date} 날짜 열을 찾을 수 없어 업데이트를 중단합니다.")
        return

    # 구글 시트에서 키워드-UTM 쌍 가져오기
    keyword_utm_pairs = get_search_terms_from_sheet()
    if not keyword_utm_pairs:
        print("키워드-UTM 쌍을 가져올 수 없습니다.")
        return

    print("\n=== 키워드별 클릭 이벤트 수 (E열에 값이 있는 것만) ===")
    print("키워드 (B열)\t\t클릭 이벤트 수")
    print("-" * 60)

    # 각 키워드별로 데이터 조회하고 바로 기록
    for pair in keyword_utm_pairs:
        keyword = pair["keyword"]
        row_number = pair["row_number"]

        print(f"'{keyword}' 조회 중...")
        response = get_analytics_data_for_search_term(keyword, start_date, end_date)

        total_clicks = 0
        if response and "rows" in response:
            for row in response["rows"]:
                clicks = int(row["metricValues"][0]["value"])
                total_clicks += clicks

        print(f"{keyword}\t\t{total_clicks}")

        # 구글 시트에 기록
        update_single_cell(sheets_service, keyword, total_clicks, today_column, row_number)

    print("\n모든 데이터 기록 완료!")


if __name__ == "__main__":
    main()
