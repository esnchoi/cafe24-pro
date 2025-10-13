import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timedelta

# Google Analytics Data API v1beta 사용
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly', 'https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = r'C:\Users\7040_64bit\Documents\코드 테스트\사내뉴스레터 커뮤니티\client_secret.json'
TOKEN_FILE = r'C:\Users\7040_64bit\Documents\코드 테스트\카페24프로\ga_token.json'
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
            # 손상된 토큰 파일 삭제
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
                # 리프레시 실패 시 토큰 파일 삭제하고 새로 생성
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                    print("만료된 토큰 파일을 삭제했습니다.")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                print("새로운 인증이 완료되었습니다.")
        else:
            print("새로운 인증을 시작합니다...")
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            print("인증이 완료되었습니다.")
        
        # 토큰을 파일에 저장
        try:
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            print(f"토큰이 {TOKEN_FILE}에 저장되었습니다.")
        except Exception as e:
            print(f"토큰 저장 중 오류: {e}")
    
    return creds

def get_search_terms_from_sheet():
    """구글 시트에서 검색어 목록을 가져옵니다"""
    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)
    
    # B열의 검색어들을 가져오기
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="B:B"  # B열 전체
    ).execute()
    
    values = result.get('values', [])
    search_terms = []
    
    for i, row in enumerate(values):
        if i == 0:  # 첫 번째 행(헤더)은 건너뛰기
            continue
        if row and row[0].strip():  # 빈 셀이 아닌 경우만
            search_terms.append(row[0].strip())
    
    print(f"구글 시트에서 {len(search_terms)}개의 검색어를 가져왔습니다.")
    return search_terms

def get_analytics_data_for_search_term(search_term, start_date, end_date):
    """특정 검색어에 대한 GA4 데이터를 가져옵니다"""
    creds = get_credentials()
    analytics = build('analyticsdata', 'v1beta', credentials=creds)

    # 모든 검색어를 정확한 매칭으로 설정
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
            {"name": "sessionSourceMedium"},
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
                            "fieldName": "sessionSourceMedium",
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
    """오늘 날짜에 해당하는 열을 찾습니다"""
    # 첫 번째 행(헤더)에서 날짜 찾기
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="1:1"  # 첫 번째 행 전체
    ).execute()
    
    values = result.get('values', [[]])
    if not values or not values[0]:
        print("헤더 행을 찾을 수 없습니다.")
        return None
    
    today = datetime.now().strftime("%Y-%m-%d")
    today_short = datetime.now().strftime("%m/%d")  # MM/DD 형식도 확인
    today_dot = datetime.now().strftime("%Y.%m.%d")  # YYYY.MM.DD 형식도 확인
    
    # 날짜 형식들을 확인
    date_formats = [today, today_short, today_dot]
    
    for i, cell in enumerate(values[0]):
        if cell:
            cell_str = str(cell).strip()
            for date_format in date_formats:
                if date_format in cell_str:
                    # 열 인덱스를 열 문자로 변환 (A=1, B=2, ..., Z=26, AA=27, ...)
                    col_letter = chr(65 + i) if i < 26 else chr(64 + i // 26) + chr(65 + i % 26)
                    print(f"오늘 날짜 열 발견: {col_letter}1 ({cell_str})")
                    return col_letter
    
    print(f"오늘 날짜({today})에 해당하는 열을 찾을 수 없습니다.")
    return None

def update_single_cell(sheets_service, search_term, click_count, today_column, row_number):
    """단일 셀을 업데이트합니다"""
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
    # 날짜 설정 (2월 1일부터 오늘까지)
    start_date = "2025-02-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Google Analytics 데이터 수집: {start_date} ~ {end_date}")
    
    # 구글 시트 서비스 초기화
    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)
    
    # 오늘 날짜 열 찾기
    today_column = find_today_column(sheets_service)
    if not today_column:
        print("오늘 날짜 열을 찾을 수 없어 업데이트를 중단합니다.")
        return
    
    # 검색 대상: "viral / paid_youtube" 한 개만 사용
    search_terms = ["viral / paid_youtube"]
    
    if not search_terms:
        print("검색어를 가져올 수 없습니다.")
        return
    
    # B열 데이터 다시 가져오기 (행 번호 계산용)
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SEARCH_TERMS_SHEET_ID,
        range="B:B"  # B열 전체
    ).execute()
    values = result.get('values', [])
    
    # 특정 검색어별 추가 숫자 설정
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
    
    # 각 검색어별로 데이터 조회하고 바로 기록
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
        
        # 특정 검색어에 대해 추가 숫자 더하기
        if search_term in additional_values:
            total_clicks += additional_values[search_term]
        
        print(f"{search_term}\t\t{total_clicks}")
        
        # B열에서 해당 검색어의 실제 행 번호 찾기
        actual_row_number = None
        for row_idx, row in enumerate(values):
            if row and row[0].strip() == search_term:
                actual_row_number = row_idx + 1
                break
        
        if actual_row_number:
            # 바로 구글 시트에 기록
            update_single_cell(sheets_service, search_term, total_clicks, today_column, actual_row_number)
    
    print("\n모든 데이터 기록 완료!")

if __name__ == "__main__":
    main()
