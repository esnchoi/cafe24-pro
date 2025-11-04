import requests
from bs4 import BeautifulSoup
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import time
from urllib.parse import unquote
import re

# Google Sheets API 설정
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = r'C:\Users\7040_64bit\Documents\코드 테스트\사내뉴스레터 커뮤니티\client_secret.json'
TOKEN_FILE = r'C:\Users\7040_64bit\Documents\코드 테스트\카페24프로\ga_token.json'
SPREADSHEET_ID = "1vxP7tVII0oWaGtro8puSXy7lDvYDrnppaRPv2qFACm0"

# Confluence 위키 정보
WIKI_URL = "https://wiki.simplexi.com"
WIKI_PAGE_ID = "2731295782"
WIKI_USERNAME = "esnchoi"
WIKI_PASSWORD = "meikai@11"

def get_credentials():
    """Google Sheets 인증"""
    print("Google 인증 시작")
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print("기존 토큰 파일에서 인증 완료")
        except Exception as e:
            print(f"토큰 파일 로드 중 오류: {e}")
            if os.path.exists(TOKEN_FILE):
                os.remove(TOKEN_FILE)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                print("토큰 갱신 완료")
            except Exception as e:
                print(f"토큰 갱신 실패: {e}")
                if os.path.exists(TOKEN_FILE):
                    os.remove(TOKEN_FILE)
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                print("새 인증 완료")
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            print("새 인증 완료")
        
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return creds

def parse_channel(channel_text):
    """영역(채널) 데이터 파싱: (카페) 텍스트 -> ('텍스트', '카페')"""
    # 괄호 패턴 찾기: (카페) 또는 (유튜브) 등
    pattern = r'^\(([^)]+)\)\s*(.+)$'
    match = re.match(pattern, channel_text)
    
    if match:
        # 괄호 안 내용 (C열에 입력, 괄호 제거)
        bracket_content = match.group(1).strip()
        # 뒷부분 (A열에 입력)
        rest_content = match.group(2).strip()
        return (rest_content, bracket_content)
    else:
        # 괄호가 없으면 전체를 A열에만 입력
        return (channel_text, None)

def get_wiki_pr_data():
    """Confluence 위키에서 PR팀 데이터 추출 (팀=PR인 것만)"""
    print("위키 페이지에서 PR팀 데이터 추출 시작")
    
    # Confluence REST API를 사용하여 페이지 내용 가져오기
    api_url = f"{WIKI_URL}/rest/api/content/{WIKI_PAGE_ID}?expand=body.storage"
    
    # Basic Auth로 인증
    response = requests.get(
        api_url,
        auth=(WIKI_USERNAME, WIKI_PASSWORD),
        headers={'Accept': 'application/json'}
    )
    
    if response.status_code != 200:
        print(f"위키 페이지 접근 실패: {response.status_code}")
        print(f"응답 내용: {response.text}")
        return []
    
    page_data = response.json()
    html_content = page_data['body']['storage']['value']
    
    # 디버깅: 전체 HTML 확인 (처음 2000자)
    print(f"\n=== 전체 HTML (처음 2000자) ===")
    print(html_content[:2000])
    print("=" * 50)
    
    # BeautifulSoup으로 파싱
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # PR팀 데이터: (keyword, channel_a, channel_c) 튜플 리스트
    pr_data = []
    
    # 표에서 데이터 추출
    tables = soup.find_all('table')
    print(f"발견된 표 개수: {len(tables)}")
    
    # 전체 HTML에서 utm_campaign=pr이 있는 URL 모두 찾기 (디버깅용)
    all_urls = re.findall(r'https?://[^\s<>"]+', html_content)
    pr_urls = [url for url in all_urls if 'utm_campaign=pr' in url.lower()]
    print(f"\n전체 HTML에서 찾은 PR URL 개수: {len(pr_urls)}")
    if pr_urls:
        print(f"첫 3개 PR URL:")
        for url in pr_urls[:3]:
            print(f"  - {url}")
    
    for table_idx, table in enumerate(tables):
        rows = table.find_all('tr')
        print(f"\n=== 표 {table_idx + 1} ===")
        print(f"총 행 개수: {len(rows)}")
        
        if not rows:
            continue
        
        # 헤더 행 찾기 (첫 2행을 확인)
        header_row = None
        header_cells = []
        
        # 첫 번째 행이 헤더인지 확인
        first_row = rows[0]
        first_cells = first_row.find_all(['th', 'td'])
        if first_cells:
            first_text = ' '.join([cell.get_text(strip=True).lower() for cell in first_cells])
            print(f"첫 번째 행 텍스트: {first_text}")
            # 헤더 특성 확인 (팀, 영역, 채널, url 등이 포함되어 있으면 헤더)
            if any(keyword in first_text for keyword in ['팀', '영역', '채널', 'url', '링크', '코드']):
                header_row = first_row
                header_cells = first_cells
                print("첫 번째 행을 헤더로 인식했습니다.")
        
        # 헤더에서 열 위치 찾기
        team_col = None
        channel_col = None
        url_col = None
        
        print(f"헤더 셀 개수: {len(header_cells)}")
        for idx, cell in enumerate(header_cells):
            cell_text = cell.get_text(strip=True)
            print(f"  헤더[{idx}]: '{cell_text}'")
            cell_text_lower = cell_text.lower()
            # 팀 열 찾기
            if '팀' in cell_text_lower and team_col is None:
                team_col = idx
                print(f"    -> 팀 열로 인식: 인덱스 {idx}")
            # 채널 열 찾기
            elif ('영역' in cell_text_lower or '채널' in cell_text_lower) and channel_col is None:
                # "영역 (채널) - 노출 페이지 url"도 채널로 인식하되, url 열은 별도로 찾음
                channel_col = idx
                print(f"    -> 채널 열로 인식: 인덱스 {idx}")
            # URL 열 찾기 (url 또는 링크가 포함된 경우)
            elif ('url' in cell_text_lower or '링크' in cell_text_lower) and url_col is None:
                url_col = idx
                print(f"    -> URL 열로 인식: 인덱스 {idx}")
        
        # 헤더가 없거나 열을 찾지 못한 경우 기본값 사용
        if team_col is None:
            team_col = 0
            print(f"팀 열을 찾지 못해 기본값 0 사용")
        if channel_col is None:
            channel_col = 1
            print(f"채널 열을 찾지 못해 기본값 1 사용")
        if url_col is None:
            url_col = 2
            print(f"URL 열을 찾지 못해 기본값 2 사용")
        
        print(f"\n최종 열 위치: 팀={team_col}, 채널={channel_col}, URL={url_col}")
        
        # 데이터 행 처리 (헤더 행 제외)
        start_row = 1 if header_row else 0
        
        # 헤더의 셀 개수 확인 (병합된 셀 감지용)
        header_cell_count = len(header_cells) if header_cells else 4
        print(f"\n데이터 행 처리 시작 (헤더 제외 행: {start_row}부터, 헤더 셀 개수: {header_cell_count})")
        
        # 첫 번째 행의 전체 HTML 구조 확인 (디버깅용)
        if len(rows) > start_row:
            first_data_row = rows[start_row]
            print(f"\n=== 첫 번째 데이터 행 전체 HTML (처음 1000자) ===")
            print(str(first_data_row)[:1000])
            print("=" * 50)
        
        for row_idx, row in enumerate(rows[start_row:], start=start_row):
            cells = row.find_all(['td', 'th'])
            if len(cells) == 0:
                print(f"  행 {row_idx}: 셀이 없음")
                continue
            
            print(f"  행 {row_idx}: 셀 개수={len(cells)} (헤더 셀 개수={header_cell_count})")
            
            # 첫 몇 개 행만 상세 출력
            if row_idx <= 3:
                for cell_idx, cell in enumerate(cells):
                    print(f"    셀[{cell_idx}] 텍스트: '{cell.get_text(strip=True)[:50]}'")
                    print(f"    셀[{cell_idx}] HTML (처음 300자): {str(cell)[:300]}")
            
            # 셀 개수가 헤더보다 적으면 첫 번째 열이 병합된 것으로 판단
            is_first_col_merged = len(cells) < header_cell_count
            
            # URL 열 찾기 및 utm_campaign=pr 확인
            if is_first_col_merged:
                # 병합된 경우: 첫 번째 셀이 채널 열 (인덱스 0), 두 번째 셀이 URL 열 (인덱스 1)
                actual_channel_col = 0
                actual_url_col = 1
                print(f"    -> 병합된 행: 채널=인덱스{actual_channel_col}, URL=인덱스{actual_url_col}")
            else:
                # 병합되지 않은 경우: 헤더에서 찾은 열 인덱스 사용
                # 첫 번째 셀이 팀 열이므로, 채널과 URL은 헤더에서 찾은 인덱스 그대로 사용
                actual_channel_col = channel_col
                actual_url_col = url_col
                print(f"    -> 병합되지 않은 행: 채널=인덱스{actual_channel_col}, URL=인덱스{actual_url_col}")
            
            # URL 열에서 utm_campaign=pr 확인
            url = None
            
            if actual_url_col < len(cells):
                url_cell = cells[actual_url_col]
            else:
                # URL 열이 범위를 벗어났으면 행 전체에서 URL 찾기
                url_cell = None
            
            # 방법 1: 셀이 있으면 셀 전체 HTML에서 URL 패턴 찾기 (가장 확실한 방법)
            if url_cell:
                cell_html_str = str(url_cell)
                # URL 패턴 찾기 (더 포괄적인 패턴)
                url_pattern = r'https?://[^\s<>\"]+'
                matches = re.findall(url_pattern, cell_html_str)
                if matches:
                    url = matches[0]
                    print(f"    -> 셀 HTML에서 URL 발견: {url}")
            
            # 방법 2: 셀 텍스트에서 URL 패턴 찾기
            if not url and url_cell:
                cell_text = url_cell.get_text(strip=True)
                url_pattern = r'https?://[^\s<>"]+'
                matches = re.findall(url_pattern, cell_text)
                if matches:
                    url = matches[0]
                    print(f"    -> 셀 텍스트에서 URL 발견: {url}")
            
            # 방법 3: 행 전체 HTML에서 URL 패턴 찾기 (셀에서 못 찾으면)
            if not url:
                row_html_str = str(row)
                url_pattern = r'https?://[^\s<>\"]+'
                matches = re.findall(url_pattern, row_html_str)
                if matches:
                    # URL 열에 해당하는 URL 찾기 (인덱스 기반)
                    if len(matches) > actual_url_col:
                        url = matches[actual_url_col]
                        print(f"    -> 행 HTML에서 URL 발견: {url}")
                    elif matches:
                        url = matches[0]
                        print(f"    -> 행 HTML에서 첫 번째 URL 발견: {url}")
            
            # 방법 4: <a> 태그에서 href 찾기
            if not url and url_cell:
                link = url_cell.find('a')
                if link:
                    href = link.get('href')
                    if href:
                        url = href
                        print(f"    -> <a> 태그에서 URL 발견: {url}")
            
            # 방법 5: 모든 링크 태그 확인
            if not url and url_cell:
                all_links = url_cell.find_all(['a', 'ac:link'])
                for link_elem in all_links:
                    href = link_elem.get('href')
                    if href:
                        url = href
                        print(f"    -> 링크 태그에서 URL 발견: {url}")
                        break
                    # ri:url 확인
                    ri_url = link_elem.find('ri:url')
                    if ri_url:
                        url = ri_url.get_text(strip=True)
                        print(f"    -> ri:url에서 URL 발견: {url}")
                        break
            
            # URL을 찾았으면 처리
            if url:
                print(f"    -> URL 발견: {url}")
                
                # utm_campaign=pr 확인 (대소문자 구분 없이)
                if 'utm_campaign=pr' in url.lower():
                    print(f"    -> PR 팀 확인됨 (utm_campaign=pr)")
                    
                    # 채널 열 가져오기
                    if actual_channel_col < len(cells):
                        channel_text = cells[actual_channel_col].get_text(strip=True)
                        print(f"    -> 채널 텍스트: '{channel_text}'")
                    else:
                        channel_text = ""
                        print(f"    -> 채널 열 인덱스 범위 초과")
                    
                    # URL에서 utm_source 파라미터 추출 (키워드)
                    keyword = None
                    if 'utm_source=' in url:
                        try:
                            keyword = url.split('utm_source=')[1].split('&')[0]
                            keyword = unquote(keyword)  # URL 디코딩
                            print(f"    -> 키워드 추출: '{keyword}'")
                        except Exception as e:
                            print(f"    -> 키워드 추출 실패: {e}")
                    else:
                        print(f"    -> URL에 utm_source 파라미터 없음")
                    
                    if keyword and channel_text:
                        # 영역(채널) 파싱
                        channel_a, channel_c = parse_channel(channel_text)
                        pr_data.append((keyword, channel_a, channel_c))
                        print(f"    ✓ 추출 성공: 키워드={keyword}, 영역={channel_text} -> A열={channel_a}, C열={channel_c}")
                    else:
                        if not keyword:
                            print(f"    ✗ 키워드 없음")
                        if not channel_text:
                            print(f"    ✗ 채널 텍스트 없음")
                else:
                    print(f"    -> PR 팀이 아님 (utm_campaign=pr 없음)")
            else:
                # URL을 찾지 못한 경우
                if url_cell:
                    cell_text = url_cell.get_text(strip=True)
                    cell_html = str(url_cell)
                    print(f"    -> URL 셀에 링크 없음")
                    if row_idx <= 5:  # 처음 몇 개 행만 상세 출력
                        print(f"    -> 셀 텍스트: '{cell_text[:200]}'")
                        print(f"    -> 셀 HTML (처음 500자): {cell_html[:500]}")
                        # 모든 태그 확인
                        all_tags = url_cell.find_all(True)
                        print(f"    -> 셀 내부 태그 개수: {len(all_tags)}")
                        if all_tags:
                            print(f"    -> 태그 목록 (처음 5개): {[tag.name for tag in all_tags[:5]]}")
                else:
                    print(f"    -> URL 열 인덱스 범위 초과")
    
    if not pr_data:
        print("테이블에서 PR팀 데이터를 찾지 못했습니다.")
        print(f"페이지 제목: {page_data.get('title', 'N/A')}")
    
    print(f"\n추출된 PR팀 데이터 개수: {len(pr_data)}")
    return pr_data

def get_first_empty_cell_in_column(sheets_service, column='B'):
    """B열에서 첫 번째로 비어있는 셀의 행 번호 찾기"""
    print(f"{column}열에서 첫 번째 빈 셀 찾기")
    
    # B열 전체 데이터 가져오기
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{column}:{column}"
    ).execute()
    
    values = result.get('values', [])
    
    # 첫 번째 행은 헤더일 수 있으므로 2번째 행부터 확인
    for i in range(1, len(values)):
        if not values[i] or not values[i][0].strip():
            # 빈 셀 발견
            return i + 1  # 행 번호는 1부터 시작
    
    # 모든 행이 차있으면 마지막 행 다음
    return len(values) + 1

def check_keyword_exists(sheets_service, keyword):
    """구글 시트 B열에 키워드가 이미 존재하는지 확인"""
    print(f"키워드 '{keyword}' 존재 여부 확인")
    
    # B열 전체 데이터 가져오기
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="B:B"
    ).execute()
    
    values = result.get('values', [])
    
    for row in values:
        if row and row[0].strip() == keyword:
            print(f"키워드 '{keyword}'가 이미 존재합니다.")
            return True
    
    print(f"키워드 '{keyword}'가 존재하지 않습니다.")
    return False

def add_data_to_sheet(sheets_service, keyword, channel_a, channel_c, row_number):
    """구글 시트의 지정된 행에 데이터 추가 (A열: channel_a, B열: keyword, C열: channel_c)"""
    print(f"행 {row_number}에 데이터 추가")
    print(f"  A열(영역): {channel_a}")
    print(f"  B열(키워드): {keyword}")
    print(f"  C열(채널): {channel_c}")
    
    try:
        # A, B, C 열을 한 번에 업데이트
        values = []
        if channel_a:
            values.append(channel_a)
        else:
            values.append('')
        
        values.append(keyword)
        
        if channel_c:
            values.append(channel_c)
        else:
            values.append('')
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"A{row_number}:C{row_number}",
            valueInputOption='USER_ENTERED',
            body={'values': [values]}
        ).execute()
        print(f"✓ 데이터 추가 완료 (행 {row_number})")
        return True
    except Exception as e:
        print(f"✗ 데이터 추가 실패: {e}")
        return False

def main():
    print("프로그램 시작")
    
    # 1. Google Sheets 인증
    creds = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=creds)
    print("Google Sheets API 서비스 빌드 완료")
    
    # 2. 위키에서 PR팀 데이터 추출
    pr_data = get_wiki_pr_data()
    
    if not pr_data:
        print("위키에서 PR팀 데이터를 찾을 수 없습니다.")
        return
    
    print(f"\n추출된 데이터 개수: {len(pr_data)}")
    
    # 3. 각 데이터에 대해 처리
    for keyword, channel_a, channel_c in pr_data:
        # 키워드가 이미 존재하는지 확인
        if not check_keyword_exists(sheets_service, keyword):
            # 존재하지 않으면 첫 번째 빈 셀 찾기
            empty_row = get_first_empty_cell_in_column(sheets_service, 'B')
            # 데이터 추가 (A열: channel_a, B열: keyword, C열: channel_c)
            add_data_to_sheet(sheets_service, keyword, channel_a, channel_c, empty_row)
            time.sleep(0.5)  # API 호출 제한 방지
        else:
            print(f"키워드 '{keyword}'는 이미 존재하므로 건너뜁니다.")
    
    print("\n프로그램 종료")

if __name__ == '__main__':
    main()

