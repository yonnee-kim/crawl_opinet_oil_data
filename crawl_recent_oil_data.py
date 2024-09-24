import asyncio
import json
import aiohttp
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException 
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import time
import os
import pandas as pd
import sys
import urllib.parse

async def get_sigun_code():
    # getAreaCode
    project_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(project_dir, f'json/')
    file_name = 'sido_sigun_code.json' 
    file_path = os.path.join(download_dir,file_name)
    path = 'json/sido_sigun_code.json';
    repo = 'yonnee-kim/fetch_opinet_api'
    token = os.getenv('REPOSITORY_TOKEN')  # 환경 변수에서 토큰 가져오기
    url = f'https://api.github.com/repos/{repo}/contents/{path}'
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github.v3.raw',
    }
    data = None
    is_connect = False
    try_count = 5
    delay_seconds = 1

    while not is_connect:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers = headers) as response:
                try:
                    if response.status == 200:
                        data = await response.json(content_type='application/vnd.github.v3.raw')  # JSON 응답을 직접 파싱
                        print('get_sigun_code url 연결 성공')
                        is_connect = True
                    elif try_count > 0:
                        try_count -= 1
                        print(f'get_sigun_code url 연결 오류 {delay_seconds}초 후 재시도. 남은 재시도 횟수: {try_count} \nresponse statusCode: {response.status}')
                        time.sleep(delay_seconds)
                    else:
                        print(f"get_sigun_code 종료. url 연결 오류 response statusCode: {response.status}")
                        return
                except Exception as e:
                    try_count -= 1
                    print(f'get_sigun_code url 연결 오류 {delay_seconds}초 후 재시도. 남은 재시도 횟수: {try_count} \nerror: {e}')
                    time.sleep(delay_seconds)
                    if try_count <= 0:
                        print('get_sigun_code 실패. 함수 종료.')
                        return
    # 파일 읽기
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            existing_file = file.read()
    else:
        existing_file = ''
    # 파일 업데이트
    if json.dumps(data, ensure_ascii=False) != existing_file:
        print('시도 시군 코드 변경사항 업데이트 완료.')
        with open(file_path, 'w') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    else:
        print('시도 시군 코드 변경사항 없음.')

def crawl_for_sido(sido_name, project_dir, sidosigun_code, code_start_time):
    download_dir = os.path.join(project_dir, f'excel/{sido_name}')
    os.makedirs(download_dir, exist_ok=True)  # 디렉토리가 없으면 생성
    old_file_name = '지역_위치별(주유소).xls' 
    sido_oil_data_list = []
    sigun_list =[]

    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "intl.accept_languages": "ko-KR",
    })
    # 추가적인 Chrome 옵션을 설정
    chrome_options.add_argument("--headless")  # Headless 모드 추가
    chrome_options.add_argument("--start-maximized")  # 최대화 시작
    chrome_options.add_argument("--disable-gpu")  # GPU 비활성화
    chrome_options.add_argument("--window-size=1920x1080")  # 창 크기 설정
    chrome_options.add_argument("--no-sandbox")  # 보안 관련 옵션
    chrome_options.add_argument("--disable-dev-shm-usage")  # 리소스 제한 문제 해결
    chrome_options.add_argument("--disable-popup-blocking")  #  브라우저 팝업 비활성화
    chrome_options.add_argument("--disable-notifications")  #  브라우저 알림 비활성화

    # 시군리스트 초기화
    for sido in sidosigun_code['SIDO']:
        if sido['AREA_NM'] == sido_name :
            for sigun in sido['SIGUN']:
                sigun_list.append(sigun['AREA_NM'])

    while True:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://www.opinet.co.kr/searRgSelect.do")
        try:
            start_time = time.time()
            # 특정 요소가 나타날 때까지 최대 10초 대기
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="SIDO_NM0"]'))
            )
            end_time = time.time()
            elapsed_time = end_time - start_time
            # print(f"{sido_name} 초기 웹페이지 로드 완료! 걸린 시간 : {elapsed_time:.1f}초")
            time.sleep(3)
            break
        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"{sido_name} 초기 웹페이지 로드 실패 {elapsed_time:.1f}초:", e)
            driver.quit()  # 새로고침
            time.sleep(2)
    
    for sigun_name in sigun_list:
        retry = True
        while retry:
            cut_time = time.time()
            if cut_time - code_start_time > 1200 :
                print(f"{sido_name} {sigun_name} 실행시간 초과, 코드 종료...\n")
                sys.exit(1)
            try:
                start_time = time.time()
                # 특정 요소가 나타날 때까지 대기
                WebDriverWait(driver, 40).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="SIDO_NM0"]'))
                )
                end_time = time.time()
                elapsed_time = end_time - start_time
                # print(f"{sido_name} 웹페이지 로드 성공 {elapsed_time:.1f}초:")
                time.sleep(2)
            except Exception as e:
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"{sido_name} 웹페이지 로드 실패 {elapsed_time:.1f}초:", e)
                driver.quit()  # 재시작
                time.sleep(1)
                driver = webdriver.Chrome(options=chrome_options)
                driver.get("https://www.opinet.co.kr/searRgSelect.do")
                time.sleep(2)
                continue
            # 시도란 입력
            sido = WebDriverWait(driver,60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="SIDO_NM0"]'))
            )
            Select(sido).select_by_visible_text(sido_name)
            start_time = time.time()
            trycount = 0
            while True : 
                try:
                    trycount += 1
                    sigun_names = driver.find_elements(By.XPATH, '//*[@id="SIGUNGU_NM0"]/option')
                    test = sigun_names[1].get_attribute('value')
                    if test in sigun_list :
                        break
                    elif trycount > 50:
                        break
                    else:
                        time.sleep(0.1)
                except Exception as e:
                    time.sleep(0.1)
            if trycount > 50:
                print(f"{sido_name} 시도란 입력실패. 걸린 시간 : {elapsed_time:.1f}초")
                driver.quit()
                time.sleep(1)
                driver = webdriver.Chrome(options=chrome_options)
                driver.get("https://www.opinet.co.kr/searRgSelect.do")
                time.sleep(2)
                continue
            time.sleep(1)
            # 시군란 입력       
            sigun = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="SIGUNGU_NM0"]'))
            )
            Select(sigun).select_by_visible_text(sigun_name) # 시군 네임 입력
            start_time = time.time()
            trycount=0
            is_sigun_zero = False
            while True:
                try:
                    trycount += 1
                    totcnt = driver.find_element(By.ID, "totCnt")
                    totcnt_text = totcnt.text
                    if totcnt_text == '0':
                        is_sigun_zero = True
                        break
                    else:
                        addr = driver.find_element(By.XPATH, '//*[@id="body1"]/tr[1]/td[1]/a')
                        addr_href = addr.get_attribute('href')
                        # URL 디코딩
                        decoded_part = urllib.parse.unquote(addr_href)
                        gun_name = sigun_name.replace('시', '')
                        if gun_name[:2] in decoded_part:
                            retry = False
                            break
                    if trycount > 200 :
                        break
                    time.sleep(0.1)
                except Exception as e:
                    #print(f'{sido_name} {sigun_name} totCnt : {e}')
                    time.sleep(0.1)
            if is_sigun_zero:
                print(f'시군란 목록 0. 다음으로 넘어감.')
                break
            end_time = time.time()
            elapsed_time = end_time - start_time
            if trycount>200:
                print(f"{sido_name} {sigun_name} 시군란 입력실패. 걸린 시간 : {elapsed_time:.1f}초")
                driver.quit()
                time.sleep(1)
                driver = webdriver.Chrome(options=chrome_options)
                driver.get("https://www.opinet.co.kr/searRgSelect.do")
                time.sleep(2)
                continue
            time.sleep(2)
            # 엑셀 파일 제거
            if os.listdir(download_dir):
                excel_file_name = os.listdir(download_dir)[0]
                excel_file_path = os.path.join(download_dir, excel_file_name)
                os.remove(excel_file_path)
            # 엑셀 다운로드
            excel_download_button = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="templ_list0"]/div[7]/div/a'))
            )
            driver.execute_script("arguments[0].click();", excel_download_button)
            # 엑셀 다운로드 확인
            trycount = 0
            while True :
                trycount += 1
                file_list = os.listdir(download_dir)  # 리스트를 변수에 저장
                if file_list:  # 파일이 있는 경우
                    excel_file_name = file_list[0]
                    if '.' in excel_file_name:  # 확장자가 있는지 확인
                        extension = excel_file_name.split('.')[-1]  # 확장자 추출
                        if extension in ['xls', 'xlsx']:  # 확장자가 xls 또는 xlsx인지 확인
                            retry = False
                            break
                elif trycount > 15 :
                    print(f"{sido_name} {sigun_name} excel 파일 다운로드 실패.. 다시시작")
                    retry = True
                    break
                time.sleep(1)
            if retry :
                driver.quit()
                time.sleep(1)
                driver = webdriver.Chrome(options=chrome_options)
                driver.get("https://www.opinet.co.kr/searRgSelect.do")
                time.sleep(2)
                continue
            print(f'{sido_name} {sigun_name} excel 파일 저장 완료. 시도횟수 : {trycount}')
                
            # 엑셀 파일을 List로 변환
            excel_file_path = os.path.join(download_dir, excel_file_name)
            while True:
                try:
                    if extension == 'xls' : 
                        data_frame = pd.read_excel(excel_file_path, skiprows=[0, 1], engine='xlrd')
                        break
                    else :
                        print(extension)   
                        data_frame = pd.read_excel(excel_file_path, skiprows=[0, 1], engine='openpyxl')
                        break
                except Exception as e:
                    print(f"엑셀 파일 읽기 중 오류 발생: {e}")
                    retry = True
                    break
            if retry:
                driver.quit()
                time.sleep(1)
                driver = webdriver.Chrome(options=chrome_options)
                driver.get("https://www.opinet.co.kr/searRgSelect.do")
                time.sleep(2)
                continue
            data_frame_list = data_frame.to_dict(orient='records')
            sido_oil_data_list.extend(data_frame_list)
            # 엑셀 파일 제거
            os.remove(excel_file_path)
            # 파일이 없거나/삭제될때 까지 대기
            while os.path.exists(excel_file_path):
                time.sleep(0.1)
            print(f'{sido_name} {sigun_name} excel 파일 제거완료')
            driver.refresh()
            time.sleep(1)

        if is_sigun_zero:
            continue

    driver.quit()
    print(f"{sido_name} 크롤링 완료")
    return sido_oil_data_list  # 각 시/군/구에 대한 데이터 반환

def get_opinet_oildata_crawler():
    code_start_time = time.time()
    project_dir = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(project_dir, f'json/')
    siguncode_file_name = 'sido_sigun_code.json' 
    siguncode_file_path = os.path.join(json_dir,siguncode_file_name)
    sidosigun_code = ''
    if os.path.exists(siguncode_file_path):
        with open(siguncode_file_path, 'r') as siguncode_file:
            sidosigun_code = json.load(siguncode_file)
    sido_list = [sido['AREA_NM'] for sido in sidosigun_code['SIDO']]
    print(f'sido list = {sido_list}')
    recent_oil_data_list = []
    with ThreadPoolExecutor(max_workers=4) as executor:  # 스레드 풀 생성
        future_to_sido = {executor.submit(crawl_for_sido, sido_name, project_dir, sidosigun_code, code_start_time): sido_name for sido_name in sido_list}
        for future in as_completed(future_to_sido):
            sido_name = future_to_sido[future]
            try:
                data_frame_list = future.result()
                recent_oil_data_list.extend(data_frame_list)  # 데이터 추가
            except Exception as e:
                print(f"{sido_name} 처리 중 오류 발생:", e)

    # 중복 요소를 저장할 빈 집합과 중복 요소 리스트 생성
    seen = set()
    duplicates = set()
    # 리스트를 순회하며 중복 요소 찾기
    for item in recent_oil_data_list:
        item_tuple = tuple(sorted(item.items()))  # 딕셔너리를 튜플로 변환
        if item_tuple in seen:
            duplicates.add(item_tuple)  # 중복된 요소 추가
        else:
            seen.add(item_tuple)  # 처음 본 요소 추가
    print("중복된 요소 개수:",len(list(duplicates)))  # 출력: 중복된 요소 개수
    print("중복된 요소:", list(duplicates))  # 출력: 중복된 요소: [1, 2, 3]
    json_file_name = 'recent_oil_data.json'  # JSON 파일 이름 설정
    json_dir = os.path.join(project_dir, 'json/') # JSON 경로 설정
    data_file_path = os.path.join(json_dir, json_file_name)
    recent_oil_data_df = pd.DataFrame(recent_oil_data_list)
    recent_oil_data_df.to_json(data_file_path, orient='records', force_ascii=False)  # JSON으로 변환하여 저장
        # JSON파일 저장될 때까지 대기
    while not os.path.exists(data_file_path):
        time.sleep(0.1) 
    print(f'JSON 데이터 저장 완료: {data_file_path}')
    # JSON 변환 후 엑셀 파일 삭제
    print('완료')
    # 실행 시간 측정
    end_time = time.time()
    elapsed_time = end_time - code_start_time
    print(f'get_opinet_oildata_crawler 함수 총 실행 시간: {elapsed_time:.2f}초')
    # 현재 UTC 시각 얻기
    utc_now = datetime.now(timezone.utc)
    # KST로 변환 (KST는 UTC+9)
    kst_now = utc_now + timedelta(hours=9)
    print(f'{kst_now} 오피넷 유가정보 크롤링 완료.')

# 함수 호출
async def main():
    await get_sigun_code()
    get_opinet_oildata_crawler()

if __name__ == "__main__":
    asyncio.run(main())