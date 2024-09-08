import aiohttp
import asyncio
import json
import os
import time
from bs4 import BeautifulSoup

async def get_sigun_code():
    # getAreaCode
    opinet_api_key = 'F240811247'
    project_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(project_dir, f'json/')
    file_name = 'sido_sigun_code.json' 
    file_path = os.path.join(download_dir,file_name)
    url = f'http://www.opinet.co.kr/api/areaCode.do?out=json&code={opinet_api_key}'

    data = None
    is_connect = False
    try_count = 5
    delay_seconds = 1

    # 시도 리스트 가져오기
    while not is_connect:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                try:
                    if response.status == 200:
                        print('get_sigun_code url 연결 성공')
                        data = await response.json(content_type='text/html')  # JSON 응답을 직접 파싱
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

    # 시군구 리스트 가져오기
    if data['RESULT']['OIL'] is not None:
        sido_list = data['RESULT']['OIL']
        for sido in sido_list:
            is_connect = False
            try_count = 5
            area_name = sido["AREA_NM"]
            area_code = sido["AREA_CD"]
            print(f'{area_name} areaCode: {area_code}')
            url = f'http://www.opinet.co.kr/api/areaCode.do?out=json&code={opinet_api_key}&area={area_code}'
            
            while not is_connect:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        try:
                            if response.status == 200:
                                sigun_code_data = await response.json(content_type='text/html')
                                sido['SIGUN'] = sigun_code_data['RESULT']['OIL']
                                is_connect = True
                                print(f'{area_name} 시군코드 가져오기 완료')
                            elif try_count > 0:
                                try_count -= 1
                                print(f'get_sigun_code {area_name} url 연결 오류 {delay_seconds}초 후 재시도. 남은 재시도 횟수: {try_count} \nresponse statusCode: {response.status}')
                                time.sleep(delay_seconds)
                            else:
                                print(f"get_sigun_code 종료. url 연결 오류 response statusCode: {response.status}")
                                return
                        except Exception as e:
                            try_count -= 1
                            print(f'get_sigun_code {area_name} url 연결 오류 {delay_seconds}초 후 재시도. 남은 재시도 횟수: {try_count} \nerror: {e}')
                            time.sleep(delay_seconds)
                            if try_count <= 0:
                                print('get_sigun_code 실패. 함수 종료.')
                                return

        # 파일 업데이트
        sido_sigun_code = {"SIDO": sido_list}
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_file = file.read()
        else:
            existing_file = ''

        if json.dumps(sido_sigun_code, ensure_ascii=False) != existing_file:
            print('시도 시군 코드 변경사항 업데이트 완료.')
            with open(file_path, 'w') as file:
                json.dump(sido_sigun_code, file, ensure_ascii=False, indent=4)
        else:
            print('시도 시군 코드 변경사항 없음.')
    else:
        print('에러발생: get_sigun_code data["RESULT"]["OIL"] 값이 null 임.')
        print(f"data['RESULT']['OIL']: {data['RESULT']['OIL']}")

# 비동기 루프 실행
if __name__ == "__main__":
    asyncio.run(get_sigun_code())