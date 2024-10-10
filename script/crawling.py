from bs4 import BeautifulSoup   # 크롤링
from selenium import webdriver  # 크롤링
from selenium.webdriver.chrome.service import Service   # 크롤링
from selenium.webdriver.common.by import By # 크롤링
from webdriver_manager.chrome import ChromeDriverManager    # 크롤링
from selenium.webdriver import ActionChains # 크롤링
from selenium.common.exceptions import NoSuchElementException # 크롤링 - 페이지 에러 발생
import time # 크롤링
import re   # img url에서 자동차 id 추출
import pandas as pd # 전처리 - DataFrame으로 변환
from sqlalchemy import create_engine # DataFrame으로 변환한 데이터 MySQL로 저장

# 케이카 직영중고차 크롤링
def CrawlingKcar(tmp_info):
        html = driver.page_source   # html 파싱
        soup = BeautifulSoup(html, 'html.parser') 
        carIds = [] # 자동차 ID 저장 변수

        # 자동차 ID 크롤링       
        tmpImg = soup.find("div", {"class":"carListWrap"}).find_all("img", {"src":re.compile('https:\\/\\/[\\w.]+\\/([\\w]+|carpicture)')})

        for img in tmpImg:
            if img['alt'] == "챠량이미지":
                tmp = img['src'].split('/')
                
                if len(tmp) == 10:
                    carIds.append(tmp[7].split('_')[0])
                else:
                    carIds.append(tmp[6].split('_')[1])

        # 자동차 정보 크롤링
        carList = soup.find_all("div", {"class":"detailInfo srchTimedeal"})

        # tmp_info에 정보와 id를 담는다
        for item, ids in zip(carList, carIds):
            
            # 자동차 id
            carId = int(ids)

            # 자동차 이름
            name = item.find("div", "carName").text.strip()
            
            # 매매관련 정보들
            info = item.find("div", "carListFlex").text.strip().split('\n')
            price = info[0].strip()   # 가격(단위: 만원)
            details = info[1:]    # 세부사항들
            installment = ""    # 할부, 렌트, 보증금 등등
            model_year = ""   # 연식(단위: x년 x월식)
            distance = ""   # 키로수(단위: xkm)
            fuel = ""   # 연료
            area = ""   # 판매 지역

            # 할부 여부 체크
            if len(details) > 1:
                tmp = details[1].strip().split()

                installment = details[0].strip() + ' | ' + tmp[0] + ' ' + tmp[1] + ' ' + tmp[2]

                try:
                    model_year = tmp[3] + ' ' + tmp[4]
                except:
                    print(f'{name}\n{info}')
                    
                distance = tmp[5][:-2]
                fuel = tmp[6]
                area = tmp[7]
            else:
                tmp = details[0].strip().split()
                
                if tmp[0] == '할부':
                    installment = tmp[0] + ' ' + tmp[1] + ' ' + tmp[2]
                    model_year = tmp[3] + ' ' + tmp[4]
                    distance = tmp[5]
                    fuel = tmp[6]
                    area = tmp[7]
                else:
                    model_year = tmp[0] + ' ' + tmp[1]
                    distance = tmp[2][:-2]
                    fuel = tmp[3]
                    area = tmp[4]

            tmp_info.append({
                                "id" : carId,
                                "name": name, 
                                "price": price,
                                "installment": installment,
                                "year": model_year,
                                "distance": distance,
                                "fuel": fuel,
                                "area": area
                                })

# 페이지 이동
def move_page(page):
    action = ActionChains(driver)

    try:
        button = driver.find_element(By.XPATH, '//*[@id="app"]/div[2]/div[2]/div[2]/div[4]/div[1]/div[7]/div/ul/li[12]/button')
    except NoSuchElementException:  # 다음 페이지 버튼이 없는 경우 => 마지막 페이지를 의미
        return 1 
    except: # 크롤링 중간에 오류 발생한 경우
        print(f'{page} Page Error')
        return -1

    action.click(button).perform()   
    time.sleep(1)

    return 0

# 시간 비교
start_time = time.time()

# 옵션 생성
chrome_options = webdriver.ChromeOptions()

# 브라우저 창 숨기는 옵션
chrome_options.add_argument('headless')

# 크롬 드라이버 최신 버전 설정
service = Service(service=Service(ChromeDriverManager().install()))

# driver 실행
driver = webdriver.Chrome(service=service, options=chrome_options)

# 해당 주소의 웹페이지로 이동
driver.get('https://www.kcar.com/bc/search') 

car_info = []   # 크롤링한 자동차 데이터 저장할 리스트
isLast = 0 # 마지막 페이지 체크
page = 1    # 페이지 번호


# 크롤링 시작
while True:

    CrawlingKcar(car_info)

    # 페이지 이동
    isLast = move_page(page)
    
    if page == 1:
        df = pd.DataFrame(data=car_info)    # 크롤링한 데이터 DataFrame으로 변환
        print(df)
        break

    # -1 또는 1이면 종료
    # -1은 에러 발생, 1은 마지막 페이지를 의미
    if isLast == -1 or isLast == 1:
        driver.quit()
        break

    page += 1
    
if isLast == 1:
    size = len(car_info)
    print(f'총 페이지 : {size}\n')
    print('-----1페이지-----')    
    print(car_info[0])
    print()
    print(f'-----{size}페이지-----')    
    print(car_info[size-1])

# 1페이지 1번째 매물 찜하기 버튼 경로
# //*[@id="app"]/div[2]/div[2]/div[2]/div[4]/div[1]/div[6]/div[2]/div/div[1]/ul/li