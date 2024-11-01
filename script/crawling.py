from bs4 import BeautifulSoup   # 크롤링
from selenium import webdriver  # 크롤링
from selenium.webdriver.chrome.service import Service   # 크롤링
from selenium.webdriver.common.by import By # 크롤링
from webdriver_manager.chrome import ChromeDriverManager    # 크롤링
from selenium.webdriver import ActionChains # 크롤링
from selenium.common.exceptions import NoSuchElementException # 크롤링 - 페이지 에러 발생
import time # 크롤링
import re   # 크롤링 - img url에서 자동차 id 추출

from datetime import datetime as dt # 전처리 - model_year
import pandas as pd # 전처리 - DataFrame으로 변환
import pymysql # 전처리
from sqlalchemy import create_engine, text # 전처리 - DataFrame으로 변환한 데이터 MySQL로 저장


# 케이카 직영중고차 크롤링
def CrawlingKcar(tmp_info):
    html = driver.page_source   # html 파싱
    soup = BeautifulSoup(html, 'html.parser') 
    carIds = [] # 자동차 ID 저장 변수

    # 자동차 ID & 정보 크롤링    
    try:   
        tmp = soup.find("div", {"class":"carListWrap"})
        img = tmp.find_all("img", {"src":re.compile('https:\\/\\/[\\w.]+\\/([\\w]+|carpicture)')})   
        carList = soup.find_all("div", {"class":"detailInfo srchTimedeal"})
    except Exception as e:
        print(e)

    # img URL에서 자동차 ID 추출
    for item in img:
        if item['alt'] == "챠량이미지":
            tmp = item['src'].split('/')
            
            if len(tmp) == 10:
                carIds.append(tmp[7].split('_')[0])
            else:
                carIds.append(tmp[6].split('_')[1])

    # tmp_info에 정보와 id를 담는다
    for item, ids in zip(carList, carIds):
        # 판매 url
        url = "https://www.kcar.com/bc/detail/carInfoDtl?i_sCarCd=EC" + ids

        # 자동차 id
        carId = int(ids)

        # 자동차 이름
        name = item.find("div", "carName").text.strip()
        
        # 매매관련 정보들
        info = item.find("div", "carListFlex").text.strip().split('\n')

        price = int(info[0].strip().split()[0].replace(',', '').replace('만원', ''))   # 가격(단위: 만원)
        details = info[1:]    # 세부사항들
        purchase_type = ""    # 할부, 렌트, 보증금 등등
        model_year = ""   # 연식(단위: x년 x월식)
        distance = ""   # 키로수(단위: xkm)
        fuel = ""   # 연료
        area = ""   # 판매 지역
        crawled_at = dt.today().strftime("%Y-%m-%d") # 크롤링 시작 시각

        # 할부 여부 체크
        if len(details) > 1:
            tmp = details[1].strip().split()

            purchase_type = details[0].strip() + ' | ' + tmp[0] + ' ' + tmp[1] + ' ' + tmp[2]

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
                purchase_type = tmp[0] + ' ' + tmp[1] + ' ' + tmp[2]
                model_year = tmp[3] + ' ' + tmp[4]
                distance = tmp[5][:-2]
                fuel = tmp[6]
                area = tmp[7]
            else:
                model_year = tmp[0] + ' ' + tmp[1]
                distance = tmp[2][:-2]
                fuel = tmp[3]
                area = tmp[4]

        # 거리 - 천단위 콤마 제거 및 정수로 형변환
        distance = int(distance.replace(',', ''))

        # 연식 - (xx년형) 제거 및 xx-xx 형식으로 변경
        tmp_idx = model_year.find('(')
        if tmp_idx != -1:  model_year = model_year[:tmp_idx]
        
        model_year = model_year.replace("년 ", "-").replace("월식","")
        tmp_year = dt.strptime(model_year, '%y-%m').date()
        model_year = tmp_year.strftime("%Y-%m")

        tmp_info.append({
                            "id" : carId,
                            "name": name, 
                            "price": price,
                            "purchase_type": purchase_type,
                            "model_year": model_year,
                            "distance": distance,
                            "fuel": fuel,
                            "area": area,
                            "url" : url,
                            "crawled_at" : crawled_at
                            })

# 페이지 이동
def move_page(p):
    # 페이지 별 태그 번호 계산
    tmp = p

    if tmp % 10 == 1:
        tmp = 12
    elif tmp % 10 == 0:
        tmp = 11
    else:
        tmp = (tmp % 10) + 1

    # 다음 페이지 클릭
    action = ActionChains(driver)

    try:
        button = driver.find_element(By.XPATH, f'//*[@id="app"]/div[2]/div[2]/div[2]/div[4]/div[1]/div[7]/div/ul/li[{tmp}]')
        
    except NoSuchElementException as e: # 크롤링 중간에 오류 발생한 경우
        print(f'{e} At Page {p}')
        return -1

    action.click(button).perform()
    driver.implicitly_wait(10)

# 크롤링 데이터 전처리 및 SQL로 변환
def load(infos):
    db_connections = f'mysql+pymysql://root:!CLT-c403s@localhost/car'
    engine = create_engine(db_connections)
    conn = engine.connect()

    df = pd.DataFrame(data=infos) 
    df.index += 1
    df.to_csv('C:/Users/pirou/OneDrive/바탕 화면/carInfo3.csv')

    if df.duplicated().sum():
        print("Duplicated Data Exists")
        df.drop_duplicates(inplace=True)

    try:
        # crawling 테이블에 삽입
        df.reset_index().rename(columns={"index" : "idx"}).to_sql(name='crawling', con=engine, if_exists='append', index=False)

        # total 테이블과 비교 후, total 테이블에 없는 값들을 삽입
        query1 = """
        INSERT INTO total(id, name, price, purchase_type, model_year, distance, fuel, area, url, crawled_at)
        SELECT id, name, price, purchase_type, model_year, distance, fuel, area, url, crawled_at
        FROM crawling
        WHERE NOT EXISTS (
            SELECT 1
            FROM total
            WHERE total.id = crawling.id
        )
        """
        conn.execute(text(query1))

        # crawling 테이블에 없지만 total 테이블에 있는 값들은 sold 테이블 삽입
        query2 = """
        INSERT INTO sold(id, name, price, purchase_type, model_year, distance, fuel, area, url, crawled_at)
        SELECT id, name, price, purchase_type, model_year, distance, fuel, area, url, crawled_at
        FROM total
        WHERE NOT EXISTS (
            SELECT 1
            FROM crawling
            WHERE crawling.id = total.id
        )
        """
        conn.execute(text(query2))

        query3 = """
        TRUNCATE TABLE crawling
        """
        conn.execute(text(query3))

        conn.commit()
        conn.close()
    except Exception as e:
        print(f'오류 발생 : {e} ')

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
driver.implicitly_wait(10)

car_info = []   # 크롤링한 자동차 데이터 저장할 리스트
isLast = 0 # 마지막 페이지 체크
page = 1    # 페이지 번호

# 크롤링 시작
while True:
    CrawlingKcar(car_info)

    html = driver.page_source   
    soup = BeautifulSoup(html, 'html.parser') 
    nextBtn = soup.find("div", {"class" : "paging"}).find_all("img")    
    paging = soup.find("div", {"class" : "paging"}).find_all("li")
    pages = dict(enumerate([int(*p.text.split()) for p in paging], start = 1))

    try:
        if nextBtn[-1]['alt'] != '다음':
            print(nextBtn[-1]['alt'])
            print(page)

            # 전체 자동차 개수
            total = soup.find("h2", {"class" : "subTitle mt64 ft22"})
            total_KC = int(total.text.strip().split()[-1][:-1].replace(",", ""))
            isLast = 1
            break
    except Exception as e:
        print(e)
        print(nextBtn)
        print(pages)
        break
    
    # 페이지 이동
    page += 1
    isLast = move_page(page)
    time.sleep(1.5)

    if isLast == -1:
        print("Crawling ERROR")
        break

size = len(car_info)

if isLast == 1:    
    print(f'isLast : {isLast}')
    print(f'Total Page : {page}')
    print(f'Total Car Cnt : {total_KC}\n')
    print(f'crawled Data Cnt : {size}\n')
    print('-----1번-----')    
    print(car_info[0])
    print()
    print(f'-----{size}번-----')    
    print(car_info[size-1])

    # 크롤링한 데이터를 SQL로 LOAD
    load(car_info)

else:
    print(f'isLast : {isLast}')
    print(f'Page Error At {page} ')
    print(f'Total Car cnt : {total_KC}\n')
    print(f'crawled Data Cnt : {size}\n')
    print('-----1번-----')    
    print(car_info[0])
    print()
    print(f'-----{size}번-----')    
    print(car_info[size-1])

    df = pd.DataFrame(data=car_info) 
    df.index += 1
    df.to_csv(f'C:/Users/pirou/OneDrive/바탕 화면/tmp{size}.csv')