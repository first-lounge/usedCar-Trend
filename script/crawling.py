import sys  # Airflow에서 실패 처리를 위해 추가
from datetime import datetime as dt # 전처리 - model_year
from tl_process import load   # 변환

# 크롤링
import re   # img url에서 자동차 id 추출
import time 
from bs4 import BeautifulSoup   
from selenium import webdriver  
from selenium.webdriver.common.by import By 
from selenium.webdriver import ActionChains 
from selenium.webdriver.chrome.service import Service   
from webdriver_manager.chrome import ChromeDriverManager    
from selenium.common.exceptions import NoSuchElementException # 페이지 에러 발생

# 케이카 직영중고차 크롤링
def CrawlingKcar(page, result):
    html = driver.page_source   # html 파싱
    soup = BeautifulSoup(html, 'html.parser') 
    carIds = [] # 자동차 ID 저장 변수

    # 자동차 ID & 정보    
    infos = soup.find("div", {"class":"resultCnt"})
    imgs = infos.find_all("img", {"src":re.compile('https:\\/\\/[\\w.]+\\/([\\w]+|carpicture)')})   
    carList = infos.find_all("div", {"class":"detailInfo srchTimedeal"})

    # img URL에서 자동차 ID 추출
    for img in imgs:
        if img['alt'] == "챠량이미지":
            tmp = img['src'].split('/')
            
            if len(tmp) == 10:
                carIds.append(tmp[7].split('_')[0])
            else:
                carIds.append(tmp[6].split('_')[1])

    # result에 정보와 id를 담는다
    for item, ids in zip(carList, carIds):
        try:
            # 판매 url
            url = "https://www.kcar.com/bc/detail/carInfoDtl?i_sCarCd=EC" + ids

            # 자동차 id
            carId = int(ids)

            # 자동차 이름
            name = item.find("div", {"class":"carName"}).text.strip()

            # 가격(단위: 만원)
            price = int(item.find("p", {"class":"carExp"}).get_text().split()[0].replace(",", "").replace("만원", ""))

            # 판매 방식(할부, 렌트, 리스 등)
            # ex) 할부 월xx만원 | 렌트 월xx만원
            pc_type = " | ".join([tmp.get_text().lstrip() for tmp in item.find("ul", {"class":"carPayMeth"}).find_all("span", {"class":"el-link--inner"})])

            # 세부 사항
            detail = item.find("p", {"class":"detailCarCon"}).find_all("span")

            # 연식(단위: x년 x월식) - (xx년형) 제거 및 xx-xx 형식으로 변경
            tmp = detail[0].text[:detail[0].text.find('식')].replace("년 ", "-").replace("월","")
            model_year= dt.strptime(tmp, '%y-%m').date().strftime("%Y-%m")

            # 키로수(단위: xkm) - 천단위 콤마 제거 및 정수로 형변환
            km = int(detail[1].text.replace('km', '').replace(',', ''))   
            
            fuel = detail[2].text   # 연료
            
            area = detail[3].text   # 판매 지역
            
            crawled_at = dt.today().strftime("%Y-%m-%d") # 크롤링 시작 시각

            result.append({
                                "id" : carId,
                                "name": name, 
                                "price": price,
                                "pc_type": pc_type,
                                "model_year": model_year,
                                "km": km,
                                "fuel": fuel,
                                "area": area,
                                "url" : url,
                                "crawled_at" : crawled_at
                                })
        except Exception as e:  # 크롤링 에러 발생 시
            print(f"{page}page")
            print(e)
            sys.exit(1)
        

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
        
    except NoSuchElementException: # 오류 발생 or 마지막 페이지인 경우
        return -1

    action.click(button).perform()
    driver.implicitly_wait(10)

# 옵션 생성
chrome_options = webdriver.ChromeOptions()

# 브라우저 창 숨기는 옵션
chrome_options.add_argument("--headless")

chrome_options.add_argument("--no-sandbox")

chrome_options.add_argument("--disable-dev-shm-usage")

# ChromeDriverManager로 ChromeDriver 경로 지정
driver_path = ChromeDriverManager().install()

# Chrome 드라이버 서비스 설정
service = Service(driver_path)

# driver 실행
driver = webdriver.Chrome(service=service, options=chrome_options)

# 해당 주소의 웹페이지로 이동
driver.get("https://www.kcar.com/bc/search") 
driver.implicitly_wait(10)

car_info = []   # 크롤링 데이터 저장하는 리스트
isLast = 0 # 마지막 페이지인지 체크
page = 1    # 페이지
total = 0   # 전체 자동차 개수

# 크롤링 시작
while True:
    time.sleep(1.5)
    CrawlingKcar(page, car_info)
    
    # 마지막 페이지인지 확인
    html = driver.page_source   
    soup = BeautifulSoup(html, 'html.parser') 
    nextBtn = soup.find("div", {"class" : "paging"}).find_all("img")    
    paging = soup.find("div", {"class" : "paging"}).find_all("li")
    pages = dict(enumerate([int(*p.text.split()) for p in paging], start = 1))
    
    try:
        if nextBtn[-1]['alt'] != '다음':
            total = int(soup.find("h2", {"class" : "subTitle mt64 ft22"}).find("span", {"class":"textRed"}).text.strip().replace(',', ''))

            print(nextBtn[-1]['alt'])
            isLast = 1
            break
    except Exception as e:  # 크롤링 에러 발생 시
        print(e)
        print(pages)
        break
    
    # 페이지 이동
    page += 1

    if move_page(page) == -1:
        break

if isLast == 1:    
    print(f'Total Page : {page - 1}')
    print(f'Total Car Cnt : {total}')
    print(f'crawled Data Cnt : {len(car_info)}')
    
    # 크롤링한 데이터 전처리 및 SQL로 삽입
    load(car_info)
else:
    print("Crawling ERROR")
    print(f'Page Error At {page} ')
    sys.exit(1)

driver.quit()