from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains
import time
import sys

# 케이카 크롤링
def CrawlingKcar():
        html = driver.page_source   # html 파싱
        soup = BeautifulSoup(html, 'html.parser')
        
        car_list = soup.find_all("div", {"class":"detailInfo srchTimedeal"})
        tmp_info = {}

        for idx, item in enumerate(car_list):
            # 자동차 이름을 car_name_list에 넣는다
            name = item.find("div", "carName").text.strip()
            
            # 매매관련 정보들
            info = item.find("div", "carListFlex").text.strip().split('\n')
            price = info[0].strip()   # 가격
            details = info[1:]    # 세부사항
            installment = ""    # 할부 / 렌트 / 보증금 등등
            model_year = ""   # 연식
            distance = ""   # 키로수
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
                    
                distance = tmp[5]
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
                    distance = tmp[2]
                    fuel = tmp[3]
                    area = tmp[4]

            tmp_info[idx+1] = {"name": name, 
                                    "price": price,
                                    "installment": installment,
                                    "year": model_year,
                                    "distance": distance,
                                    "fuel": fuel,
                                    "area": area
                                    }

        return tmp_info

chrome_options = webdriver.ChromeOptions()
# 브라우저 꺼짐 방지 옵션
chrome_options.add_experimental_option("detach", True)

# 크롬 드라이버 최신 버전 설정
service=Service(service=Service(ChromeDriverManager().install()), options=chrome_options)

with webdriver.Chrome(service=service) as driver:
    driver.get("https://www.kcar.com/bc/search")    # 해당 주소의 웹페이지로 이동
    car_info = []   # 크롤링한 자동차 데이터 저장할 리스트
    page = 1    # 페이지 번호

    # 홈페이지의 각 페이지마다 크롤링
    while page <= 3:

        car_info.append(CrawlingKcar())

        # 페이지 이동
        action = ActionChains(driver)
        button = driver.find_element(By.XPATH, '//*[@id="app"]/div[2]/div[2]/div[2]/div[4]/div[1]/div[7]/div/ul/li[12]/button')
        action.click(button).perform()   
        time.sleep(1)

        page += 1
        
    print(car_info)