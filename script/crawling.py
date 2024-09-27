from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


# 크롬 드라이버 최신 버전 설정
service=Service(ChromeDriverManager().install())

with webdriver.Chrome(service=service) as driver:
    endpoint = ""

    driver.get("https://www.kcar.com/bc/search")    # 해당 주소의 웹페이지로 이동
    driver.implicitly_wait(0.5)
    
    html = driver.page_source

    # html 파싱
    soup = BeautifulSoup(html, 'html.parser')

    # 필요한 태그에 접근
    car_list = soup.find_all("div", {"class":"detailInfo srchTimedeal"})
    car_infor_dict = {}
    
    for idx, item in enumerate(car_list):
        # 자동차 이름을 car_name_list에 넣는다
        name = item.find("div", "carName").text.strip()
        
        # 자동차관련 정보들을 보기 좋게 나누어서 car_infor_dict에 넣는다.
        info = item.find("div", "carListFlex").text.strip().split('\n')
        price = info[0]   # 가격
        
        details = info[1].strip().split()   # 세부사항
        installment = ""    # 할부
        model_year = ""   # 연식
        distance = ""   # 키로수
        fuel = ""   # 연료
        area = ""   # 판매 지역

        # 할부 여부 체크
        if details[0] == "할부":
            installment = details[0] + ' ' + details[1] + ' ' + details[2]
            model_year = details[3] + ' ' + details[4]
            distance = details[5]
            fuel = details[6]
            area = details[7]
        else:
            model_year = details[0] + ' ' + details[1]
            distance = details[2]
            fuel = details[3]
            area = details[4]

        car_infor_dict[idx+1] = {"name": name, 
                                "price": price,
                                "installment": installment,
                                "year": model_year,
                                "distance": distance,
                                "fuel": fuel,
                                "area": area
                                }

    print(car_infor_dict)