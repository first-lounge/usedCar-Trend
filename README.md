# UsedCar Trend 
중고차 매매 데이터 추출 파이프라인 구축 프로젝트

## 프로젝트 소개
Kcar 사이트에서 중고차 매매 데이터를 크롤링하여 데이터 파이프라인 구축 및 시각화하였습니다.

## 프로젝트 구성
홈페이지 - https://usedcar-trend.streamlit.app/
<details>
  <summary>메인화면</summary>
  <figure class="half"><a href="link"><img src="./img/main.png"></a> <a href="link"><img src="./img/main2.png"></a> </figure> 
</details>

<details>
  <summary>브랜드 및 차량 선택 화면</summary>
  <figure class="half"><a href="link"><img src="./img/brand_selected.png"></a> <a href="link"><img src="./img/brand_car_selected.png" "></a></figure> 
</details>

## 시작 가이드
### 요구사항
- Python 3.12
- MySQL
- Streamlit

## ERD 다이어그램
![ERD](./img/db_schema.png)

## 아키텍처
![Architecture](./img/architecture.png)
### *Data Source*
- [Kcar](https://www.kcar.com/bc/search)

### *Extract*
- Selenium으로 동적 크롤링 및 BS4로 파싱 진행
- 각 페이지 별로 자동차 정보들을 추출

### *Transfrom*
- 추출한 자동차 데이터들을 DB에 적재시키기 위한 형태로 전처리 및 Dictionary로 변환
- 변환한 Dictionary 데이터들을 전체 List에 저장
- 전체 List를 DataFrame으로 변환 및 중복 여부 확인
### *Load*
- DataFrame으로 변환한 데이터들을 MySQL에 삽입
- MySQL에는 총 1개의 임시 테이블(crawling)과 3개의 테이블(main, price_info, sales_list) 존재


## :exclamation: 알게된 점들
### *1. 대량의 데이터 저장*
- SQLAlchemy로 DB INSERT 과정
  > for문으로 12826개의 데이터를 하나씩 INSERT하면서 매번 commit을 진행했고 총 5.22초가 걸렸습니다. <br>
  > ![12826](./img/sqlalchemy_execute_12826.png)

- 데이터 개수가 약 20만개라면?
  > for문 + execute을 사용했더니 약 5분 34초가 걸렸습니다. <br>
  > ![205216](./img/sqlalchemy_execute_205216.png)

  > to_sql을 사용하고 chunksize를 20000으로 설정하니 8.99초가 걸렸습니다. <br>
  > ![20000](./img/sqlalchemy_to_sql.png)

- execute과 to_sql 성능 차이 발생 이유
  - execute 같은 경우 매번 commit을 호출하고 to_sql은 전체 데이터를 한 번에 DB에 넣기 때문에 commit이 한 번만 호출됩니다.
  - 매번 commit을 호출하는 방식은 디스크에 많이 접근하기 때문에 성능이 저하될 가능성이 높습니다.
  - 따라서, 대량의 데이터인 경우 batch 단위로 삽입하는 **to_sql**을 사용하는 것이 효율적입니다.


