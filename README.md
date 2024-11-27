# UsedCar Trend
중고차 매매 데이터 추출 파이프라인 구축 프로젝트

## 프로젝트 소개
Kcar 사이트에서 중고차 매매 데이터를 크롤링하여 데이터 파이프라인 구축 및 시각화하였습니다.

## 시작 가이드
### 요구사항
- Python 3.12
- MySQL
- Streamlit

## 아키텍처
### Data Sources
- Kcar (https://www.kcar.com/bc/search)

### Extract
- Selenium으로 동적 크롤링을 진행
- 각 페이지 별로 자동차 정보들을 추출

### Transfrom
### Load
