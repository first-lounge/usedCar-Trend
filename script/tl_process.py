import pymysql
import configparser   # ini 파일 읽기
import pandas as pd 
from sqlalchemy import create_engine, text 

def info_transform(df):
    # 데이터프레임의 pc_type 컬럼에서 '할부 ... | 렌트 ...'로 있는 경우 처리
    df['pc_type'] = df['pc_type'].str.split(' \| ')  # 문자열을 리스트로 분리
    df = df.explode('pc_type').reset_index(drop=True)  # 리스트를 행으로 확장

    # price의 purchaset_type 값들을 [할부, 비용] 형식으로 리스트로 만든 후, 새 컬럼인 tmp에 삽입
    df['pc_type'] = df['pc_type'].str.replace('만원','')
    df[['pc_type', 'monthly_cost']] = df['pc_type'].str.split(' \월 ', expand=True)
    df.drop('pc_type', axis=1, inplace=True)

    # price의 monthly_cost 컬럼 타입 변경 및 새로운 dataframe에 삽입
    df['monthly_cost'] = df['monthly_cost'].astype(int)
            
    return df

def load(info):
    # sql 연결
    config = configparser.ConfigParser()
    config.read('C:/Users/pirou/OneDrive/바탕 화면/중고차 매매 프로젝트/settings.ini')

    db_connections = f'mysql+pymysql://{config['db_info']['user']}:{config['db_info']['passwd']}@{config['db_info']['host']}/{config['db_info']['db']}'
    engine = create_engine(db_connections)
    conn = engine.connect()

    df = pd.DataFrame(data=info)
    df.to_csv('C:/Users/pirou/OneDrive/바탕 화면/carInfos1.csv')

    if df.duplicated().sum():
        print("Duplicated Data Exists")
        df.drop_duplicates(inplace=True)

    # 데이터 전처리
    final = info_transform(df)

    try:
        # crawling 테이블에 삽입
        final.to_sql(name='crawling', con=engine, if_exists='append', index=False)

        # crawling 테이블과 main 테이블 비교 후, main 테이블에 없는 값들 삽입
        query1 = """
        INSERT IGNORE INTO main_tmp(id, name, crawled_at)
        SELECT id, name, crawled_at
        FROM crawling
        WHERE NOT EXISTS (
            SELECT 1
            FROM main_tmp
            WHERE main_tmp.id = crawling.id
        )
        """
        conn.execute(text(query1))

        # crawling 테이블과 car_info 테이블 비교 후, car_info 테이블에 없는 값들 삽입
        query2 = """
        INSERT IGNORE INTO car_info_tmp(id, model_year, distance, fuel, area, url)
        SELECT id, model_year, distance, fuel, area, url
        FROM crawling
        WHERE NOT EXISTS (
            SELECT 1
            FROM car_info_tmp
            WHERE car_info_tmp.id = crawling.id
        )
        """
        conn.execute(text(query2))

        # main 테이블과 crawling 테이블 비교 후, 판매 여부 업데이트
        query = """
        UPDATE main_tmp mt
        SET mt.is_sold = 1
        WHERE NOT EXISTS(
            SELECT 1
            FROM crawling
            WHERE crawling.id = mt.id
        )
        """
        conn.execute(text(query))
        
        # price_info 테이블에 없는 새로운 차량들 삽입
        query3 = """
        INSERT INTO price_info_tmp(id, pc_type, monthly_cost, price)
        SELECT id, pc_type, monthly_cost, price
        FROM crawling
        WHERE NOT EXISTS (
            SELECT 1
            FROM price_info_tmp
            WHERE price_info_tmp.id = crawling.id
        )
        """
        conn.execute(text(query3))
        
        # 모든 과정을 마친 후 crawling 테이블 전체 초기화
        query4 = """
        TRUNCATE TABLE crawling
        """
        conn.execute(text(query4))

        conn.commit()
        conn.close()
    except Exception as e:
        print(f'오류 발생 : {e} ')