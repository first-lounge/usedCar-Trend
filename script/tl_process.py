import os
import pandas as pd 
from pathlib import Path
from datetime import datetime as dt
from configparser import ConfigParser   # ini 파일 읽기
from sqlalchemy import create_engine, text 

BASE_DIR = Path(__file__).resolve().parent.parent  # crawling.py 상위 폴더
config_path = BASE_DIR / 'settings.ini'
config = ConfigParser()
config.read(config_path, encoding='utf-8')

def info_transform(df):
  # 데이터프레임의 pc_type 컬럼에서 '할부 ... | 렌트 ...'로 있는 경우 처리
  df['pc_type'] = df['pc_type'].str.split(' \| ')  # 문자열을 리스트로 분리
  df = df.explode('pc_type').reset_index(drop=True)  # 리스트를 행으로 확장

  # price의 purchaset_type 값들을 [할부, 비용] 형식으로 리스트로 만든 후, 새 컬럼인 tmp에 삽입
  df['pc_type'] = df['pc_type'].str.replace('만원','')
  df[['pc_type', 'monthly_cost']] = df['pc_type'].str.split(' \월 ', expand=True)

  # price의 monthly_cost 컬럼 타입 변경 및 새로운 dataframe에 삽입
  df['monthly_cost'] = df['monthly_cost'].astype(int)
            
  return df

def load(info):
    df = pd.DataFrame(data=info)

    if df.duplicated().sum():
        print(f"Duplicated Data Exists : {df.duplicated().sum()}")
        df.drop_duplicates(inplace=True)

    if (df.isnull().sum() > 0).any():
        print(f"Null Data Exists : {df.isnull().sum()}")
        df.dropna(inplace=True)

    save_path = os.path.join(config['paths']['data_dir'], f'carInfos_{dt.now().strftime("%Y%m%d%H")}.csv')
    df.to_csv(save_path)       

    # 데이터 전처리
    final = info_transform(df)

    # DB 연결 및 삽입
    db_connections = f"mysql+pymysql://{config['db_info']['user']}:{config['db_info']['passwd']}@{config['db_info']['host']}/{config['db_info']['db']}"
    engine = create_engine(db_connections, future=True)
    
    with engine.begin() as conn:  # with 블록 안에서 commit/rollback 자동 관리        
        try:
            # crawling 테이블에 삽입
            final.to_sql(name='crawling', con=engine, if_exists='append', index=False)

            # crawling 테이블과 main 테이블 비교 후, main 테이블에 없는 값들 삽입
            query1 = """
            INSERT INTO main(id, name, model_year, km, fuel, area, url)
            SELECT DISTINCT id, name, model_year, km, fuel, area, url
            FROM crawling
            WHERE NOT EXISTS (
                SELECT 1
                FROM main
                WHERE main.id = crawling.id
            )
            """
            conn.execute(text(query1))

            # crawling 테이블과 price_info 테이블 비교 후, price_info 테이블에 없는 값들 삽입
            query2 = """
            INSERT INTO price_info(id, pc_type, monthly_cost, price)
            SELECT id, pc_type, monthly_cost, price
            FROM crawling
            WHERE NOT EXISTS (
                SELECT 1
                FROM price_info
                WHERE price_info.id = crawling.id
            )
            """
            conn.execute(text(query2))

            # crawling 테이블과 sales_list 테이블 비교 후, sales_list에 없는 값들 삽입
            query3 = """
            INSERT INTO sales_list(id, crawled_at)
            SELECT DISTINCT id, crawled_at
            FROM crawling
            WHERE NOT EXISTS (
                SELECT 1
                FROM sales_list
                WHERE sales_list.id = crawling.id
            )
            """
            conn.execute(text(query3))

            # sales_list 테이블과 crawling 테이블 비교 후, is_sold 컬럼 업데이트
            query4 = """
            UPDATE sales_list sl
            SET is_sold = TRUE, sold_at = CURRENT_DATE
            WHERE 
                is_sold = FALSE 
                AND NOT EXISTS(
                    SELECT 1
                    FROM crawling
                    WHERE crawling.id = sl.id
                )
            """
            conn.execute(text(query4))

            query5 = """
            UPDATE sales_list sl
            SET is_sold = FALSE, sold_at = null
            WHERE 
                is_sold = TRUE
                AND EXISTS(
                    SELECT 1
                    FROM crawling
                    WHERE crawling.id = sl.id
                )
            """
            conn.execute(text(query5))

            # 모든 과정을 마친 후 crawling 테이블 전체 초기화
            query6 = """
            DELETE TABLE crawling
            """
            conn.execute(text(query6))

        except Exception as e:
            print(f'오류 발생 : {e} ')