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
    db_connections = f"postgresql+psycopg2://{config['db_info']['user']}:{config['db_info']['passwd']}@{config['db_info']['host']}/{config['db_info']['db']}?{config['db_info']['charset']}"
    engine = create_engine(db_connections, future=True)

    with engine.begin() as conn:  # with 블록 안에서 commit/rollback 자동 관리        
        try:
            # crawling 테이블에 삽입
            final.to_sql(name='crawling', con=engine, if_exists='append', index=False)

            # crawling 테이블과 main 테이블 비교 후, 신규 차량만 삽입
            # 만약, 이전에 판매된 차량 중 id를 재활용하여 새로운 차량이 등록된 경우, 
            # 해당 차량 정보들을 변경하여 저장
            query1 = """
            INSERT INTO main(id, name, model_year, km, fuel, area, url)
            SELECT DISTINCT id, name, model_year, km, fuel, area, url
            FROM crawling
            ON CONFLICT (id) DO UPDATE
            SET
                name    = EXCLUDED.name,
                model_year = EXCLUDED.model_year,
                km         = EXCLUDED.km,
                fuel       = EXCLUDED.fuel,
                area       = EXCLUDED.area,
                url        = EXCLUDED.url
            """
            conn.execute(text(query1))

            # 이전에 등록된 차량의 정보가 크롤링한 데이터와 다르면 판매되었다고 판단
            # 이후, crawling 테이블에 존재하지 않는 차량의 정보들은 삭제
            query2 = """
            DELETE FROM price_info p
            WHERE
                p.id IN (SELECT DISTINCT id FROM crawling)
                AND NOT EXISTS (
                    SELECT 1
                    FROM crawling c
                    WHERE c.id = p.id AND c.pc_type = p.pc_type
                )
            """
            conn.execute(text(query2))

            # crawling 테이블의 모든 데이터를 price_info로 삽입
            # 만약, 존재한다면 새로운 값으로 업데이트
            query3 = """
            INSERT INTO price_info (id, pc_type, monthly_cost, price)
            SELECT DISTINCT id, pc_type, monthly_cost, price
            FROM crawling
            ON CONFLICT (id, pc_type) DO UPDATE 
            SET
                monthly_cost = EXCLUDED.monthly_cost,
                price = EXCLUDED.price
            """
            conn.execute(text(query3))

            # crawling 테이블과 sales_list 테이블 비교 후, 신규 차량만 삽입
            query4 = """
            INSERT INTO sales_list(id, crawled_at)
            SELECT DISTINCT id, crawled_at
            FROM crawling
            ON CONFLICT (id) DO NOTHING
            """
            conn.execute(text(query4))

            # 이전 목록에 있었지만 판매된 차량을 '판매 완료'로 처리
            query5 = """
            UPDATE sales_list s
            SET is_sold = TRUE, sold_at = CURRENT_DATE
            WHERE 
                s.is_sold = FALSE 
                AND NOT EXISTS(
                    SELECT 1
                    FROM crawling c
                    WHERE c.id = s.id
                )
            """
            conn.execute(text(query5))
            
            # 이전에 판매된 차량이지만, 같은 id로 등록된 새로운 차량의 상태를 '판매 중'으로 복원
            query6 = """
            UPDATE sales_list s
            SET is_sold = FALSE, crawled_at = CURRENT_DATE, sold_at = NULL
            FROM crawling c
            WHERE 
                s.is_sold = TRUE
                AND s.id = c.id
            """
            conn.execute(text(query6))

            # 모든 과정을 마친 후 crawling 테이블 전체 초기화
            query7 = """
            DELETE FROM crawling
            """
            conn.execute(text(query7))

        except Exception as e:
            print(f'오류 발생 : {e} ')