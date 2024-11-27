import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DIR_PATH = os.path.abspath('.')
SCRIPT_PATH = f'{DIR_PATH}\script'

# DAG의 기본 설정을 정의하는 딕셔너리
default_args = {
    'owner': 'hojae',  # DAG의 소유자 또는 책임자
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 27, tzinfo="Aisa/Seoul"),  # DAG 시작 날짜
    'catchup':False,
    'schedule_interval':timedelta(hours=8), # 8시간마다 실행
    'retries': 4,  # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 간 대기 시간
}

# DAG 객체 생성
with DAG(
    dag_id='daily_dag',  # DAG의 고유 식별자
    default_args=default_args,  # 위에서 정의한 기본 설정 적용
    description='데이터 변환 및 적재 파이프라인',  # DAG에 대한 설명
    schedule_interval='@daily',  # DAG 실행 주기 (매일)
) as dag:
    crawling_task = BashOperator(
        task_id=f'crawling',
        bash_command=f'python3 {SCRIPT_PATH}\crawling.py'
    )

    crawling_task