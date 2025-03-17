from airflow import DAG
from datetime import timedelta
import pendulum
from airflow.operators.bash import BashOperator

default_args = {
    'email_on_retry': False,
    'email_on_failure': True,
    'email': ['pirouette36@naver.com'],
    'retries': 3,  # 실패 시 최대 2번 재시도
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
}

with DAG(
    dag_id="daily",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 18, tz="Asia/Seoul"),  # 한국 시간 timezone 설정,
    schedule_interval="0 */6 * * *",
    catchup=False
) as dag:
    crawling = BashOperator(
        task_id="start_crawling",
        bash_command="cd /root/usedCar-Trend/script; python3 crawling.py"
    )