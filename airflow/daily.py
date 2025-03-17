from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    'email_on_retry': False,
    'email_on_failure': True,
    'email' : ['zxcz9878@email.com']
}

with DAG(
    dag_id="daily",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 17, tz="Asia/Seoul"),   # 한국 시간 timezone 설정
    catchup=False
) as dag:
    crawling = BashOperator(
        task_id="start_crawling",
        bash_command=f"cd /root/usedCar-Trend/script; python3 tl_process.py"
    )
    send_email = EmailOperator(
        task_id='error_email',
        to='pirouette36@naver.com',
        subject='크롤링 실패',
        html_content='크롤링이 실패하였습니다.',
    )
    
    crawling >> send_email