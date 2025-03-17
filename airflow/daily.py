from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator



with DAG(
    dag_id="daily",
    # schedule="0 8 1 * *", # 매월 1일 8시
    start_date=pendulum.datetime(2025, 3, 17, tzinfo=pendulum.timezone("Asia/Seoul")),
    catchup=False
) as dag:
    send_email = EmailOperator(
        task_id='email_test',
        to='pirouette36@naver.com', # 수신자
        # cc= '참조'
        subject='Airflow 성공메일', # 제목
        html_content='Airflow 작업이 완료되었습니다' # 내용
    )

send_email


# with DAG(
#     dag_id="daily",
#     start_date=datetime(2025, 3, 17, tzinfo=pendulum.timezone("Asia/Seoul")),   # 한국 시간 timezone 설정
#     catchup=False
# ) as dag:
    # crawling = BashOperator(
    #     task_id="start_crawling",
    #     bash_command=f"cd /root/usedCar-Trend/script; python3 crawling.py"
    # )

    # crawling