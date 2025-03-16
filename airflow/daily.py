from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator



# default_args = {
#     'email_on_retry': False,
#     'email_on_failure': True,
#     'email' : ['zxcz9878@email.com']
# }


with DAG(
    dag_id="daily",
    # default_args=default_args,
    start_date=datetime(2025, 3, 17, tzinfo=pendulum.timezone("Asia/Seoul")),   # 한국 시간 timezone 설정
    catchup=False
) as dag:
    # email_operator = EmailOperator(
    #     task_id='send_email',
    #     to='zxcz9878@gmail.com',
    #     subject='[TEST] 테스트 메일입니다.',
    #     html_content="""
    #                     테스트 메일입니다.<br/><br/> 
    #                     ninja template<br/>
    #                     {{ data_interval_start }}<br/>
    #                     {{ ds }}<br/>
    #                 """,
    # )
    
    # email_operator
    crawling = BashOperator(
        task_id="start_crawling",
        bash_command=f"cd /root/usedCar-Trend/script; python3 crawling.py"
    )

    crawling