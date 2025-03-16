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
    start_date=datetime(2025, 3, 17, tzinfo=pendulum.timezone("Asia/Seoul")),   # 한국 시간 timezone 설정
    schedule_interval='0 */8 * * *',
    catchup=False,
    retries=3,  # 실패 시, 3번 재시도
    retry_delay=timedelta(minutes=5), # 재시도하는 시간 간격
) as dag:
    email_operator = EmailOperator(
        task_id='send_email',
        to='pirouette36@naver.com',
        subject='[TEST] 테스트 메일입니다.',
        html_content="""
                        테스트 메일입니다.<br/><br/> 
                        ninja template<br/>
                        {{ data_interval_start }}<br/>
                        {{ ds }}<br/>
                    """,
    )
    
    email_operator
    # bash_t1 = BashOperator(
    #     task_id="bash_t1",
    #     bash_command=f"cd /root/usedCar-Trend/script; python3 crawling.py"
    # )

    # bash_t1