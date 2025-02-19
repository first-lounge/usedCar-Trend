from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.bash import BashOperator

# 한국 시간 timezone 설정
local_tz = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="daily",
    start_date=datetime(2025, 2, 17, tz=local_tz),
    schedule_interval='0 */8 * * *',
    catchup=False,
    email_on_failure=True,
    email_on_retry=False,
    retries=3,  # 실패 시, 3번 재시도
    retry_delay=timedelta(minutes=5), # 재시도하는 시간 간격
) as dag:

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command=f"cd /home/hojae/usedCar-Trend/script; python3 crawling.py"
    )

    bash_t1