from airflow import DAG
import os
import datetime
import pendulum
from airflow.operators.bash import BashOperator

DIR_PATH = os.path.abspath(__file__)
SCRIPT_PATH = f'{DIR_PATH}/script'

with DAG(
    dag_id="dags_ex",
    schedule="* * * * *",
    start_date=pendulum.datetime(2024, 12, 11, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="cat /home/hojae/usedCar-Trend/script/crawling.py",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command=f"echo {SCRIPT_PATH}",
    )

    bash_t1 >> bash_t2