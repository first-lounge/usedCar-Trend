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
        bash_command="pwd",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command=f"cd {SCRIPT_PATH}",
    )

    bash_t3 = BashOperator(
        task_id="bash_t3",
        bash_command=f"cd ./airflow/../usedCar-Trend/script; ls"
    )

    bash_t1 >> bash_t3 >> bash_t2