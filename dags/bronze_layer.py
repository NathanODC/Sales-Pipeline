from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import sys

sys.path.append("/opt/airflow/")

from support.dag_support import extract_source_data, load_to_gcs_data


with DAG(
    "inbev-case-bronze",
    start_date=datetime(2024, 1, 1),
    schedule_interval="00 30 * * *",
    catchup=False,
) as dag:
    get_source_data = PythonOperator(
        task_id="get_source_data", python_callable=extract_source_data
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data", python_callable=load_to_gcs_data
    )

    load_success = BashOperator(
        task_id="load_sucess", bash_command="echo 'Files upload sucessfully'"
    )

    load_fail = BashOperator(
        task_id="load_fail", bash_command="echo 'Files not upload sucessfully'"
    )

    get_source_data >> load_raw_data >> [load_success, load_fail]
