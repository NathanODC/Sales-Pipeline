from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import sys

sys.path.append("/opt/airflow/")

from support.dag_support import pull_gcs_data, transform_to_df


with DAG(
    "inbev-case-silver",
    start_date=datetime(2024, 1, 1),
    schedule_interval="00 45 * * *",
    catchup=False,
) as dag:
    get_raw_data = PythonOperator(task_id="get_raw_data", python_callable=pull_gcs_data)

    create_dataframe = PythonOperator(
        task_id="create_dataframe", python_callable=transform_to_df
    )

    get_raw_data >> create_dataframe
