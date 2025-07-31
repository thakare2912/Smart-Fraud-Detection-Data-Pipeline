
import os
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "hanumant",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#Define DAG
dag = DAG(
    "frausd_detect",
    default_args=default_args,
    description="fraud_detect",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
)
  
load_to_snowflake_1 = BashOperator(
    task_id="user_spend_trends",
    bash_command = "python /opt/airflow/dags/scripts/load_data_snowflake_user_spend_trends.py {{ ds }}",
    dag=dag
)

load_to_snowflake_2 = BashOperator(
    task_id="snowflake_fraud_records",
    bash_command = "python /opt/airflow/dags/scripts/load_fraud_records.pyto_snowflake_fraud_records.py {{ ds }}",
    dag=dag
)

load_to_snowflake_3 = BashOperator(
    task_id="category_trends",
    bash_command = "python /opt/airflow/dags/scripts/load_to_snowflake_category_trends.py {{ ds }}",
    dag=dag
)

complete = DummyOperator(
    task_id = "complelete",
    dag = dag
)

load_to_snowflake_1 >> complete
load_to_snowflake_2  >> complete
load_to_snowflake_3  >> complete
