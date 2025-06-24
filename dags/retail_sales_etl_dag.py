
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'harshitha',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='retail_sales_etl',
    default_args=default_args,
    start_date=datetime(2025, 6, 24),
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'etl'],
) as dag:

    bronze = BashOperator(
        task_id='bronze_layer',
        bash_command='spark-submit /opt/airflow/dags/scripts/bronze_layer.py'
    )

    silver = BashOperator(
        task_id='silver_layer',
        bash_command='spark-submit /opt/airflow/dags/scripts/silver_layer.py'
    )

    gold = BashOperator(
        task_id='gold_layer',
        bash_command='spark-submit /opt/airflow/dags/scripts/gold_layer.py'
    )

    bronze >> silver >> gold
