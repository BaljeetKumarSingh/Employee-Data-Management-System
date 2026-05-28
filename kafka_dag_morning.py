from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime, timedelta

# Define the default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 5), 
    'retries': 1,
}

# Define the morning job DAG
morning_dag = DAG(
    'morning_job_dag',
    default_args=default_args,
    description='Run morning.py job every day at midnight UTC',
    schedule_interval='0 0 * * *',  # Runs at midnight UTC
    catchup=False,
)

# Define the task to run morning.py
morning_task = BashOperator(
    task_id='trigger_morning_job',
    bash_command="""
    source /home/ubuntu/spark_venv/bin/activate
    export PYSPARK_PYTHON=/home/ubuntu/spark_venv/bin/python
    export PYSPARK_DRIVER_PYTHON=/home/ubuntu/spark_venv/bin/python
    spark-submit /home/ubuntu/morning.py
    """,
    dag=morning_dag
)