from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 5),  
    'retries': 1,
}

# Define the every job DAG
every_dag = DAG(
    'every_job_dag',
    default_args=default_args,
    description='Run every.py job every 5 minutes',
    schedule_interval='*/5 * * * *',  # Runs every 5 minutes
    catchup=False,
)

# Define the task to run every.py
every_task = BashOperator(
    task_id='trigger_every_job',
    bash_command="""
    source /home/ubuntu/spark_venv/bin/activate
    export PYSPARK_PYTHON=/home/ubuntu/spark_venv/bin/python
    export PYSPARK_DRIVER_PYTHON=/home/ubuntu/spark_venv/bin/python
    spark-submit /home/ubuntu/every.py
    """,
    dag=every_dag
)