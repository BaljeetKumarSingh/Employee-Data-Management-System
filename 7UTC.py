from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 1),
    'retries': 0,
    'catchup': False
}

DAILY_GLUE_JOBS = [
    'employee-data-management-system-employee-data',
    'employee-data-management-system-employee-timeframe-data',
    'employee-data-management-system-employee-leave-data',
    'employee-data-management-system-count_by_designation',
    'employee-data-management-system-threshold'
]

MONTHLY_JOB = 'employee-data-management-system-Quota_80%'

with DAG(
    dag_id='daily_employee_glue_jobs_sequential',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    max_active_runs=1,
    description='Trigger Glue jobs in sequence with optional monthly job',
    tags=['glue', 'daily', 'conditional']
) as dag:

    previous_task = None

    for job_name in DAILY_GLUE_JOBS:
        task = GlueJobOperator(
            task_id=f'trigger_{job_name.replace("-", "_")}',
            job_name=job_name,
            region_name='us-east-1',
            wait_for_completion=True,
            trigger_rule='all_done'
        )

        if previous_task:
            previous_task >> task
        previous_task = task

    # Conditional Monthly Job (runs only on 1st)
    from airflow.operators.python import BranchPythonOperator
    from airflow.operators.dummy import DummyOperator

    def is_first_day():
        return 'quota_job' if datetime.utcnow().day == 1 else 'skip_quota'

    check_day = BranchPythonOperator(
        task_id='check_first_day',
        python_callable=is_first_day
    )

    quota_job = GlueJobOperator(
        task_id='quota_job',
        job_name=MONTHLY_JOB,
        region_name='us-east-1',
        wait_for_completion=True
    )

    skip_quota = DummyOperator(task_id='skip_quota')

    previous_task >> check_day
    check_day >> [quota_job, skip_quota]
