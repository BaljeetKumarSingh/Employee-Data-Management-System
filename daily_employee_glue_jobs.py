from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 1),
    'retries': 0
}

DAILY_GLUE_JOBS = {
    'employee_data': 'employee-data-management-system-employee-data',
    'timeframe_data': 'employee-data-management-system-employee-timeframe-data',
    'leave_data': 'employee-data-management-system-employee-leave-data',
    'count_by_designation': 'employee-data-management-system-count_by_designation',
    'threshold': 'employee-data-management-system-threshold'
}

MONTHLY_JOB = 'employee-data-management-system-Quota_80%'

with DAG(
    dag_id='daily_employee_glue_jobs_grouped1',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    max_active_runs=1,
    catchup=False,
    description='Organized Glue job DAG with dependencies and monthly conditional task',
    tags=['glue', 'grouped', 'daily', 'monthly']
) as dag:

    employee_data = GlueJobOperator(
        task_id="employee_data",
        job_name=DAILY_GLUE_JOBS['employee_data'],
        region_name='us-east-1',
        wait_for_completion=True
    )

    timeframe_data = GlueJobOperator(
        task_id="timeframe_data",
        job_name=DAILY_GLUE_JOBS['timeframe_data'],
        region_name='us-east-1',
        wait_for_completion=True,
        trigger_rule='all_done'
    )

    leave_data = GlueJobOperator(
        task_id="leave_data",
        job_name=DAILY_GLUE_JOBS['leave_data'],
        region_name='us-east-1',
        wait_for_completion=True,
        trigger_rule='all_done'
    )

    count_by_designation = GlueJobOperator(
        task_id="count_by_designation",
        job_name=DAILY_GLUE_JOBS['count_by_designation'],
        region_name='us-east-1',
        wait_for_completion=True
    )

    threshold = GlueJobOperator(
        task_id="threshold",
        job_name=DAILY_GLUE_JOBS['threshold'],
        region_name='us-east-1',
        wait_for_completion=True
    )

    # Define strict dependencies
    employee_data >> timeframe_data
    timeframe_data >> [leave_data, count_by_designation]
    leave_data >> threshold

    # Monthly branching
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

    threshold >> check_day
    check_day >> [quota_job, skip_quota]
