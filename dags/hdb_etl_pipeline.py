"""
HDB Resale Flat Prices ETL Pipeline - Airflow DAG
Clean DAG with task definitions only
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import operations
from data_operations.extract import extract_and_merge
from data_operations.validate import generate_profile, validate_data, separate_valid_failed
from data_operations.transform import transform_data
from config.api_config import RAW_DATA_DIR, STAGE_DATA_DIR, FAILED_DATA_DIR
from config.validation_rules import DQ_CHECKS, DUPLICATE_CHECK, OUTLIER_CHECK, populate_rules_from_profile

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hdb_resale_etl',
    default_args=default_args,
    description='HDB Resale Flat Prices ETL Pipeline (2012-2016)',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['hdb', 'etl'],
    catchup=False,
)


# Task 1: Extract and merge
task_extract_merge = PythonOperator(
    task_id='extract_and_merge',
    python_callable=extract_and_merge,
    op_kwargs={
        "raw_dir": RAW_DATA_DIR,
    },
    dag=dag,
)


# Task 2: Generate profile
task_profile = PythonOperator(
    task_id='generate_profile',
    python_callable=generate_profile,
    op_kwargs={
        "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
        "output_file": f"{STAGE_DATA_DIR}/profile.json",
    },
    dag=dag,
)


# Task 3: Data Quality Checks (looped)
dq_tasks = []

for check_type, check_config in DQ_CHECKS.items():
    for column, params in check_config.items():
        task_id = f"dq_check_{check_type}_{column}"

        dq_task = PythonOperator(
            task_id=task_id,
            python_callable=validate_data,
            op_kwargs={
                "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
                "check_name": check_type,
                "column": column,
                **params
            },
            dag=dag,
        )

        dq_tasks.append(dq_task)


# Task 4: Check duplicates
task_check_duplicates = PythonOperator(
    task_id='check_duplicates',
    python_callable=validate_data,
    op_kwargs={
        "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
        "check_name": "duplicates",
        **DUPLICATE_CHECK
    },
    dag=dag,
)


# Task 5: Check outliers
task_check_outliers = PythonOperator(
    task_id='check_outliers',
    python_callable=validate_data,
    op_kwargs={
        "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
        "check_name": "outliers",
        **OUTLIER_CHECK
    },
    dag=dag,
)


# Task 6: Separate valid/failed records
task_separate_valid_failed = PythonOperator(
    task_id='separate_valid_failed',
    python_callable=separate_valid_failed,
    op_kwargs={
        "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
        "validation_results": None,  # Will be combined from previous checks
        "stage_dir": STAGE_DATA_DIR,
        "failed_dir": FAILED_DATA_DIR,
    },
    dag=dag,
)


# Task 7: Transform data
task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        "input_file": f"{STAGE_DATA_DIR}/validated.csv",
        "reference_date": None,
    },
    dag=dag,
)


# Task dependencies
task_extract_merge >> task_profile
task_profile >> dq_tasks
task_profile >> [task_check_duplicates, task_check_outliers]

# All DQ checks must complete before separating valid/failed
dq_tasks >> task_separate_valid_failed
task_check_duplicates >> task_separate_valid_failed
task_check_outliers >> task_separate_valid_failed

# Transform after separation
task_separate_valid_failed >> task_transform
