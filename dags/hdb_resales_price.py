"""
HDB Resale Prices ETL Pipeline
"""

from datetime import timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_operations.extract   import fetch_and_save_from_api, merge_raw_files
from data_operations.validate  import (
    check_null, check_categorical, check_string_format, check_date_format,
    check_duplicates, check_resale_price_outlier, separate_valid_failed,
)
from data_operations.transform import transform_data
from config.CONFIG_hdb_resales_price import DATASET_ID, RAW_DATA_DIR, STAGE_DATA_DIR, FAILED_DATA_DIR
from config.DQC_hdb_resales_price    import DQ_CHECKS, DUPLICATE_CHECK, RESALE_PRICE_OUTLIER_CHECK

DQC_RESULTS_DIR = f"{STAGE_DATA_DIR}/dqc_results"

CHECK_FUNCTIONS = {
    "null":          check_null,
    "categorical":   check_categorical,
    "string_format": check_string_format,
    "date_format":   check_date_format,
}

default_args = {
    "owner": "jxy",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "hdb_resales_price",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
)


# Block 1: Extract

download_tasks = []
for name, resource_id in DATASET_ID.items():
    download_tasks.append(PythonOperator(
        task_id=f"download_{name}",
        python_callable=fetch_and_save_from_api,
        op_kwargs={"name": name, "resource_id": resource_id, "output_dir": RAW_DATA_DIR},
        dag=dag,
    ))

merge_task = PythonOperator(
    task_id="merge",
    python_callable=merge_raw_files,
    op_kwargs={"raw_dir": RAW_DATA_DIR},
    dag=dag,
)


# Block 2: DQC

dqc_tasks = []
for check_type, check_config in DQ_CHECKS.items():
    items = [(col, {}) for col in check_config] if isinstance(check_config, list) else check_config.items()
    for column, params in items:
        dqc_tasks.append(PythonOperator(
            task_id=f"{check_type}__{column}",
            python_callable=CHECK_FUNCTIONS[check_type],
            op_kwargs={
                "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
                "output_dir": DQC_RESULTS_DIR,
                "column":     column,
                **params,
            },
            dag=dag,
        ))

check_duplicates_task = PythonOperator(
    task_id="check_duplicates",
    python_callable=check_duplicates,
    op_kwargs={
        "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
        "output_dir": DQC_RESULTS_DIR,
        **DUPLICATE_CHECK,
    },
    dag=dag,
)

check_outlier_task = PythonOperator(
    task_id="check_price_outlier",
    python_callable=check_resale_price_outlier,
    op_kwargs={
        "input_file": f"{RAW_DATA_DIR}/merged_raw.csv",
        "output_dir": DQC_RESULTS_DIR,
        **RESALE_PRICE_OUTLIER_CHECK,
    },
    dag=dag,
)


# Block 3: Separate and transform

separate_task = PythonOperator(
    task_id="separate_valid_failed",
    python_callable=separate_valid_failed,
    op_kwargs={
        "input_file":      f"{RAW_DATA_DIR}/merged_raw.csv",
        "dqc_results_dir": DQC_RESULTS_DIR,
        "stage_dir":       STAGE_DATA_DIR,
        "failed_dir":      FAILED_DATA_DIR,
    },
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    op_kwargs={
        "input_file":     f"{STAGE_DATA_DIR}/validated.csv",
        "reference_date": None,
    },
    dag=dag,
)


# Dependencies

all_dqc = dqc_tasks + [check_duplicates_task, check_outlier_task]

download_tasks >> merge_task >> all_dqc >> separate_task >> transform_task
