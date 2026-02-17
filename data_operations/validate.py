"""
Each check reads merged_raw.csv, runs one check, and saves a 0/1 result column to dqc_results/
(1 = row failed). separate_valid_failed sums all columns per row — any row with sum > 0
failed at least one check.
"""

import pandas as pd
from pathlib import Path
from config.CONFIG_hdb_resales_price import STAGE_DATA_DIR, FAILED_DATA_DIR

DQC_RESULTS_DIR = f"{STAGE_DATA_DIR}/dqc_results"


def check_null(input_file, output_dir, column):
    df = pd.read_csv(input_file)
    result = df[column].isna().astype(int)
    result.to_csv(f"{output_dir}/null__{column}.csv", index=True, header=True)


def check_categorical(input_file, output_dir, column, allowed_values):
    df = pd.read_csv(input_file)
    result = (~df[column].isin(allowed_values)).astype(int)
    result.to_csv(f"{output_dir}/categorical__{column}.csv", index=True, header=True)


def check_string_format(input_file, output_dir, column, pattern):
    df = pd.read_csv(input_file)
    result = (~df[column].astype(str).str.match(pattern, na=False)).astype(int)
    result.to_csv(f"{output_dir}/string_format__{column}.csv", index=True, header=True)


def check_date_format(input_file, output_dir, column, fmt):
    df = pd.read_csv(input_file)
    result = pd.to_datetime(df[column].astype(str), format=fmt, errors="coerce").isna().astype(int)
    result.to_csv(f"{output_dir}/date_format__{column}.csv", index=True, header=True)


def check_duplicates(input_file, output_dir, key_columns=None):
    df = pd.read_csv(input_file)
    if key_columns is None:
        key_columns = [c for c in df.columns if c != "resale_price"]
    df_sorted = df.sort_values("resale_price", ascending=False)
    keep_mask = ~df_sorted.duplicated(subset=key_columns, keep="first")
    result = (~keep_mask.sort_index()).astype(int)
    result.to_csv(f"{output_dir}/duplicates.csv", index=True, header=True)


def check_resale_price_outlier(input_file, output_dir, column, threshold_pct, group_by):
    df = pd.read_csv(input_file)
    df[column] = pd.to_numeric(df[column], errors="coerce")
    group_mean = df.groupby(group_by)[column].transform("mean")
    result = (~df[column].between(group_mean * (1 - threshold_pct), group_mean * (1 + threshold_pct))).astype(int)
    result.to_csv(f"{output_dir}/resale_price_outlier.csv", index=True, header=True)


def separate_valid_failed(input_file, dqc_results_dir=DQC_RESULTS_DIR,
                          stage_dir=STAGE_DATA_DIR, failed_dir=FAILED_DATA_DIR):
    df = pd.read_csv(input_file)

    fail_sum = pd.Series(0, index=df.index)
    for result_file in Path(dqc_results_dir).glob("*.csv"):
        col = pd.read_csv(result_file, index_col=0).squeeze()
        fail_sum = fail_sum + col

    df_valid     = df[fail_sum == 0]
    df_non_valid = df[fail_sum > 0]

    df_valid.to_csv(f"{stage_dir}/validated.csv", index=False)
    df_non_valid.to_csv(f"{failed_dir}/non_valid_records.csv", index=False)
