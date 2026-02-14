"""
Data Validation Operations
Individual check functions that can be called from DAG
"""

import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from typing import Dict, List, Any
from config.api_config import STAGE_DATA_DIR, FAILED_DATA_DIR, RAW_DATA_DIR

logger = logging.getLogger(__name__)


def generate_profile(input_file: str, output_file: str = None) -> Dict:
    """
    Generate statistical profile for dataset

    Args:
        input_file: Path to CSV file
        output_file: Path to save profile JSON (optional)

    Returns:
        Profile dictionary
    """
    df = pd.read_csv(input_file)
    logger.info(f"Generating profile for {len(df)} records...")

    profile = {
        "total_records": len(df),
        "total_columns": len(df.columns),
        "columns": {}
    }

    for column in df.columns:
        col_data = df[column]
        col_profile = {
            "dtype": str(col_data.dtype),
            "null_count": int(col_data.isnull().sum()),
            "null_pct": float(col_data.isnull().sum() / len(col_data) * 100),
            "unique_count": int(col_data.nunique())
        }

        # Numeric columns
        if pd.api.types.is_numeric_dtype(col_data):
            col_profile.update({
                "min": float(col_data.min()) if not pd.isna(col_data.min()) else None,
                "max": float(col_data.max()) if not pd.isna(col_data.max()) else None,
                "mean": float(col_data.mean()) if not pd.isna(col_data.mean()) else None,
                "q1": float(col_data.quantile(0.25)) if not pd.isna(col_data.quantile(0.25)) else None,
                "q3": float(col_data.quantile(0.75)) if not pd.isna(col_data.quantile(0.75)) else None,
            })
        # Categorical columns
        elif col_data.nunique() <= 50:
            col_profile["unique_values"] = sorted([str(v) for v in col_data.dropna().unique()])

        profile["columns"][column] = col_profile

    # Save if output_file provided
    if output_file:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(profile, f, indent=2)
        logger.info(f"✓ Profile saved to {output_file}")

    return profile


def check_null(df: pd.DataFrame, column: str, allow_null: bool = False) -> pd.Series:
    """
    Check for null values

    Args:
        df: DataFrame to check
        column: Column name
        allow_null: Whether nulls are allowed

    Returns:
        Boolean Series (True = valid)
    """
    if allow_null:
        return pd.Series([True] * len(df))

    is_valid = df[column].notnull()
    invalid_count = (~is_valid).sum()
    logger.info(f"  Null check [{column}]: {invalid_count} nulls found")

    return is_valid


def check_categorical(df: pd.DataFrame, column: str, allowed_values: List[str]) -> pd.Series:
    """
    Check categorical values against allowed list

    Args:
        df: DataFrame to check
        column: Column name
        allowed_values: List of allowed values

    Returns:
        Boolean Series (True = valid)
    """
    is_valid = df[column].isin(allowed_values) | df[column].isnull()
    invalid_count = (~is_valid).sum()
    logger.info(f"  Categorical check [{column}]: {invalid_count} invalid values")

    return is_valid


def check_date_range(df: pd.DataFrame, column: str, min_date: str, max_date: str) -> pd.Series:
    """
    Check dates within valid range

    Args:
        df: DataFrame to check
        column: Column name
        min_date: Minimum allowed date
        max_date: Maximum allowed date

    Returns:
        Boolean Series (True = valid)
    """
    is_valid = ((df[column] >= min_date) & (df[column] <= max_date)) | df[column].isnull()
    invalid_count = (~is_valid).sum()
    logger.info(f"  Date range check [{column}]: {invalid_count} out of range")

    return is_valid


def check_duplicates(df: pd.DataFrame, key_columns: List[str] = None, strategy: str = "keep_higher_price") -> pd.Series:
    """
    Check for duplicates based on composite key

    Args:
        df: DataFrame to check
        key_columns: Columns forming composite key (None = all except resale_price)
        strategy: How to handle duplicates

    Returns:
        Boolean Series (True = keep, False = discard)
    """
    if key_columns is None:
        key_columns = [col for col in df.columns if col != 'resale_price']

    duplicates_mask = df.duplicated(subset=key_columns, keep=False)
    num_duplicates = duplicates_mask.sum()

    if num_duplicates == 0:
        logger.info(f"  Duplicate check: No duplicates found")
        return pd.Series([True] * len(df))

    logger.info(f"  Duplicate check: {num_duplicates} duplicate records found")

    if strategy == "keep_higher_price":
        # Sort by price descending, keep first
        df_sorted = df.sort_values('resale_price', ascending=False)
        is_valid = ~df_sorted.duplicated(subset=key_columns, keep='first')
        # Restore original order
        is_valid = is_valid.sort_index()
    else:
        is_valid = ~df.duplicated(subset=key_columns, keep='first')

    kept_count = is_valid.sum()
    logger.info(f"  Keeping {kept_count} records, discarding {num_duplicates - (len(df) - kept_count)} duplicates")

    return is_valid


def check_outliers(df: pd.DataFrame, column: str, method: str = "multi",
                   threshold: Dict = None, group_by: List[str] = None) -> pd.Series:
    """
    Check for outliers using statistical methods

    Args:
        df: DataFrame to check
        column: Column name
        method: Detection method ('iqr', 'z_score', 'multi')
        threshold: Threshold parameters
        group_by: Columns to group by for context-aware detection

    Returns:
        Boolean Series (True = not outlier)
    """
    if threshold is None:
        threshold = {"iqr_multiplier": 1.5, "z_score": 3}

    df_work = df.copy()
    is_outlier = pd.Series([False] * len(df))

    # Group-based detection
    if group_by:
        grouped = df_work.groupby(group_by)
    else:
        grouped = [(None, df_work)]

    for group_key, group in grouped:
        prices = group[column]
        idx = group.index

        # IQR method
        if method in ["iqr", "multi"]:
            Q1 = prices.quantile(0.25)
            Q3 = prices.quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - threshold["iqr_multiplier"] * IQR
            upper = Q3 + threshold["iqr_multiplier"] * IQR
            iqr_outliers = (prices < lower) | (prices > upper)
            is_outlier.loc[idx] |= iqr_outliers

        # Z-score method
        if method in ["z_score", "multi"] and prices.std() > 0:
            z_scores = np.abs((prices - prices.mean()) / prices.std())
            z_outliers = z_scores > threshold["z_score"]
            is_outlier.loc[idx] |= z_outliers

    outlier_count = is_outlier.sum()
    logger.info(f"  Outlier check [{column}]: {outlier_count} outliers detected ({outlier_count/len(df)*100:.2f}%)")

    return ~is_outlier


def separate_valid_failed(input_file: str, validation_results: pd.Series,
                         stage_dir: str = STAGE_DATA_DIR,
                         failed_dir: str = FAILED_DATA_DIR) -> Dict:
    """
    Separate valid and failed records based on validation results

    Args:
        input_file: Path to input CSV
        validation_results: Boolean Series (True = valid)
        stage_dir: Directory for validated data
        failed_dir: Directory for failed data

    Returns:
        Dictionary with file paths and counts
    """
    logger.info("Separating valid and failed records...")

    df = pd.read_csv(input_file)

    df_valid = df[validation_results].copy()
    df_failed = df[~validation_results].copy()
    df_failed['failure_reason'] = 'Failed validation checks'

    # Save outputs
    Path(stage_dir).mkdir(parents=True, exist_ok=True)
    Path(failed_dir).mkdir(parents=True, exist_ok=True)

    validated_path = Path(stage_dir) / "validated.csv"
    df_valid.to_csv(validated_path, index=False)
    logger.info(f"✓ Saved {len(df_valid)} valid records to {validated_path}")

    failed_path = Path(failed_dir) / "failed_records.csv"
    if not df_failed.empty:
        df_failed.to_csv(failed_path, index=False)
        logger.info(f"✓ Saved {len(df_failed)} failed records to {failed_path}")

    return {
        "status": "success",
        "validated_file": str(validated_path),
        "failed_file": str(failed_path) if not df_failed.empty else None,
        "valid_count": len(df_valid),
        "failed_count": len(df_failed),
        "pass_rate": f"{len(df_valid)/len(df)*100:.2f}%"
    }


def validate_data(input_file: str, check_name: str, column: str = None, **check_params) -> pd.Series:
    """
    Run a specific validation check
    This is called by individual DAG tasks

    Args:
        input_file: Path to CSV file
        check_name: Name of check function
        column: Column name (if applicable)
        **check_params: Additional parameters for the check

    Returns:
        Boolean Series (True = valid)
    """
    df = pd.read_csv(input_file)

    logger.info(f"Running {check_name} on {column if column else 'dataset'}...")

    if check_name == "null":
        return check_null(df, column, **check_params)
    elif check_name == "categorical":
        return check_categorical(df, column, **check_params)
    elif check_name == "date_range":
        return check_date_range(df, column, **check_params)
    elif check_name == "duplicates":
        return check_duplicates(df, **check_params)
    elif check_name == "outliers":
        return check_outliers(df, column, **check_params)
    else:
        raise ValueError(f"Unknown check: {check_name}")
