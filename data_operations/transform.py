"""
Data Transformation Operations
Three main transformations: remaining lease, create identifier, hash identifier
"""

import pandas as pd
import hashlib
import re
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import Dict
from config.api_config import STAGE_DATA_DIR, PROD_DATA_DIR

logger = logging.getLogger(__name__)


def calculate_remaining_lease(df: pd.DataFrame, reference_date: datetime = None) -> pd.DataFrame:
    """
    Calculate remaining lease (99-year assumption, rounded down)

    Args:
        df: DataFrame with lease_commence_date column
        reference_date: Reference date (defaults to today)

    Returns:
        DataFrame with remaining_lease column added
    """
    if reference_date is None:
        reference_date = datetime.now()

    def calc_lease(lease_year):
        lease_start = datetime(year=int(lease_year), month=1, day=1)
        lease_end = lease_start + relativedelta(years=99)
        remaining = relativedelta(lease_end, reference_date)

        years = remaining.years
        months = remaining.months

        if years < 0:
            return "0 years 0 months"

        return f"{years} years {months} months"

    df['remaining_lease'] = df['lease_commence_date'].apply(calc_lease)
    logger.info(f"✓ Calculated remaining lease for {len(df)} records")

    return df


def create_resale_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create Resale Identifier: S + [3-digit block] + [2-digit avg price] + [2-digit month] + [1-char town]

    All digit extraction logic is embedded here

    Args:
        df: DataFrame with block, month, town, flat_type, resale_price columns

    Returns:
        DataFrame with resale_identifier column added
    """
    # Calculate average prices by group
    avg_prices = df.groupby(['month', 'town', 'flat_type'])['resale_price'].mean().reset_index()
    avg_prices.columns = ['month', 'town', 'flat_type', 'avg_resale_price']
    df = df.merge(avg_prices, on=['month', 'town', 'flat_type'], how='left')

    logger.info(f"Calculated {len(avg_prices)} average price groups")

    def build_identifier(row):
        # Extract 3-digit block (remove non-digits, pad left with zeros)
        block_digits = re.sub(r'\D', '', str(row['block']))
        if not block_digits:
            block_digits = "000"
        else:
            block_digits = block_digits[:3].zfill(3)

        # Extract 2-digit price (1st and 2nd digit of average)
        price_str = str(int(row['avg_resale_price']))
        price_digits = price_str[:2].zfill(2) if len(price_str) >= 2 else price_str.zfill(2)

        # Extract 2-digit month (MM from YYYY-MM)
        try:
            month_digits = datetime.strptime(row['month'], "%Y-%m").strftime("%m")
        except:
            month_digits = "00"

        # Extract 1st character of town
        town_char = str(row['town']).strip().upper()[0] if row['town'] else "X"

        return f"S{block_digits}{price_digits}{month_digits}{town_char}"

    df['resale_identifier'] = df.apply(build_identifier, axis=1)
    df = df.drop('avg_resale_price', axis=1)

    unique_count = df['resale_identifier'].nunique()
    logger.info(f"✓ Created {unique_count} unique identifiers")

    return df


def hash_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hash resale identifiers using SHA-256

    Args:
        df: DataFrame with resale_identifier column

    Returns:
        DataFrame with resale_identifier_hash column added
    """
    def hash_sha256(identifier):
        return hashlib.sha256(identifier.encode('utf-8')).hexdigest()

    df['resale_identifier_hash'] = df['resale_identifier'].apply(hash_sha256)

    unique_hashes = df['resale_identifier_hash'].nunique()
    logger.info(f"✓ Hashed {unique_hashes} unique identifiers")

    if unique_hashes == len(df):
        logger.info("  Hash uniqueness verified!")

    return df


def transform_data(input_file: str, reference_date: datetime = None) -> Dict:
    """
    Main transformation function - applies all 3 transformations
    This is called by the DAG

    Args:
        input_file: Path to validated CSV file
        reference_date: Reference date for remaining lease

    Returns:
        Dictionary with status and output file paths
    """
    logger.info("=" * 80)
    logger.info("TRANSFORM DATA")
    logger.info("=" * 80)

    # Load data
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} records from {input_file}")

    # Apply transformations
    df = calculate_remaining_lease(df, reference_date)
    df = create_resale_identifier(df)
    df = hash_identifiers(df)

    # Save outputs
    prod_dir = Path(PROD_DATA_DIR)
    prod_dir.mkdir(parents=True, exist_ok=True)

    # Transformed data (with plain identifier)
    transformed_path = prod_dir / "transformed.csv"
    df_transformed = df.drop('resale_identifier_hash', axis=1)
    df_transformed.to_csv(transformed_path, index=False)
    logger.info(f"✓ Saved transformed data: {transformed_path}")

    # Hashed data (with hash only, no plain identifier)
    hashed_path = prod_dir / "hashed.csv"
    df_hashed = df.drop('resale_identifier', axis=1)
    df_hashed.to_csv(hashed_path, index=False)
    logger.info(f"✓ Saved hashed data: {hashed_path}")

    return {
        "status": "success",
        "transformed_file": str(transformed_path),
        "hashed_file": str(hashed_path),
        "total_records": len(df)
    }
