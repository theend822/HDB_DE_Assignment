"""
Data Extraction Operations
Simple, straightforward functions for API extraction and merging
"""

import requests
import pandas as pd
import logging
from pathlib import Path
from typing import Dict
from config.api_config import API_BASE_URL, API_KEY, RESOURCE_IDS, RAW_DATA_DIR

# Constants
REQUEST_TIMEOUT = 30  # seconds
RECORDS_PER_REQUEST = 100000  # max records per API call

logger = logging.getLogger(__name__)


def fetch_and_save_from_api(resource_id: str, output_dir: str = RAW_DATA_DIR) -> str:
    """
    Fetch data from API for one resource and save to CSV

    Args:
        resource_id: Resource ID to fetch
        output_dir: Directory to save raw file

    Returns:
        Path to saved file
    """
    all_records = []
    offset = 0

    logger.info(f"Fetching resource: {resource_id}")

    # Fetch all pages
    while True:
        # Build URL with resource_id
        url = f"{API_BASE_URL}?resource_id={resource_id}&limit={RECORDS_PER_REQUEST}&offset={offset}"

        if API_KEY:
            url += f"&api_key={API_KEY}"

        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                logger.error(f"API error: {data}")
                break

            records = data["result"]["records"]
            if not records:
                break

            all_records.extend(records)
            logger.info(f"  Fetched {len(records)} records (total: {len(all_records)})")

            if len(records) < RECORDS_PER_REQUEST:
                break

            offset += RECORDS_PER_REQUEST

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    # Save to CSV
    df = pd.DataFrame(all_records)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / f"raw_hdb_{resource_id}.csv"
    df.to_csv(file_path, index=False)

    logger.info(f"✓ Saved {len(df)} records to {file_path}")
    return str(file_path)


def merge_raw_files(raw_dir: str = RAW_DATA_DIR) -> pd.DataFrame:
    """
    Merge all raw CSV files into single DataFrame

    Args:
        raw_dir: Directory containing raw CSV files

    Returns:
        Merged DataFrame
    """
    logger.info("Merging raw files...")

    raw_path = Path(raw_dir)
    csv_files = list(raw_path.glob("raw_hdb_*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    dataframes = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        dataframes.append(df)
        logger.info(f"  Loaded {csv_file.name}: {len(df):,} records")

    merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
    logger.info(f"✓ Merged into {len(merged_df):,} total records")

    return merged_df


def extract_and_merge(raw_dir: str = RAW_DATA_DIR) -> Dict:
    """
    Main extraction function: fetch from API and merge
    This is called by the DAG

    Args:
        raw_dir: Directory for raw files

    Returns:
        Dictionary with status and file paths
    """
    logger.info("=" * 80)
    logger.info("EXTRACT AND MERGE")
    logger.info("=" * 80)

    # Step 1: Fetch from API
    saved_files = []
    for resource_id in RESOURCE_IDS:
        file_path = fetch_and_save_from_api(resource_id, raw_dir)
        saved_files.append(file_path)

    logger.info(f"\n✓ Fetched {len(saved_files)} resources")

    # Step 2: Merge
    merged_df = merge_raw_files(raw_dir)

    # Step 3: Save merged file
    merged_path = Path(raw_dir) / "merged_raw.csv"
    merged_df.to_csv(merged_path, index=False)
    logger.info(f"✓ Saved merged file: {merged_path}")

    return {
        "status": "success",
        "raw_files": saved_files,
        "merged_file": str(merged_path),
        "total_records": len(merged_df)
    }
