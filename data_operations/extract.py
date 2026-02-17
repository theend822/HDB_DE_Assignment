"""
Data Extraction Operations
"""

import requests
import pandas as pd
from pathlib import Path
from config.CONFIG_hdb_resales_price import API_BASE_URL, API_KEY, RAW_DATA_DIR


def fetch_and_save_from_api(name: str, resource_id: str, output_dir: str = RAW_DATA_DIR):
    url = f"{API_BASE_URL}?resource_id={resource_id}"
    if API_KEY:
        url += f"&api_key={API_KEY}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    records = response.json()["result"]["records"]

    pd.DataFrame(records).to_csv(f"{output_dir}/{name}.csv", index=False)


def merge_raw_files(raw_dir: str = RAW_DATA_DIR):
    csv_files = [f for f in Path(raw_dir).glob("*.csv") if f.name != "merged_raw.csv"]

    if not csv_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    merged = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True, sort=False)
    merged.to_csv(f"{raw_dir}/merged_raw.csv", index=False)
