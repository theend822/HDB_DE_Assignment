"""
Data Extraction Operations
"""

import time
import requests
import pandas as pd
from pathlib import Path
from config.CONFIG_hdb_resales_price import API_BASE_URL, API_KEY, RAW_DATA_DIR

HEADERS = {"x-api-key": API_KEY} if API_KEY else {}


def fetch_and_save_from_api(name, resource_id, output_dir=RAW_DATA_DIR, limit=10000, sleep_secs=12):
    records = []
    offset = 0

    while True:
        url = f"{API_BASE_URL}?resource_id={resource_id}&limit={limit}&offset={offset}"

        data = requests.get(url, headers=HEADERS, timeout=60).json()
        batch = data["result"]["records"]
        total = data["result"].get("total", 0)

        if not batch:
            break

        records.extend(batch)
        offset += len(batch)

        if offset >= total:
            break

        time.sleep(sleep_secs)

    pd.DataFrame(records).to_csv(f"{output_dir}/{name}.csv", index=False)


def merge_raw_files(raw_dir=RAW_DATA_DIR):
    csv_files = [f for f in Path(raw_dir).glob("*.csv") if f.name != "merged_raw.csv"]

    if not csv_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    dfs = []
    for f in csv_files:
        df = pd.read_csv(f)
        df = df.drop(columns=["_id", "remaining_lease"], errors="ignore")
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True, sort=False)
    merged.to_csv(f"{raw_dir}/merged_raw.csv", index=False)
