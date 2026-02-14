"""
API Configuration for data.gov.sg
Contains API endpoints and resource IDs
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_BASE_URL = "https://data.gov.sg/api/action/datastore_search"

# API Key (optional - data.gov.sg public API doesn't require key for read access)
API_KEY = os.getenv("DATA_GOV_SG_API_KEY", None)

# Resource IDs for HDB Resale Flat Prices (Jan 2012 - Dec 2016)
RESOURCE_IDS = [
    "1b702208-44bf-4829-b620-4615ee19b57c",  # 2012-2014
    "83b2fc37-ce8c-4df4-968b-370fd818138b",  # 2015-2016
]

# File Paths
RAW_DATA_DIR = "data/raw"
STAGE_DATA_DIR = "data/stage"
PROD_DATA_DIR = "data/prod"
FAILED_DATA_DIR = "data/failed"
