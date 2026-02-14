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
DATASET_ID = [
    "d_ebc5ab87086db484f88045b47411ebc5", # 1990 - 1999
    "d_43f493c6c50d54243cc1eab0df142d6a",  # 2000 - Feb 2012
    "d_ea9ed51da2787afaf8e51f827c304208",  # Jan 2015 to Dec 2016
    "d_2d5ff9ea31397b66239f245f57751537",  # Mar 2012 to Dec 2014
    "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",  # Jan-2017 onwards
]

# File Paths
RAW_DATA_DIR = "data/raw"
STAGE_DATA_DIR = "data/stage"
PROD_DATA_DIR = "data/prod"
FAILED_DATA_DIR = "data/failed"
