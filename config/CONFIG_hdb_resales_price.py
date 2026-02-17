"""
Config for HDB Resale Prices
"""

import os
from dotenv import load_dotenv
load_dotenv()

API_BASE_URL = "https://data.gov.sg/api/action/datastore_search"
API_KEY = os.getenv("DATA_GOV_SG_API_KEY", None)

# Resource IDs for HDB Resale Flat Prices
DATASET_ID = {
    "1990_1999":    "d_ebc5ab87086db484f88045b47411ebc5",
    "2000_2012feb": "d_43f493c6c50d54243cc1eab0df142d6a",
    "2012mar_2014": "d_2d5ff9ea31397b66239f245f57751537",
    "2015_2016":    "d_ea9ed51da2787afaf8e51f827c304208",
    "2017_onwards": "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
}

# File Paths
RAW_DATA_DIR = "data/raw"
STAGE_DATA_DIR = "data/stage"
PROD_DATA_DIR = "data/prod"
FAILED_DATA_DIR = "data/failed"
