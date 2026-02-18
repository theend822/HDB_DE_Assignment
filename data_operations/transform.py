"""
Data Transformation Operations
Three transformations: remaining lease, resale identifier, hash identifier
"""

import pandas as pd
import hashlib
from datetime import datetime
from dateutil.relativedelta import relativedelta
from config.CONFIG_hdb_resales_price import PROD_DATA_DIR


def calculate_remaining_lease(df, reference_date=None):
    if reference_date is None:
        reference_date = datetime.now()
    df["remaining_lease"] = df["lease_commence_date"].apply(
        lambda y: "{} years {} months".format(
            max(relativedelta(datetime(int(y), 1, 1) + relativedelta(years=99), reference_date).years, 0),
            max(relativedelta(datetime(int(y), 1, 1) + relativedelta(years=99), reference_date).months, 0),
        )
    )
    return df


def create_resale_identifier(df):
    df["resale_price"] = pd.to_numeric(df["resale_price"], errors="coerce")

    avg = df.groupby(["month", "town", "flat_type"])["resale_price"].mean().rename("avg_price")
    df = df.join(avg, on=["month", "town", "flat_type"])

    block_digits = df["block"].astype(str).str.replace(r"\D", "", regex=True).str[:3].str.zfill(3)
    price_digits = df["avg_price"].astype(int).astype(str).str[:2].str.zfill(2)
    month_digits = pd.to_datetime(df["month"], format="%Y-%m").dt.strftime("%m")
    town_char    = df["town"].str.strip().str[0].str.upper()

    df["resale_identifier"] = "S" + block_digits + price_digits + month_digits + town_char
    df = df.drop(columns=["avg_price"])
    return df


def hash_identifiers(df):
    df["resale_identifier_hash"] = df["resale_identifier"].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )
    return df


def transform_data(input_file, reference_date=None):
    df = pd.read_csv(input_file)

    df = calculate_remaining_lease(df, reference_date)
    df = create_resale_identifier(df)
    df = hash_identifiers(df)

    df.drop(columns=["resale_identifier_hash"]).to_csv(f"{PROD_DATA_DIR}/transformed.csv", index=False)
    df.drop(columns=["resale_identifier"]).to_csv(f"{PROD_DATA_DIR}/hashed.csv", index=False)
