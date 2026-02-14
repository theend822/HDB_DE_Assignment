"""
Data Validation Rules Configuration
Defines which checks to apply to which columns
DAG will loop over these to create tasks
"""

# Data Quality Checks Configuration
# Format: {"check_type": {"column_name": {params}}}
DQ_CHECKS = {
    "null": {
        "resale_price": {"allow_null": False},
        "town": {"allow_null": False},
        "flat_type": {"allow_null": False},
    },

    "categorical": {
        "town": {"allowed_values": []},  # Populated from profile
        "flat_type": {"allowed_values": []},
        "flat_model": {"allowed_values": []},
        "storey_range": {"allowed_values": []},
    },

    "date_range": {
        "month": {"min_date": None, "max_date": None},  # Populated from profile
    },
}

# Separate checks (not looped, individual tasks)
DUPLICATE_CHECK = {
    "key_columns": None,  # None = all columns except resale_price
    "strategy": "keep_higher_price"
}

OUTLIER_CHECK = {
    "column": "resale_price",
    "method": "multi",  # IQR + Z-score
    "threshold": {
        "iqr_multiplier": 1.5,
        "z_score": 3
    },
    "group_by": ["flat_type", "town"]
}


def populate_rules_from_profile(profile: dict) -> dict:
    """
    Populate dynamic validation rules from data profile

    Args:
        profile: Data profile dictionary

    Returns:
        Updated DQ_CHECKS dictionary
    """
    updated_checks = DQ_CHECKS.copy()

    # Populate categorical allowed values
    for column in updated_checks.get("categorical", {}).keys():
        if column in profile["columns"]:
            unique_values = profile["columns"][column].get("unique_values", [])
            updated_checks["categorical"][column]["allowed_values"] = unique_values

    # Populate date ranges
    for column in updated_checks.get("date_range", {}).keys():
        if column in profile["columns"]:
            updated_checks["date_range"][column]["min_date"] = profile["columns"][column].get("min")
            updated_checks["date_range"][column]["max_date"] = profile["columns"][column].get("max")

    return updated_checks
