"""
Data Quality Check Configuration for HDB Resale Prices
"""

# DQ_CHECKS: looped by DAG — each (check_type, column) becomes one task
DQ_CHECKS = {

    "null": [
        "month", "town", "flat_type", "block", "street_name",
        "storey_range", "floor_area_sqm", "flat_model",
        "lease_commence_date", "resale_price",
    ],

    "categorical": {
        "town": {"allowed_values": [
            "ANG MO KIO", "BEDOK", "BISHAN", "BUKIT BATOK", "BUKIT MERAH",
            "BUKIT PANJANG", "BUKIT TIMAH", "CENTRAL AREA", "CHOA CHU KANG",
            "CLEMENTI", "GEYLANG", "HOUGANG", "JURONG EAST", "JURONG WEST",
            "KALLANG/WHAMPOA", "LIM CHU KANG", "MARINE PARADE", "PASIR RIS",
            "QUEENSTOWN", "SEMBAWANG", "SENGKANG", "SERANGOON", "TAMPINES",
            "TOA PAYOH", "WOODLANDS", "YISHUN","PUNGGOL",
        ]},
        "flat_type": {"allowed_values": [
            "1 ROOM", "2 ROOM", "3 ROOM", "4 ROOM", "5 ROOM",
            "EXECUTIVE", "MULTI-GENERATION","MULTI GENERATION",
        ]},
        "flat_model": {"allowed_values": [
            "IMPROVED", "NEW GENERATION", "MODEL A", "STANDARD", "SIMPLIFIED",
            "MODEL A-MAISONETTE", "APARTMENT", "MAISONETTE", "TERRACE",
            "2-ROOM", "IMPROVED-MAISONETTE", "MULTI GENERATION",
            "PREMIUM APARTMENT", "Improved", "New Generation", "Model A",
            "Standard", "Apartment", "Simplified", "Model A-Maisonette",
            "Maisonette", "Multi Generation", "Adjoined flat",
            "Premium Apartment", "Terrace", "Improved-Maisonette",
            "Premium Maisonette", "2-room", "Model A2", "DBSS", "Type S1",
            "Type S2", "Premium Apartment Loft", "3Gen",
        ]},
    },

    # storey_range format: "10 TO 12" — exactly 2-digit + " TO " + 2-digit
    "string_format": {
        "storey_range": {"pattern": r"^\d{2} TO \d{2}$"},
    },

    # date_format check: tries to parse the value with the given format
    "date_format": {
        "month":               {"fmt": "%Y-%m"},
        "lease_commence_date": {"fmt": "%Y"},
    },

}

# Duplicate check
DUPLICATE_CHECK = {
    "key_columns": None,       # None = all columns except resale_price
}

# Resale price outlier check
# Within each group, flag rows where resale_price deviates > 20% from group mean
RESALE_PRICE_OUTLIER_CHECK = {
    "column": "resale_price",
    "threshold_pct": 0.20,
    "group_by": ["month", "flat_type", "block", "street_name", "storey_range", "floor_area_sqm"],
}
