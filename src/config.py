import os
from datetime import date

DB = {
    "user": os.environ.get("ORACLE_USER", ""),
    "password": os.environ.get("ORACLE_PASSWORD", ""),
    "dsn": os.environ.get("ORACLE_DSN", ""),
}

SQL_PARAMS = {
    "START_DATE": date(2023, 1, 1),
    "START_YYYYMM": 20230101,
    "PRODUCT_NAME": os.environ.get("PRODUCT_NAME", "TARGET_PRODUCT"),
}

OUTPUT_CSV = "output/base_features_6M.csv"
STAGING_DIR = "staging"
