"""
Staged ETL pipeline for base_extract.

Materialises each CTE into an intermediary Oracle table (TMP_BE_*),
then runs a lightweight final join.  This avoids holding millions of
rows in memory across nested CTEs and lets Oracle optimise each step
independently.

Usage:
    python -m src.etl.run_base_extract
"""

from datetime import date
from pathlib import Path
import logging
import time

import pandas as pd

from src.config import DB
from src.db_manager import OracleConnectionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent / "pipeline" / "sql" / "base_extract"

SQL_PARAMS = {
    "START_DATE": date(2021, 1, 1),
    "START_YYYYMM": 202101,
}

INTERMEDIARY_STEPS: list[tuple[str, str]] = [
    ("TMP_BE_CARD_SNAPSHOT",    "01_card_snapshot.sql"),
    ("TMP_BE_STMT_AGG",         "02_stmt_agg.sql"),
    ("TMP_BE_STMT_DETAIL_AGG",  "03_stmt_detail_agg.sql"),
    ("TMP_BE_STMT_MONTHLY",     "04_stmt_monthly.sql"),
    ("TMP_BE_PROFIT_MONTHLY",   "05_profit_monthly.sql"),
    ("TMP_BE_TLMK_MONTHLY",     "06_tlmk_monthly.sql"),
    ("TMP_BE_TRX_MONTHLY",      "07_trx_monthly.sql"),
    ("TMP_BE_MCC_FREQ_RANKED",  "08_mcc_freq_ranked.sql"),
    ("TMP_BE_MCC_SPENT_RANKED", "09_mcc_spent_ranked.sql"),
]

FINAL_SQL_FILE = "10_final_join.sql"


def _drop_table_if_exists(cursor, table_name: str) -> None:
    cursor.execute(
        """
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE """ + table_name + """ PURGE';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN RAISE; END IF;
        END;
        """
    )


def _read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text().strip().rstrip(";")


def run_base_extract(cleanup: bool = True) -> pd.DataFrame:
    """
    Execute the staged base_extract pipeline against Oracle.

    1. Creates one intermediary table per original CTE (NOLOGGING, PARALLEL).
    2. Runs the final lightweight join across those tables.
    3. Optionally drops the intermediary tables on completion.

    Parameters
    ----------
    cleanup : bool
        If True (default), drop all TMP_BE_* tables after fetching results.
        Set to False to keep them for debugging / inspection.
    """
    manager = OracleConnectionManager(
        user=DB["user"],
        password=DB["password"],
        dsn=DB["dsn"],
    )

    with manager.connection() as conn:
        cursor = conn.cursor()

        for table_name, sql_file in INTERMEDIARY_STEPS:
            _drop_table_if_exists(cursor, table_name)

            log.info("Creating %s  (%s) ...", table_name, sql_file)
            t0 = time.perf_counter()
            cursor.execute(_read_sql(sql_file), SQL_PARAMS)
            conn.commit()
            elapsed = time.perf_counter() - t0
            log.info("  -> %s created in %.1fs", table_name, elapsed)

        log.info("Running final join ...")
        t0 = time.perf_counter()
        cursor.execute(_read_sql(FINAL_SQL_FILE))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        elapsed = time.perf_counter() - t0
        log.info("  -> Final join returned %d rows in %.1fs", len(rows), elapsed)

        if cleanup:
            log.info("Cleaning up intermediary tables ...")
            for table_name, _ in reversed(INTERMEDIARY_STEPS):
                _drop_table_if_exists(cursor, table_name)
            conn.commit()
            log.info("  -> Cleanup complete")

        cursor.close()

    manager.close()
    return pd.DataFrame(rows, columns=columns)


if __name__ == "__main__":
    df = run_base_extract()
    print(f"Extracted {len(df)} rows x {len(df.columns)} columns")
    print(df.head())
