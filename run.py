"""
Credit Card Churn Model - ETL Pipeline (Prefect 3)
===================================================

Usage:
    python run.py
"""

import logging
import os
import time
from datetime import datetime
from pathlib import Path

os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
os.environ["NO_PROXY"] = "127.0.0.1,localhost,::1"
 
import pandas as pd
import pandera.pandas as pa
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run

from src.config import (
    DB,
    OUTPUT_CSV,
    STAGING_DIR,
    SQL_PARAMS,
)
from src.db_manager import OracleConnectionManager
from src.etl.pipeline.etl_steps import (
    create_lagged_originals,
    create_engineered_base,
    create_engineered_lagged_and_derived,
    create_churn_targets,
)
from src.etl.pipeline.schemas import (
    RawExtractSchema,
    LaggedOriginalsSchema,
    EngineeredBaseSchema,
    FinalFeaturesSchema,
    FeaturesTargetSchema,
)
from src.etl.pipeline.validator import DataValidator

SQL_PATH = os.path.join("src", "etl", "pipeline", "sql", "base_extract.sql")
LOG_DIR = "logs"


# ---------------------------------------------------------------------------
#  File logger (persists alongside Prefect UI logs)
# ---------------------------------------------------------------------------

def _setup_file_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"pipeline_{timestamp}.log")

    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info("Log file: %s", log_file)
    return logger


# ---------------------------------------------------------------------------
#  Helper: get a run identifier for staging paths
# ---------------------------------------------------------------------------

def _get_run_id() -> str:
    try:
        run_id = flow_run.get_id()
        if run_id:
            return str(run_id)
    except Exception:
        pass
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
#  Tasks
# ---------------------------------------------------------------------------

@task(
    name="extract_from_db",
    description="Execute SQL base extract against Oracle",
    tags=["extract", "oracle"],
    retries=3,
    retry_delay_seconds=30,
)
def extract_from_db(sql_path: str, sql: bool = True) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Connecting to Oracle (%s)", DB["dsn"])
    if sql:
        manager = OracleConnectionManager(
            user=DB["user"], password=DB["password"], dsn=DB["dsn"]
        )
        conn = manager.get_connection()
        logger.info("DB connection validated: %s", manager.validate_connection(conn))

    with open(sql_path, "r") as f:
        query = f.read().strip().rstrip(";")

    start = time.perf_counter()
    
    if sql:
        cursor = conn.cursor()
        cursor.execute(query, SQL_PARAMS)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
    elapsed = time.perf_counter() - start
    if sql:
        manager.close()
    if sql:
        df = pd.DataFrame(rows, columns=columns)
    else:
        df = pd.read_csv(r"output\CHURN_2025_6M.csv")

    logger.info(
        "SQL extract complete: %d rows x %d cols (%.2fs)",
        len(df), len(df.columns), elapsed,
    )
    return df


@task(
    name="save_staging_parquet",
    description="Persist DataFrame to staging/{run_id}/{step}.parquet",
    tags=["io", "staging"],
)
def save_staging_parquet(
    df: pd.DataFrame,
    step_name: str,
    run_id: str,
) -> str:
    logger = get_run_logger()
    staging_path = Path(STAGING_DIR) / run_id
    staging_path.mkdir(parents=True, exist_ok=True)
    file_path = staging_path / f"{step_name}.parquet"

    df.to_parquet(file_path, index=False, engine="pyarrow")

    size_mb = file_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Saved staging: %s (%d rows, %.2f MB)", file_path, len(df), size_mb,
    )
    return str(file_path)


@task(
    name="validate_step",
    description="Run pandera validation and create Prefect artifact",
    tags=["validation"],
)
def validate_step(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema,
    step_name: str,
    file_logger: logging.Logger,
) -> pd.DataFrame:
    validator = DataValidator(file_logger)
    validated_df, report_md = validator.validate(df, schema, step_name)

    create_markdown_artifact(
        key=f"validation-{step_name.replace('_', '-')}",
        markdown=report_md,
        description=f"Validation report for {step_name}",
    )

    return validated_df


@task(name="create_lagged_originals", tags=["transform"])
def task_create_lagged_originals(
    df: pd.DataFrame, file_logger: logging.Logger,
) -> pd.DataFrame:
    logger = get_run_logger()
    n_cols_before = len(df.columns)
    result = create_lagged_originals(df)
    new_cols = len(result.columns) - n_cols_before
    msg = (
        "create_lagged_originals: %d rows x %d cols (+%d lag columns)"
    )
    logger.info(msg, len(result), len(result.columns), new_cols)
    file_logger.info(msg, len(result), len(result.columns), new_cols)
    return result


@task(name="create_engineered_base", tags=["transform"])
def task_create_engineered_base(
    df: pd.DataFrame, file_logger: logging.Logger,
) -> pd.DataFrame:
    logger = get_run_logger()
    n_cols_before = len(df.columns)
    result = create_engineered_base(df)
    new_cols = len(result.columns) - n_cols_before
    msg = (
        "create_engineered_base: %d rows x %d cols (+%d engineered columns)"
    )
    logger.info(msg, len(result), len(result.columns), new_cols)
    file_logger.info(msg, len(result), len(result.columns), new_cols)
    return result


@task(name="create_engineered_lagged_and_derived", tags=["transform"])
def task_create_engineered_lagged_and_derived(
    df: pd.DataFrame, file_logger: logging.Logger,
) -> pd.DataFrame:
    logger = get_run_logger()
    n_cols_before = len(df.columns)
    result = create_engineered_lagged_and_derived(df)
    new_cols = len(result.columns) - n_cols_before
    msg = (
        "create_engineered_lagged_and_derived: %d rows x %d cols "
        "(+%d derived columns)"
    )
    logger.info(msg, len(result), len(result.columns), new_cols)
    file_logger.info(msg, len(result), len(result.columns), new_cols)
    return result


@task(name="create_churn_targets", tags=["transform"])
def task_create_churn_targets(
    df: pd.DataFrame, file_logger: logging.Logger,
) -> pd.DataFrame:
    logger = get_run_logger()
    result = create_churn_targets(df)
    for h in [1, 2, 3]:
        col = f"CHURN_{h}M"
        n_churn = result[col].sum()
        msg = "%s: %d / %d (%.2f%%)"
        args = (col, n_churn, len(result), n_churn / max(len(result), 1) * 100)
        logger.info(msg, *args)
        file_logger.info(msg, *args)
    return result


@task(name="save_output_csv", tags=["io"])
def save_output_csv(df: pd.DataFrame, output_path: str) -> None:
    logger = get_run_logger()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    df.to_csv(output_path, index=False)
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info("Saved CSV: %s (%.2f MB)", output_path, size_mb)


# ---------------------------------------------------------------------------
#  Flow
# ---------------------------------------------------------------------------

def _on_flow_failure(flow, flow_run_obj, state):
    """Hook called when the flow fails. Extend with Slack / email later."""
    print(f"PIPELINE FAILED: {flow.name} | run={flow_run_obj.id} | state={state}")


@flow(
    name="churn_etl_pipeline",
    description="Credit card churn feature-engineering pipeline",
    log_prints=True,
    on_failure=[_on_flow_failure],
)
def churn_etl_pipeline() -> None:
    file_logger = _setup_file_logger()
    run_id = _get_run_id()

    file_logger.info("=" * 60)
    file_logger.info("Pipeline started  (run_id=%s)", run_id)
    file_logger.info("=" * 60)

    # Step 1: SQL base extract
    print(f"[Step 1/5] Extracting raw data from Oracle...")
    raw_df = extract_from_db(SQL_PATH, sql=False)
    raw_df = validate_step(raw_df, RawExtractSchema, "raw_extract", file_logger)
    save_staging_parquet(raw_df, "01_raw_extract", run_id)

    # Step 2: Lagged originals
    print(f"[Step 2/5] Creating lagged originals...")
    df = task_create_lagged_originals(raw_df, file_logger)
    df = validate_step(df, LaggedOriginalsSchema, "lagged_originals", file_logger)
    save_staging_parquet(df, "02_lagged_originals", run_id)

    # Step 3: Engineered base
    print(f"[Step 3/5] Creating engineered base features...")
    df = task_create_engineered_base(df, file_logger)
    df = validate_step(df, EngineeredBaseSchema, "engineered_base", file_logger)
    save_staging_parquet(df, "03_engineered_base", run_id)

    # Step 4: Engineered lagged and derived
    print(f"[Step 4/5] Creating lagged, delta and derived features...")
    df = task_create_engineered_lagged_and_derived(df, file_logger)
    df = validate_step(df, FinalFeaturesSchema, "final_features", file_logger)
    save_staging_parquet(df, "04_final_features", run_id)

    # Step 5: Churn targets
    print(f"[Step 5/5] Creating churn target flags...")
    df = task_create_churn_targets(df, file_logger)
    df = validate_step(df, FeaturesTargetSchema, "features_target", file_logger)
    save_staging_parquet(df, "05_churn_targets", run_id)

    # Final output
    save_output_csv(df, OUTPUT_CSV)

    file_logger.info("=" * 60)
    file_logger.info("Pipeline completed successfully")
    file_logger.info("=" * 60)


if __name__ == "__main__":
    churn_etl_pipeline()
