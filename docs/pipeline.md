# Credit Card Churn Model - ETL Pipeline

## Overview

The pipeline extracts card-level monthly snapshots from Oracle, engineers
lag / delta / trend features, winsorises outliers, validates the data with
**pandera**, and writes the final feature set to CSV and Parquet.

Orchestrated by **Prefect 3** with per-step logging, validation artifacts,
and failure notifications.

```
run.py  (churn_etl_pipeline flow)
  |
  +--> extract_from_db          -->  RawExtractSchema validation
  |                             -->  staging/01_raw_extract.parquet
  |
  +--> create_lagged_originals  -->  LaggedOriginalsSchema validation
  |
  +--> create_engineered_base   -->  EngineeredBaseSchema validation
  |
  +--> create_engineered_lagged_and_derived
  |                             -->  FinalFeaturesSchema validation
  |
  +--> winsorise_features       -->  Winsorisation artifact
  |
  +--> create_churn_targets     -->  FeaturesTargetSchema validation
  |                             -->  staging/06_final_features.parquet
  |
  +--> Save CSV
```

## Project Structure

```
ML-Preparation/
├── run.py                            # Prefect 3 flow + tasks
├── requirements.txt
├── src/
│   ├── config.py                     # DB creds (env vars), winsorisation, paths
│   ├── db_manager.py                 # Oracle connection pool
│   └── etl/
│       └── pipeline/
│           ├── etl_steps.py          # Feature engineering + winsorisation
│           ├── schemas.py            # Pandera schemas (one per stage)
│           ├── validator.py          # DataValidator (logging + Prefect artifacts)
│           └── sql/
│               ├── base_extract.sql  # Production extraction query
│               └── sample_extract.sql
├── staging/                          # Parquet snapshots per run
│   └── {run_id}/
│       ├── 01_raw_extract.parquet
│       └── 06_final_features.parquet
├── logs/                             # File logs per run
├── output/
│   └── base_features.csv
└── docs/
    ├── pipeline.md                   # This file
    └── validation-guide.md
```

## Pipeline Steps

### Step 1 - SQL Base Extract

Prefect task `extract_from_db` with **3 retries** (30s delay). Executes
the SQL query against Oracle. Output is validated against `RawExtractSchema`
and saved to `staging/{run_id}/01_raw_extract.parquet`.

### Step 2 - create_lagged_originals

Shifts five raw columns backward by 1, 2, 3, and 6 months per card:


| Source Column      | Lag Column Pattern            |
| ------------------ | ----------------------------- |
| TOTAL_EXPOSURE_AMT | TOTAL_EXPOSURE_AMT_{1,2,3,6}M |
| CARD_LIMIT         | CARD_LIMIT_{1,2,3,6}M         |
| DPD                | DPD_{1,2,3,6}M                |
| OVERDUE_AMT_RON    | OVERDUE_AMT_RON_{1,2,3,6}M    |
| UNUTILIZED_AMT     | UNUTILIZED_AMT_{1,2,3,6}M     |


Validated against `LaggedOriginalsSchema`.

### Step 3 - create_engineered_base

Computes point-in-time features: `IS_ACTIVE_MTH`, `AGE`, `TENURE_MONTHS`,
`UTILIZATION_NOW`, statement / transaction / issue aggregates.

Validated against `EngineeredBaseSchema`.

### Step 4 - create_engineered_lagged_and_derived

Lags engineered features, computes deltas, percentage changes, trend flags,
and volatility ranges.

Validated against `FinalFeaturesSchema`.

### Step 5 - Winsorisation

Caps all continuous numeric columns at the 1st and 99th percentiles
(configurable in `src/config.py`). Binary flags, IDs, categoricals, and
target columns are excluded automatically.

A Prefect Markdown artifact is created listing which columns were clipped
and how many values were affected.

### Step 6 - create_churn_targets

Creates binary churn target flags (`CHURN_1M`, `CHURN_2M`, `CHURN_3M`)
based on `CARD_CANCELLATION_DATE`.

Validated against `FeaturesTargetSchema`.

### Step 7 - Save Outputs

Saves final DataFrame to both:

- `staging/{run_id}/06_final_features.parquet`
- `output/base_features.csv`

## Staging

Parquet snapshots are saved under `staging/{run_id}/` at two pipeline
checkpoints. The `run_id` is the Prefect flow run ID (or a timestamp
fallback when running outside Prefect).

## Validation

Handled by the `DataValidator` class in `src/etl/pipeline/validator.py`.

**Behaviour:**

1. Logs shape and null statistics to both file logger and Prefect.
2. Runs `schema.validate(df, lazy=True)` to collect all errors.
3. Creates a **Prefect Markdown artifact** with the validation report.
4. If validation **passes**: returns the (possibly coerced) DataFrame.
5. If validation **fails**: logs each error as a WARNInNG ad returns the
  original DataFrame so the pipeline can continue.

Schemas are in `src/etl/pipeline/schemas.py` with domain-specific checks
(value ranges, cross-column relationships). See `docs/validation-guide.md`.

## Prefect Integration


| Feature      | Details                                                        |
| ------------ | -------------------------------------------------------------- |
| Flow         | `churn_etl_pipeline` with `log_prints=True`                    |
| Tasks        | One per pipeline step with descriptive names and tags          |
| Retries      | DB extraction: 3 retries, 30s delay                            |
| Artifacts    | Markdown validation reports + winsorisation report             |
| Failure hook | `_on_flow_failure` prints to console (extend for Slack/email)  |
| Logger       | Prefect `get_run_logger()` + file logger for local persistence |


## Logging

Every run creates a log file under `logs/`:

```
logs/pipeline_20260310_141500.log
```

Prefect tasks also emit structured logs visible in the Prefect UI.

## Configuration

All tuneable parameters live in `src/config.py`:


| Variable                  | Purpose                                                                           |
| ------------------------- | --------------------------------------------------------------------------------- |
| `DB`                      | Oracle credentials (from env vars `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`) |
| `SQL_PARAMS`              | Bind variables for `base_extract.sql`                                             |
| `OUTPUT_CSV`              | Path for the final CSV                                                            |
| `STAGING_DIR`             | Root directory for parquet staging                                                |
| `WINSOR_LOWER_PERCENTILE` | Lower percentile for winsorisation (default 0.01)                                 |
| `WINSOR_UPPER_PERCENTILE` | Upper percentile for winsorisation (default 0.99)                                 |


### Environment Variables

Set before running the pipeline:

```bash
export ORACLE_USER=your_oracle_user
export ORACLE_PASSWORD=your_password_here
export ORACLE_DSN=host:1521/service_name
```

