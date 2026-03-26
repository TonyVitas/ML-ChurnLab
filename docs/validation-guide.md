# Validation Guide

This document explains how to add, modify, or extend pandera validation
rules in the ETL pipeline.

## File Locations

| File | Role |
|------|------|
| `src/etl/pipeline/schemas.py`   | Schema definitions (column types + checks) |
| `src/etl/pipeline/validator.py` | `DataValidator` class (runs schemas, logs results, creates Prefect artifacts) |
| `run.py`                        | Wires schemas to pipeline steps via Prefect tasks |

## Schema Architecture

There are five schemas, one per pipeline stage. Each is a **superset** of
the previous one:

```
RawExtractSchema
  + _original_lag_cols       = LaggedOriginalsSchema
  + _engineered_base_cols    = EngineeredBaseSchema
  + _derived_columns         = FinalFeaturesSchema
  + _target_cols             = FeaturesTargetSchema
```

Column definitions are grouped into private dictionaries
(`_card_snapshot_cols`, `_customer_cols`, etc.) and merged into each schema.

## Current Validation Rules

### Column-Level Checks

| Column | Type | Checks |
|--------|------|--------|
| CARD_ID | any | not null |
| MONTH_END | any | not null |
| CARD_LIMIT | float | >= 0, <= 500,000 |
| STATUS | str | in {A, E, L, V, X} |
| DPD | float | >= 0, <= 365 |
| TIMES_IN_*_DPD | float | >= 0, <= 999 |
| EDUCATION | str | in {PLC, PUN, LIC, SUP, ELM} |
| CLIENT_TP | str | in {F} |
| ADVERTISING | str | in {Sunt de acord, Nu sunt de acord} |
| NO_OF_SUB_CARD | float | >= 0, <= 10 |
| CHURN_*M | int | in {0, 1} |
| IS_ACTIVE_MTH | int | in {0, 1} |
| AGE | Int64 | >= 18, <= 110 |
| TENURE_MONTHS | float | >= 0, <= 360 |
| Trend flags | int | in {0, 1} |
| Volatility ranges | float | >= 0 |

### DataFrame-Level Checks

| Check | Applied To |
|-------|-----------|
| No duplicate (CARD_ID, MONTH_END) | All schemas |
| UNUTILIZED_AMT <= CARD_LIMIT * 1.05 | RawExtractSchema |
| TIMES_IN_30_DPD <= TIMES_IN_5_DPD + 1 | RawExtractSchema |

## How to Add a Check to an Existing Column

Open `src/etl/pipeline/schemas.py`. Find the column and add a `checks`
list.

**Before (skeleton):**

```python
"CARD_LIMIT": Column(float, nullable=True),
```

**After (with a check):**

```python
from pandera import Check

"CARD_LIMIT": Column(float, checks=Check.ge(0), nullable=True),
```

Multiple checks can be passed as a list:

```python
"DPD": Column(float, checks=[Check.ge(0), Check.le(365)], nullable=True),
```

## Common Check Reference

| Check | Meaning | Example |
|-------|---------|---------|
| `Check.ge(0)` | >= 0 | Non-negative amounts |
| `Check.le(100)` | <= 100 | Percentages |
| `Check.gt(0)` | > 0 | Strictly positive |
| `Check.isin([0, 1])` | Value in set | Binary flags |
| `Check.str_matches(r"^\d{4}")` | Regex match | Date format |
| `Check.in_range(0, 1)` | 0 <= x <= 1 | Ratios |
| `Check(lambda s: s.mean() > 0)` | Custom series check | Distribution |

Full reference: https://pandera.readthedocs.io/en/stable/checks.html

## How to Add a New Column to a Schema

1. Decide which **stage** the column first appears in.
2. Add it to the corresponding private dictionary.

Example -- adding a new column that appears after `create_engineered_base`:

```python
# In _engineered_base_cols
"MY_NEW_FEATURE": Column(float, nullable=True),
```

Because `EngineeredBaseSchema` and `FinalFeaturesSchema` both include
`_engineered_base_cols`, the column is automatically expected in those two
stages.

## How to Add Lag / Delta / Pct-Change Columns

Use the helper functions instead of writing each column by hand:

```python
# 4 lag columns: NEWFEATURE_1M, NEWFEATURE_2M, NEWFEATURE_3M, NEWFEATURE_6M
_some_dict = {
    **_lag_columns("NEWFEATURE"),
}

# Delta columns for specific shifts only
_some_dict = {
    **_delta_columns("NEWFEATURE", [1, 3, 6]),
}

# Pct-change columns (now defaults to shifts [1, 2, 3, 6])
_some_dict = {
    **_pct_chg_columns("NEWFEATURE"),
}
```

## How Validation Works with Prefect

Each validation step is a Prefect task (`validate_step`) that:

1. Runs the pandera schema validation.
2. Creates a **Prefect Markdown artifact** with the validation report
   (shape, null stats, pass/fail, error details).
3. Logs results to both the Prefect UI and the local file logger.

The validation report artifacts are visible in the Prefect UI under the
flow run's Artifacts tab.

## How Validation Failures are Handled

The pipeline does **not** crash on validation errors. Instead:

1. All errors are collected at once (`lazy=True`).
2. Each error is logged as a `WARNING` with the column name, check name,
   and the failing value.
3. A Prefect Markdown artifact is created with the error table.
4. The original DataFrame is returned and the pipeline continues.

Check the Prefect UI artifacts or log files under `logs/` for details.

## Adding a DataFrame-Level Check

DataFrame-level checks validate relationships **across columns**. Example:

```python
_my_check = pa.Check(
    lambda df: (df["COL_A"] <= df["COL_B"]).all(),
    error="COL_A must not exceed COL_B.",
)

SomeSchema = DataFrameSchema(
    columns={...},
    checks=[_no_duplicate_rows, _my_check],
    coerce=True,
    strict=False,
)
```
