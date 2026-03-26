"""
Pandera validation schemas for the Credit Card Churn Model ETL pipeline.

Five schemas, one per pipeline stage:
  1. RawExtractSchema          - output of the SQL base-extract query
  2. LaggedOriginalsSchema     - after create_lagged_originals()
  3. EngineeredBaseSchema      - after create_engineered_base()
  4. FinalFeaturesSchema       - after create_engineered_lagged_and_derived()
  5. FeaturesTargetSchema      - after create_churn_targets()

Each schema is a strict superset of the previous one, so columns
accumulate as the DataFrame flows through the pipeline.
"""

from typing import Dict, List, Optional

import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check


# -- Helpers for repetitive lag / delta / pct-change columns ---------------

STANDARD_SHIFTS: List[int] = [1, 2, 3, 6]


def _lag_columns(
    prefix: str,
    shifts: Optional[List[int]] = None,
) -> Dict[str, Column]:
    """Generate ``PREFIX_{n}M`` lag columns for each shift."""
    shifts = shifts or STANDARD_SHIFTS
    return {
        f"{prefix}_{n}M": Column(float, nullable=True)
        for n in shifts
    }


def _delta_columns(prefix: str, shifts: List[int]) -> Dict[str, Column]:
    """Generate ``PREFIX_DELTA_{n}M`` delta columns for each shift."""
    return {
        f"{prefix}_DELTA_{n}M": Column(float, nullable=True)
        for n in shifts
    }


def _pct_chg_columns(
    prefix: str,
    shifts: Optional[List[int]] = None,
) -> Dict[str, Column]:
    """Generate ``PREFIX_PCT_CHG_{n}M`` percentage-change columns."""
    shifts = shifts or STANDARD_SHIFTS
    return {
        f"{prefix}_PCT_CHG_{n}M": Column(float, nullable=True)
        for n in shifts
    }


# =========================================================================
#  Stage 0 - Raw SQL extract
# =========================================================================

VALID_STATUSES = {"A", "E", "L", "V", "X"}
VALID_EDUCATION = {"PLC", "PUN", "LIC", "SUP", "ELM"}
VALID_ADVERTISING = {"Sunt de acord", "Nu sunt de acord"}

_identifier_cols: Dict[str, Column] = {
    "CARD_ID": Column(nullable=False),
    "MONTH_END": Column(nullable=False),
}

_card_snapshot_cols: Dict[str, Column] = {
    "BRCH_CODE": Column(str, nullable=True),
    "CARD_LIMIT": Column(
        float,
        checks=[Check.ge(0), Check.le(500_000)],
        nullable=True,
    ),
    "STATUS": Column(
        str,
        checks=Check.isin(list(VALID_STATUSES)),
        nullable=True,
    ),
    "TOTAL_EXPOSURE_AMT": Column(float, nullable=True),
    "OVERDUE_AMT_RON": Column(float, checks=Check.ge(0), nullable=True),
    "FUTURE_INST_AMT": Column(float, checks=Check.ge(0), nullable=True),
    "BONUS_AMT": Column(float, checks=Check.ge(0), nullable=True),
    "DPD": Column(
        float,
        checks=[Check.ge(0)],
        nullable=True,
    ),
    "TIMES_IN_5_DPD": Column(
        float,
        checks=[Check.ge(0), Check.le(999)],
        nullable=True,
    ),
    "TIMES_IN_30_DPD": Column(
        float,
        checks=[Check.ge(0), Check.le(999)],
        nullable=True,
    ),
    "TIMES_IN_60_DPD": Column(
        float,
        checks=[Check.ge(0), Check.le(999)],
        nullable=True,
    ),
    "TIMES_IN_90_DPD": Column(
        float,
        checks=[Check.ge(0), Check.le(999)],
        nullable=True,
    ),
    "TIMES_IN_90_PLUS_DPD": Column(
        float,
        checks=[Check.ge(0), Check.le(999)],
        nullable=True,
    ),
    "LAST_EXPOSURE_DATE": Column(nullable=True),
    "SENT_TO_LEGAL": Column(nullable=True),
    "DOD": Column(nullable=True),
    "POOL_RT": Column(nullable=True),
    "ILOE": Column(str, nullable=True),
    "DOUBTFUL_INT_AMT": Column(float, nullable=True),
    "DOUBTFUL_INT_PEN_AMT": Column(float, nullable=True),
    "DOUBTFUL_PRIN_AMT": Column(float, nullable=True),
    "DOUBTFUL_PRIN_PEN_AMT": Column(float, nullable=True),
    "FUTURE_INST_INT": Column(float, nullable=True),
    "FUTURE_INST_INT_OVRD_DBT": Column(float, nullable=True),
    "FUTURE_INST_OVRD_DBT": Column(float, nullable=True),
    "MARKETING_INFO": Column(str, nullable=True),
    "NO_OF_SUB_CARD": Column(
        float,
        checks=[Check.ge(0), Check.le(100)],
        nullable=True,
    ),
    "OSTND_INT_AMT": Column(float, nullable=True),
    "OSTND_PRIN_AMT": Column(float, nullable=True),
    "OVRD_INT_AMT": Column(float, nullable=True),
    "OVRD_INT_PEN_AMT": Column(float, nullable=True),
    "OVRD_PRIN_AMT": Column(float, nullable=True),
    "OVRD_PRIN_PEN_AMT": Column(float, nullable=True),
    "TOTAL_INT_AMT": Column(float, nullable=True),
    "TOTAL_PENALTY_AMT": Column(float, nullable=True),
    "CARD_CANCELLATION_DATE": Column(nullable=True),
}

_customer_cols: Dict[str, Column] = {
    "CNTRY": Column(str, nullable=True),
    "OPEN_DT": Column(nullable=True),
    "BIRTH_DT": Column(nullable=True),
    "CITY": Column(str, nullable=True),
    "EDUCATION": Column(
        str,
        checks=Check.isin(list(VALID_EDUCATION)),
        nullable=True,
    ),
    "RISK_LEVEL": Column(nullable=True),
    "ADVERTISING": Column(
        str,
        checks=Check.isin(list(VALID_ADVERTISING)),
        nullable=True,
    ),
}

_statement_cols: Dict[str, Column] = {
    "PRE_STMT_PYMNT_MIN_AMT": Column(float, nullable=True),
    "STMT_PYMNT_AMT": Column(float, nullable=True),
    "STMT_PYMNT_MIN_AMT": Column(float, nullable=True),
    "STMT_REVOLVING_AMT": Column(float, nullable=True),
    "FEE_AMOUNT": Column(float, nullable=True),
    "REMAINING_INSTALLMENT_AMOUNT": Column(float, nullable=True),
    "REMAINING_INSTALLMENT_COUNT": Column(float, nullable=True),
    "TOTAL_INSTALLMENT_AMOUNT": Column(float, nullable=True),
    "TOTAL_INSTALLMENT_COUNT": Column(float, nullable=True),
}

_profit_cols: Dict[str, Column] = {
    "TOTAL_PROFIT": Column(float, nullable=True),
    "MERCHANT_COMMISSION": Column(float, nullable=True),
    "INSURANCE_COMMISSION": Column(float, nullable=True),
    "FEE": Column(float, nullable=True),
    "LOAN_NET_INTEREST_INCOME": Column(float, nullable=True),
    "IMPAIRMENT_LOSSES": Column(float, nullable=True),
    "CASH_ADVANCE_COMMISSION": Column(float, nullable=True),
}

_transaction_cols: Dict[str, Column] = {
    "TRX_CNT_MTH": Column(float, checks=Check.ge(0), nullable=True),
    "AVG_TICKET_MTH": Column(float, checks=Check.ge(0), nullable=True),
    "MEDIAN_TICKET_MTH": Column(float, checks=Check.ge(0), nullable=True),
    "TRX_AMT_MTH": Column(float, checks=Check.ge(0), nullable=True),
    "MCC_MOST_FREQUENT_MTH": Column(nullable=True),
    "MCC_MOST_SPENT_MTH": Column(nullable=True),
}

_tlmk_cols: Dict[str, Column] = {
    "NUMBER_OF_ISSUES": Column(float, checks=Check.ge(0), nullable=True),
}

_raw_columns: Dict[str, Column] = {
    **_identifier_cols,
    **_card_snapshot_cols,
    **_customer_cols,
    **_statement_cols,
    **_profit_cols,
    **_transaction_cols,
    **_tlmk_cols,
}


# =========================================================================
#  Stage 1 - Original-column lags (internal only, for delta computation)
#  Lag columns are created but not exposed in schema; only delta features are.
# =========================================================================

# No _original_lag_cols in schema - lags are computed internally for deltas only


# =========================================================================
#  Stage 2 - Engineered base (created by create_engineered_base)
# =========================================================================

_engineered_base_cols: Dict[str, Column] = {
    "MONTH_YYYYMM": Column(str, nullable=False),
    "IS_ACTIVE_MTH": Column(int, checks=Check.isin([0, 1]), nullable=False),
    "ADVERTISING_CONSENT": Column(float, checks=Check.isin([0.0, 1.0]), nullable=True),
    "AGE": Column(
        "Int64",
        checks=[Check.le(110)],
        nullable=True,
    ),
    "TENURE_MONTHS": Column(
        float,
        checks=[Check.ge(0), Check.le(360)],
        nullable=True,
    ),
    "UTILIZATION_NOW": Column(float, nullable=True),
    "PCT_INSTALLMENT_REMAINING_MTH": Column(float, nullable=True),
    "HAS_ISSUE_MTH": Column(int, checks=Check.isin([0, 1]), nullable=False),
}


# =========================================================================
#  Stage 3 - Derived features (create_engineered_lagged_and_derived)
# =========================================================================

_engineered_lag_cols: Dict[str, Column] = {
    **_lag_columns("UTILIZATION_NOW"),
    **_lag_columns("TRX_AMT_MTH"),
    **_lag_columns("TRX_CNT_MTH"),
    **_lag_columns("AVG_TICKET_MTH"),
}

_all_delta_cols: Dict[str, Column] = {
    **_delta_columns("TOTAL_EXPOSURE_AMT", STANDARD_SHIFTS),
    **_delta_columns("CARD_LIMIT", STANDARD_SHIFTS),
    **_delta_columns("DPD", STANDARD_SHIFTS),
    **_delta_columns("OVERDUE_AMT_RON", STANDARD_SHIFTS),
    **_delta_columns("UTILIZATION_NOW", STANDARD_SHIFTS),
    **_delta_columns("TRX_AMT_MTH", STANDARD_SHIFTS),
    **_delta_columns("TRX_CNT_MTH", STANDARD_SHIFTS),
}

_all_pct_chg_cols: Dict[str, Column] = {
    **_pct_chg_columns("TOTAL_EXPOSURE_AMT"),
    **_pct_chg_columns("TRX_AMT_MTH"),
    **_pct_chg_columns("TRX_CNT_MTH"),
    **_pct_chg_columns("AVG_TICKET_MTH"),
    **_pct_chg_columns("UTILIZATION_NOW"),
}

_trend_cols: Dict[str, Column] = {
    "TOTAL_EXPOSURE_AMT_TRENDING_UP_MTH": Column(int, checks=Check.isin([0, 1]), nullable=False),
    "DPD_TRENDING_UP_MTH": Column(int, checks=Check.isin([0, 1]), nullable=False),
    "UTILIZATION_NOW_TRENDING_UP_MTH": Column(int, checks=Check.isin([0, 1]), nullable=False),
}

_volatility_cols: Dict[str, Column] = {
    "TOTAL_EXPOSURE_AMT_RANGE_MTH": Column(float, checks=Check.ge(0), nullable=True),
    "DPD_RANGE_MTH": Column(float, checks=Check.ge(0), nullable=True),
}

_target_cols: Dict[str, Column] = {
    "CHURN_1M": Column(int, checks=Check.isin([0, 1]), nullable=False),
    "CHURN_2M": Column(int, checks=Check.isin([0, 1]), nullable=False),
    "CHURN_3M": Column(int, checks=Check.isin([0, 1]), nullable=False),
}

_derived_columns: Dict[str, Column] = {
    **_engineered_lag_cols,
    **_all_delta_cols,
    **_all_pct_chg_cols,
    **_trend_cols,
    **_volatility_cols,
}


# =========================================================================
#  DataFrame-level checks
# =========================================================================

_no_duplicate_rows = pa.Check(
    lambda df: ~df.duplicated(subset=["CARD_ID", "MONTH_END"]).any(),
    error="Duplicate (CARD_ID, MONTH_END) rows found.",
)

_dpd_monotonicity = pa.Check(
    lambda df: (
        df["TIMES_IN_30_DPD"].fillna(0) <= df["TIMES_IN_5_DPD"].fillna(0) + 1
    ).all(),
    error="TIMES_IN_30_DPD should not exceed TIMES_IN_5_DPD.",
)

_raw_df_checks = [_no_duplicate_rows, _dpd_monotonicity]


# =========================================================================
#  Public schema objects
# =========================================================================

RawExtractSchema = DataFrameSchema(
    columns=_raw_columns,
    checks=_raw_df_checks,
    coerce=True,
    strict=False,
    name="RawExtractSchema",
)

LaggedOriginalsSchema = DataFrameSchema(
    columns=_raw_columns,
    checks=_no_duplicate_rows,
    coerce=True,
    strict=False,
    name="LaggedOriginalsSchema",
)

EngineeredBaseSchema = DataFrameSchema(
    columns={**_raw_columns, **_engineered_base_cols},
    checks=_no_duplicate_rows,
    coerce=True,
    strict=False,
    name="EngineeredBaseSchema",
)

FinalFeaturesSchema = DataFrameSchema(
    columns={
        **_raw_columns,
        **_engineered_base_cols,
        **_derived_columns,
    },
    checks=_no_duplicate_rows,
    coerce=True,
    strict=False,
    name="FinalFeaturesSchema",
)

FeaturesTargetSchema = DataFrameSchema(
    columns={
        **_raw_columns,
        **_engineered_base_cols,
        **_derived_columns,
        **_target_cols,
    },
    checks=_no_duplicate_rows,
    coerce=True,
    strict=False,
    name="FeaturesTargetSchema",
)
