"""
Credit Card Churn Model - Feature Engineering Steps
====================================================

Pipeline: raw_df --> create_lagged_originals --> create_engineered_base
  --> create_engineered_lagged_and_derived --> winsorise_features
  --> create_churn_targets --> output_df

The input raw_df is assumed to be the output of the SQL base extract query,
with 1 row per CARD_ID per MONTH_END, sorted by (CARD_ID, MONTH_END).

Five atomic transformation functions:
  1. create_lagged_originals               : Original -> Original (lagged)
  2. create_engineered_base                : Original -> Engineered (point-in-time)
  3. create_engineered_lagged_and_derived  : (all prior) -> Engineered (lagged) + Deltas + PctChg + Trends + Volatility
  4. winsorise_features                    : Cap numeric features at percentile bounds
  5. create_churn_targets                  : Adds CHURN_1M, CHURN_2M, CHURN_3M binary target flags
"""

import logging
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def _safe_pct_change(current: pd.Series, lagged: pd.Series) -> pd.Series:
    """Percentage change: (current - lagged) / lagged * 100, safe for zeros."""
    return ((current - lagged) / lagged.replace(0, np.nan)) * 100
 
 
def _month_shift(
    df: pd.DataFrame,
    col: str,
    shifts: List[int],
    prefix: str,
) -> pd.DataFrame:
    """
    For each shift n in *shifts*, create column ``{prefix}_{n}M`` by shifting
    *col* n periods backward within each CARD_ID group.
 
    Assumes df is sorted by (CARD_ID, MONTH_END) and has one row per card
    per calendar month (no gaps).
    """
    out = df.copy()
    for n in shifts:
        out[f"{prefix}_{n}M"] = (
            out.groupby("CARD_ID")[col]
            .shift(n)
        )
    return out
 
 
# =========================================================================
# 1. Original (lagged)
# =========================================================================
 
def create_lagged_originals(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Shift raw Original columns backward by 1, 2, 3 and 6 months per card.
    These lags are used internally for delta computation only; lag columns
    are dropped later and only delta features are exposed.

    Internal lag columns (dropped before output):
        TOTAL_EXPOSURE_AMT_{1,2,3,6}M, CARD_LIMIT_{1,2,3,6}M,
        DPD_{1,2,3,6}M, OVERDUE_AMT_RON_{1,2,3,6}M
    """
    df = raw_df.copy()
    df = df.sort_values(["CARD_ID", "MONTH_END"]).reset_index(drop=True)

    lag_specs: List[Tuple[str, str]] = [
        ("TOTAL_EXPOSURE_AMT", "TOTAL_EXPOSURE_AMT"),
        ("CARD_LIMIT", "CARD_LIMIT"),
        ("DPD", "DPD"),
        ("OVERDUE_AMT_RON", "OVERDUE_AMT_RON"),
    ]
    shifts = [1, 2, 3, 6]

    for source_col, prefix in lag_specs:
        df = _month_shift(df, source_col, shifts, prefix)

    df["SENT_TO_LEGAL"] = df["SENT_TO_LEGAL"].notna().astype(int)

    return df
 
 
# =========================================================================
# 2. Engineered (base / point-in-time)
# =========================================================================
 
 
def create_engineered_base(lagged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all Engineered features that do NOT depend on lagged engineered
    values.  These are derived directly from Original or Original (lagged)
    columns already present after step 1.
 
    New columns created:
        Metadata:
            MONTH_YYYYMM
 
        Card status:
            IS_ACTIVE_MTH
 
        Demographic:
            ADVERTISING_CONSENT
            AGE                     (placeholder – needs DOB in raw; stubbed)
            TENURE_MONTHS           (placeholder – needs CARD_OPEN_DT; stubbed)
 
        Utilization (current month):
            UTILIZATION_NOW         (TOTAL_EXPOSURE_AMT / CARD_LIMIT)
 
        Statement detail aggregates (already in SQL, but renamed for clarity):
            TRX_CNT_STMT_MTH       ← COUNT of statement detail rows (= proxy from TRX_AMT_RON not null)
            FEE_AMT_MTH            ← FEE_AMOUNT
            N_INSTALLMENT_TRX_MTH  ← derived from TOTAL_INSTALLMENT_COUNT > 0
            INSTALLMENT_AMT_MTH    ← TOTAL_INSTALLMENT_AMOUNT
            REMAINING_INST_AMT_MTH ← REMAINING_INSTALLMENT_AMOUNT
            PCT_INSTALLMENT_REMAINING_MTH
 
        Statement aggregates:
            AVG_REVOLVING_AMT_MTH
            
 
        Transaction aggregates (TRX_CNT_MTH, AVG_TICKET_MTH, etc.)
            NOTE: These come from EDW_FACT.CARD_TRX_ISSUING which is NOT in
            the current SQL extract.  If your raw_df already has them, they
            pass through.  Otherwise they are stubbed as NaN and should be
            populated upstream.
 
        Issue features:
            N_ISSUES_MTH, N_ISSUES_OPEN_MTH, HAS_ISSUE_MTH
            NOTE: ISSUE table is not in the SQL extract; stubbed as 0.
    """
    df = lagged_df.copy()
 
    # ── Metadata ──────────────────────────────────────────────────────────
    # ── Metadata ──────────────────────────────────────────────────────────
    dt = pd.to_datetime(df["MONTH_END"], errors="coerce")              # Coerce → NaT si viene inválido
    df["MONTH_YYYYMM"] = dt.dt.strftime("%Y%m").astype("string")       # Year-month como string (ej. "202401")
    df["MONTH_YYYYMM"] = df["MONTH_YYYYMM"].astype("category")         # categorical, como pide el excel de features que tnemos
 
    # ── IS_ACTIVE_MTH ─────────────────────────────────────────────────────
    active_statuses = {"A", "E", "L", "V"} #FALTAN ESTOS DATOS EN EL DF BASE
 
    last_exp = pd.to_datetime(df["LAST_EXPOSURE_DATE"], errors="coerce")
    month_end_dt = pd.to_datetime(df["MONTH_END"]) #Bien pero porq no usar MONTH_YY
 
    months_since_last_exp = (
        (month_end_dt.dt.year - last_exp.dt.year) * 12 #dif de años pasado a meses
        + (month_end_dt.dt.month - last_exp.dt.month) #dif de meses
    )
 
    df["IS_ACTIVE_MTH"] = (
        df["STATUS"].isin(active_statuses)            if "STATUS" in df.columns #falta esta columna en la base
        else True
    ) & (df["DPD"].fillna(0) < 15) & (months_since_last_exp.fillna(999) <= 12) #fill na a no activo
 
    df["IS_ACTIVE_MTH"] = df["IS_ACTIVE_MTH"].astype(int)
 
    # ── ADVERTISING_CONSENT ───────────────────────────────────────────────
    if "ADVERTISING" in df.columns:
        ad_upper = df["ADVERTISING"].astype(str).str.strip().str.upper()
        df["ADVERTISING_CONSENT"] = np.where(
            ad_upper == "SUNT DE ACORD", 1,
            np.where(ad_upper == "NU SUNT DE ACORD", 0, np.nan)
        )
    else:
        df["ADVERTISING_CONSENT"] = np.nan
   
    #── AGE ─────────────────────────────────────────────────────────
    if "BIRTH_DT" in df.columns: #FALTA EN LA BASE
        dob = pd.to_datetime(df["BIRTH_DT"], errors="coerce")
 
        months = (
            (month_end_dt.dt.year - dob.dt.year) * 12
            + (month_end_dt.dt.month - dob.dt.month)
            )
 
        df["AGE"] = (months / 12)
    else:
        df["AGE"] = pd.Series([pd.NA] * len(df))
 
 
    # ── TENURE_MONTHS (stub) ──────────────────────────────────────────────
    if "OPEN_DT" in df.columns:
        open_dt = pd.to_datetime(df["OPEN_DT"], errors="coerce")
        df["TENURE_MONTHS"] = (
            (month_end_dt.dt.year - open_dt.dt.year) * 12
            + (month_end_dt.dt.month - open_dt.dt.month)
        )
    else:
        df["TENURE_MONTHS"] = np.nan
 
    # ── UTILIZATION_NOW ───────────────────────────────────────────────────
    df["UTILIZATION_NOW"] = (
        df["TOTAL_EXPOSURE_AMT"] / df["CARD_LIMIT"].replace(0, np.nan)
    )
 
    # ── Statement detail features ─────────────────────────────────────────
    df["PCT_INSTALLMENT_REMAINING_MTH"] = (
        df["REMAINING_INSTALLMENT_AMOUNT"]
        / df["TOTAL_INSTALLMENT_AMOUNT"].replace(0, np.nan)
        * 100
    )

    # ── Issue features (from NUMBER_OF_ISSUES in SQL) ─────────────────────
    df["HAS_ISSUE_MTH"] = (df["NUMBER_OF_ISSUES"].fillna(0) > 0).astype(int)

    return df


# =========================================================================
# 3. Engineered (lagged) + Deltas + Pct Changes + Trends + Volatility
# =========================================================================
 
def create_engineered_lagged_and_derived(eng_df: pd.DataFrame) -> pd.DataFrame:
    """
    From the output of step 2 (which already contains Original, Original
    (lagged), and Engineered base columns), compute:
    """
 
    df = eng_df.copy()
    df = df.sort_values(["CARD_ID", "MONTH_END"]).reset_index(drop=True)
 
    # A) Engineered lags
    eng_lag_specs: List[Tuple[str, str]] = [
        ("UTILIZATION_NOW", "UTILIZATION_NOW"),
        ("TRX_AMT_MTH",    "TRX_AMT_MTH"),
        ("TRX_CNT_MTH",    "TRX_CNT_MTH"),
        ("AVG_TICKET_MTH",  "AVG_TICKET_MTH"),
    ]
    shifts = [1, 2, 3, 6]
 
    for source_col, prefix in eng_lag_specs:
        df = _month_shift(df, source_col, shifts, prefix)
 
    # B) Deltas
    for n in [1, 2, 3, 6]:
        df[f"TOTAL_EXPOSURE_AMT_DELTA_{n}M"] = df["TOTAL_EXPOSURE_AMT"] - df[f"TOTAL_EXPOSURE_AMT_{n}M"]
 
    for n in [1, 2, 3, 6]:
        df[f"DPD_DELTA_{n}M"] = df["DPD"] - df[f"DPD_{n}M"]
 
    for n in [1, 2, 3, 6]:
        df[f"OVERDUE_AMT_RON_DELTA_{n}M"] = df["OVERDUE_AMT_RON"] - df[f"OVERDUE_AMT_RON_{n}M"]

    for n in [1, 2, 3, 6]:
        df[f"CARD_LIMIT_DELTA_{n}M"] = df["CARD_LIMIT"] - df[f"CARD_LIMIT_{n}M"]

    for n in [1, 2, 3, 6]:
        df[f"UTILIZATION_NOW_DELTA_{n}M"] = df["UTILIZATION_NOW"] - df[f"UTILIZATION_NOW_{n}M"]
 
    for n in [1, 2, 3, 6]:
        df[f"TRX_AMT_MTH_DELTA_{n}M"] = df["TRX_AMT_MTH"] - df[f"TRX_AMT_MTH_{n}M"]
 
    for n in [1, 2, 3, 6]:
        df[f"TRX_CNT_MTH_DELTA_{n}M"] = df["TRX_CNT_MTH"] - df[f"TRX_CNT_MTH_{n}M"]
 
    # C) Percentage changes
    for n in [1, 2, 3, 6]:
        df[f"TOTAL_EXPOSURE_AMT_PCT_CHG_{n}M"] = _safe_pct_change(
            df["TOTAL_EXPOSURE_AMT"], df[f"TOTAL_EXPOSURE_AMT_{n}M"]
        )
 
    for n in [1, 2, 3, 6]:
        df[f"TRX_AMT_MTH_PCT_CHG_{n}M"] = _safe_pct_change(
            df["TRX_AMT_MTH"], df[f"TRX_AMT_MTH_{n}M"]
        )
 
    for n in [1, 2, 3, 6]:
        df[f"TRX_CNT_MTH_PCT_CHG_{n}M"] = _safe_pct_change(
            df["TRX_CNT_MTH"], df[f"TRX_CNT_MTH_{n}M"]
        )
 
    for n in [1, 2, 3, 6]:
        df[f"AVG_TICKET_MTH_PCT_CHG_{n}M"] = _safe_pct_change(
            df["AVG_TICKET_MTH"], df[f"AVG_TICKET_MTH_{n}M"]
        )
 
    for n in [1, 2, 3, 6]:
        df[f"UTILIZATION_NOW_PCT_CHG_{n}M"] = _safe_pct_change(
            df["UTILIZATION_NOW"], df[f"UTILIZATION_NOW_{n}M"]
        )
 
    # D) Trends
    df["TOTAL_EXPOSURE_AMT_TRENDING_UP_MTH"] = (
        (df["TOTAL_EXPOSURE_AMT"] > df["TOTAL_EXPOSURE_AMT_3M"])
        & (df["TOTAL_EXPOSURE_AMT_3M"] > df["TOTAL_EXPOSURE_AMT_6M"])
    ).astype(int)
 
    df["DPD_TRENDING_UP_MTH"] = (
        (df["DPD"] > df["DPD_3M"])
        & (df["DPD_3M"] > df["DPD_6M"])
    ).astype(int)
 
    df["UTILIZATION_NOW_TRENDING_UP_MTH"] = (
        (df["UTILIZATION_NOW"] > df["UTILIZATION_NOW_3M"])
        & (df["UTILIZATION_NOW_3M"] > df["UTILIZATION_NOW_6M"])
    ).astype(int)
 
    # E) Volatility
    exposure_cols = [
        "TOTAL_EXPOSURE_AMT", "TOTAL_EXPOSURE_AMT_1M", "TOTAL_EXPOSURE_AMT_2M",
        "TOTAL_EXPOSURE_AMT_3M", "TOTAL_EXPOSURE_AMT_6M",
    ]
    df["TOTAL_EXPOSURE_AMT_RANGE_MTH"] = (
        df[exposure_cols].max(axis=1) - df[exposure_cols].min(axis=1)
    )
 
    dpd_cols = ["DPD", "DPD_1M", "DPD_2M", "DPD_3M", "DPD_6M"]
    df["DPD_RANGE_MTH"] = (
        df[dpd_cols].max(axis=1) - df[dpd_cols].min(axis=1)
    )

    # Drop original lag columns (expose only delta features, not lags)
    _ORIGINAL_LAG_COLS_TO_DROP: List[str] = []
    for prefix in ["TOTAL_EXPOSURE_AMT", "CARD_LIMIT", "DPD", "OVERDUE_AMT_RON"]:
        _ORIGINAL_LAG_COLS_TO_DROP.extend([f"{prefix}_{n}M" for n in [1, 2, 3, 6]])
    df = df.drop(columns=[c for c in _ORIGINAL_LAG_COLS_TO_DROP if c in df.columns])

    return df
 


# =========================================================================
# 5. Churn target flags
# =========================================================================

def create_churn_targets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create binary churn target flags for 1-, 2- and 3-month horizons.
 
    For each observation (CARD_ID, MONTH_END) the flag is **1** when the
    card's ``CARD_CANCELLATION_DATE`` falls strictly *after* MONTH_END and
    within the next *N* calendar months (inclusive).
 
    New columns
    -----------
    CHURN_1M : int  – cancellation within the next 1 month
    CHURN_2M : int  – cancellation within the next 2 months
    CHURN_3M : int  – cancellation within the next 3 months
 
    Cards without a cancellation date are labelled 0 for every horizon.
    """
    out = df.copy()
 
    month_end = pd.to_datetime(out["MONTH_END"])
    cancel_dt = pd.to_datetime(out["CARD_CANCELLATION_DATE"], errors="coerce")
 
    months_to_cancel = (
        (cancel_dt.dt.year - month_end.dt.year) * 12
        + (cancel_dt.dt.month - month_end.dt.month)
    )
 
    has_future_cancel = cancel_dt.notna() & (months_to_cancel > 0)
 
    for horizon in [1, 2, 3]:
        out[f"CHURN_{horizon}M"] = (
            has_future_cancel & (months_to_cancel <= horizon)
        ).astype(int)
 
    return out
 
 
 
