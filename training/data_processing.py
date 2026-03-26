"""
Shared data loading and preparation for churn model training notebooks.

Usage::

    from data_processing import load_and_prepare_data

    DATA_CONFIG = {
        "data_path": r"\\server\path\to\data.parquet",
        "target": "CHURN_2M",
        "drop_cols": ["CARD_ID", "CARD_CANCELLATION_DATE", ...],
        "random_state": 42,
        "test_month_start": datetime(2025, 2, 1),
        "month_window_start": datetime(2023, 2, 1),
        "month_window_end": datetime(2025, 1, 31),
        "majority_reduction_pct": 0.10,
        "n_lags": 12,
        "priority_non_null_cols": [...],
        "use_streaming_collect": True,
    }

    X_train, X_test, y_train, y_test, feature_names, data_info = (
        load_and_prepare_data(DATA_CONFIG)
    )
"""

import gc
import logging
import time
from datetime import datetime

import numpy as np
import polars as pl

log = logging.getLogger("churn.data")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def resolve_schema_columns(requested: list[str], available: list[str]) -> list[str]:
    """Map each requested name to the actual column name in *available* (case-insensitive)."""
    lower = {a.lower(): a for a in available}
    out: list[str] = []
    for name in requested:
        if name in available:
            out.append(name)
        elif name.lower() in lower:
            out.append(lower[name.lower()])
        else:
            raise KeyError(
                f"Column {name!r} not found in Parquet schema. "
                f"Available (sample): {available[:20]}..."
            )
    return out


def downcast_numeric(df: pl.DataFrame) -> pl.DataFrame:
    """Downcast numeric columns to smaller dtypes to cut RAM."""
    exprs: list[pl.Expr] = []
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == pl.Float64:
            exprs.append(pl.col(col).cast(pl.Float32))
        elif dtype == pl.Float32:
            exprs.append(pl.col(col))
        elif dtype == pl.Int64:
            s = df[col]
            vmin, vmax = s.min(), s.max()
            if vmin is not None and vmax is not None:
                if vmin >= np.iinfo(np.int8).min and vmax <= np.iinfo(np.int8).max:
                    exprs.append(pl.col(col).cast(pl.Int8))
                elif vmin >= np.iinfo(np.int16).min and vmax <= np.iinfo(np.int16).max:
                    exprs.append(pl.col(col).cast(pl.Int16))
                elif vmin >= np.iinfo(np.int32).min and vmax <= np.iinfo(np.int32).max:
                    exprs.append(pl.col(col).cast(pl.Int32))
                else:
                    exprs.append(pl.col(col))
            else:
                exprs.append(pl.col(col))
        else:
            exprs.append(pl.col(col))
    return df.select(exprs)


def cast_string_cols_to_categorical(lf: pl.LazyFrame, schema: pl.Schema) -> pl.LazyFrame:
    """Cast Utf8/String columns to Categorical in the lazy plan."""
    str_cols = [c for c, dt in schema.items() if dt in (pl.Utf8, pl.String)]
    if not str_cols:
        return lf
    return lf.with_columns(
        pl.col(c).fill_null("__MISSING__").cast(pl.Categorical)
        for c in str_cols
    )


def pivot_to_wide(
    df: pl.DataFrame,
    card_id_col: str,
    time_col: str,
    target_col: str,
    feature_cols: list[str],
    n_lags: int = 12,
) -> pl.DataFrame:
    """Pivot long-format monthly data into one-row-per-card wide format.

    The most recent month per card becomes lag 1 (anchor); its target
    value is kept as the label.  Feature columns are suffixed _lag1 ... _lagN.
    """
    df = df.sort([card_id_col, time_col])
    df = df.with_columns(
        pl.col(time_col)
        .rank("ordinal", descending=True)
        .over(card_id_col)
        .cast(pl.Int32)
        .alias("_lag")
    )
    df = df.filter(pl.col("_lag") <= n_lags)

    anchor = df.filter(pl.col("_lag") == 1).select(
        [card_id_col, pl.col(time_col).alias("_anchor_month"), target_col]
    )

    result = anchor
    for lag in range(1, n_lags + 1):
        lag_slice = df.filter(pl.col("_lag") == lag).select(
            [card_id_col]
            + [pl.col(c).alias(f"{c}_lag{lag}") for c in feature_cols]
        )
        result = result.join(lag_slice, on=card_id_col, how="left")

    return result


def priority_undersample(
    df: pl.DataFrame,
    target: str,
    priority_cols: list[str],
    reduction_pct: float,
    seed: int,
) -> pl.DataFrame:
    """Reduce never-churn majority cards in wide format (one row per card).

    Rows with target == 1 are kept.  A fraction (*reduction_pct*) of target == 0
    rows is dropped, prioritising rows with more non-null *priority_cols*.
    """
    if reduction_pct <= 0:
        return df
    if reduction_pct >= 1:
        raise ValueError("majority_reduction_pct must be in [0, 1).")

    present_priority = [c for c in priority_cols if c in df.columns]

    minority = df.filter(pl.col(target) == 1)
    majority = df.filter(pl.col(target) == 0)
    majority_before = len(majority)
    n_keep = int(round(majority_before * (1.0 - reduction_pct)))

    if n_keep >= majority_before:
        log.info(
            "Undersampling: majority keep count (%s) >= current (%s); no rows dropped.",
            n_keep,
            majority_before,
        )
        return df

    if present_priority:
        majority = majority.with_columns(
            pl.sum_horizontal(
                pl.col(c).is_not_null().cast(pl.Int8) for c in present_priority
            ).alias("_non_null_score")
        )
    else:
        majority = majority.with_columns(pl.lit(0).alias("_non_null_score"))

    majority = majority.with_columns(
        pl.Series("_rand", np.random.RandomState(seed).random(len(majority)))
    )

    majority = (
        majority.sort(["_non_null_score", "_rand"], descending=[True, False])
        .head(n_keep)
        .drop(["_non_null_score", "_rand"])
    )

    result = pl.concat([minority, majority])
    log.info(
        "Undersampling: never-churn cards %s -> %s (dropped %.1f%%), churn cards=%s",
        majority_before,
        len(majority),
        100.0 * reduction_pct,
        len(minority),
    )
    return result


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def load_and_prepare_data(config: dict) -> tuple:
    """End-to-end data loading, pivoting, splitting, and undersampling.

    Parameters
    ----------
    config : dict
        Keys: data_path, target, drop_cols, random_state, test_month_start,
        month_window_start, month_window_end, majority_reduction_pct, n_lags,
        priority_non_null_cols, use_streaming_collect.

    Returns
    -------
    X_train_pd, X_test_pd, y_train, y_test, feature_names, data_info : tuple
        *X_train_pd* / *X_test_pd* are pandas DataFrames (for model fitting).
        *data_info* is a dict with all metadata for MLflow logging.
    """
    data_path = config["data_path"]
    target = config["target"]
    drop_cols = config["drop_cols"]
    random_state = config.get("random_state", 42)
    test_month_start = config["test_month_start"]
    month_window_start = config["month_window_start"]
    month_window_end = config["month_window_end"]
    majority_reduction_pct = config.get("majority_reduction_pct", 0.10)
    n_lags = config.get("n_lags", 12)
    priority_non_null_cols = config.get("priority_non_null_cols", [])
    use_streaming = config.get("use_streaming_collect", True)
    encode_non_numeric = config.get("encode_non_numeric", True)

    data_info: dict = {
        "data_path": str(data_path),
        "target": target,
        "random_state": random_state,
        "test_month_start": str(test_month_start.date()),
        "month_window_start": str(month_window_start.date()),
        "month_window_end": str(month_window_end.date()),
        "majority_reduction_pct": majority_reduction_pct,
        "n_lags": n_lags,
        "encode_non_numeric": encode_non_numeric,
    }

    # -- 1. Lazy scan & filter -----------------------------------------------
    log.info("Building lazy scan + filters...")
    _t0 = time.perf_counter()

    lf = pl.scan_parquet(data_path)

    excluded_lf = lf.filter(
        (pl.col("STATUS") == "X") & pl.col("CARD_CANCELLATION_DATE").is_null()
    )
    lf_filtered = lf.join(excluded_lf, on="CARD_ID", how="anti")

    lf_filtered = lf_filtered.filter(
        pl.col("MONTH_END").is_between(
            pl.lit(month_window_start), pl.lit(month_window_end), closed="both"
        )
        | (pl.col("MONTH_END") >= pl.lit(test_month_start))
    )

    probe = lf_filtered.select(["CARD_ID", "MONTH_END"]).collect()
    n_rows_raw = len(probe)
    date_min, date_max = probe["MONTH_END"].min(), probe["MONTH_END"].max()
    data_info["n_rows_raw"] = n_rows_raw
    data_info["date_range_min"] = str(date_min)
    data_info["date_range_max"] = str(date_max)
    log.info(
        "Rows after filter: %s | date range: %s to %s",
        f"{n_rows_raw:,}",
        date_min,
        date_max,
    )
    del probe
    gc.collect()
    log.info("Lazy scan done in %.1fs", time.perf_counter() - _t0)

    # -- 2. Resolve columns ---------------------------------------------------
    schema_names = list(lf_filtered.collect_schema().keys())
    drop_resolved = resolve_schema_columns(drop_cols, schema_names)

    if target not in schema_names:
        raise KeyError(f"Target {target} not in Parquet schema")

    month_end_col = resolve_schema_columns(["MONTH_END"], schema_names)[0]
    card_id_col = resolve_schema_columns(["CARD_ID"], schema_names)[0]
    drop_lf = [c for c in drop_resolved if c not in (month_end_col, card_id_col)]

    feature_cols = [c for c in schema_names if c not in drop_resolved and c != target]
    data_info["n_original_features"] = len(feature_cols)
    log.info("Original features (pre-pivot): %s", len(feature_cols))

    # -- 3. Collect -----------------------------------------------------------
    log.info("Collecting full DataFrame for wide pivot...")
    _t_collect = time.perf_counter()

    lf_model = lf_filtered.drop(drop_lf)
    del lf_filtered
    gc.collect()

    model_schema = lf_model.collect_schema()

    # Do not create lag features for categorical/text columns.
    # HistGradientBoosting-style categorical handling has a hard cardinality cap,
    # and lagging high-cardinality categorical columns can easily exceed it.
    _enum_dtype = getattr(pl, "Enum", None)
    categorical_like = {pl.Utf8, pl.String, pl.Categorical}
    if _enum_dtype is not None:
        categorical_like.add(_enum_dtype)

    categorical_cols = [c for c in feature_cols if model_schema.get(c) in categorical_like]
    lag_feature_cols = [c for c in feature_cols if c not in categorical_cols]
    data_info["n_categorical_features_pre_pivot"] = len(categorical_cols)
    data_info["n_lagged_features_pre_pivot"] = len(lag_feature_cols)
    log.info(
        "Lagging numeric features only: %s lagged, %s categorical/text not lagged",
        len(lag_feature_cols),
        len(categorical_cols),
    )

    lf_model = cast_string_cols_to_categorical(lf_model, model_schema)

    if use_streaming:
        try:
            df = lf_model.collect(streaming=True)
        except (TypeError, ValueError):
            try:
                df = lf_model.collect(engine="streaming")
            except (TypeError, ValueError):
                df = lf_model.collect()
    else:
        df = lf_model.collect()

    del lf_model
    gc.collect()
    log.info(
        "Polars collect finished in %.1fs | rows=%s cols=%s",
        time.perf_counter() - _t_collect,
        len(df),
        len(df.columns),
    )

    n_unique_cards = df.select(pl.col(card_id_col).n_unique()).item()
    data_info["n_unique_cards_total"] = n_unique_cards

    # -- 4. Temporal split + pivot --------------------------------------------
    log.info("Pivoting to wide format (%s monthly lags)...", n_lags)
    _t_pivot = time.perf_counter()

    train_long = df.filter(pl.col(month_end_col) < pl.lit(test_month_start))

    test_anchor = (
        df.filter(pl.col(month_end_col) >= pl.lit(test_month_start))
        .group_by(card_id_col)
        .agg(pl.col(month_end_col).max().alias("_anchor"))
    )
    test_long = (
        df.join(test_anchor, on=card_id_col)
        .filter(pl.col(month_end_col) <= pl.col("_anchor"))
        .drop("_anchor")
    )
    del df
    gc.collect()

    train_wide = pivot_to_wide(
        train_long, card_id_col, month_end_col, target, lag_feature_cols, n_lags
    )
    del train_long
    gc.collect()

    test_wide = pivot_to_wide(
        test_long, card_id_col, month_end_col, target, lag_feature_cols, n_lags
    )
    del test_long
    gc.collect()

    n_train_cards = len(train_wide)
    n_test_cards = len(test_wide)
    log.info(
        "Pivot finished in %.1fs | train=%s cards, test=%s cards",
        time.perf_counter() - _t_pivot,
        n_train_cards,
        n_test_cards,
    )

    train_wide = train_wide.drop([card_id_col])
    test_wide = test_wide.drop([card_id_col, "_anchor_month"])

    # -- 5. Undersample -------------------------------------------------------
    n_before_undersample = len(train_wide)
    train_wide = priority_undersample(
        train_wide, target, priority_non_null_cols, majority_reduction_pct, random_state
    )
    n_after_undersample = len(train_wide)

    churn_counts = train_wide.group_by(target).len().sort(target)
    log.info("Class distribution after undersampling:\n%s", churn_counts)
    log.info(
        "Temporal split - train: %s cards, test: %s cards",
        f"{n_train_cards:,}",
        f"{n_test_cards:,}",
    )
    log.info(
        "Undersampling: %s -> %s training cards",
        f"{n_before_undersample:,}",
        f"{n_after_undersample:,}",
    )

    # Preserve anchor months for temporal CV, then drop before feature extraction
    data_info["_train_anchor_months"] = train_wide["_anchor_month"].to_numpy()
    train_wide = train_wide.drop(["_anchor_month"])

    # -- 6. Separate features / target ----------------------------------------
    feature_names = [c for c in train_wide.columns if c != target]
    n_wide_features = len(feature_names)

    X_train = train_wide.select(feature_names)
    y_train = train_wide[target].to_numpy()
    X_test = test_wide.select(feature_names)
    y_test = test_wide[target].to_numpy()

    del train_wide, test_wide
    gc.collect()

    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    scale_pos_weight = round(neg / max(pos, 1), 4)

    # Convert to pandas for model training
    X_train_pd = X_train.to_pandas()
    del X_train
    gc.collect()

    X_test_pd = X_test.to_pandas()
    del X_test
    gc.collect()

    if encode_non_numeric:
        # Ensure compatibility with estimators that only accept numeric inputs
        # (e.g., HistGradientBoostingClassifier). Convert any object/category
        # columns to integer codes (missing -> -1).
        for _df in (X_train_pd, X_test_pd):
            non_numeric_cols = [
                c
                for c in _df.columns
                if str(_df[c].dtype) in ("object", "category", "string")
            ]
            for c in non_numeric_cols:
                _df[c] = (
                    _df[c]
                    .astype("category")
                    .cat.add_categories(["__MISSING__"])
                    .fillna("__MISSING__")
                    .cat.codes.astype(np.int32)
                )

    train_pos_rate = round(float(y_train.mean()), 4)
    test_pos_rate = round(float(y_test.mean()), 4)

    data_info.update(
        {
            "n_cards_train": n_after_undersample,
            "n_cards_test": n_test_cards,
            "n_cards_train_before_undersample": n_before_undersample,
            "n_features": n_wide_features,
            "train_positive_rate": train_pos_rate,
            "test_positive_rate": test_pos_rate,
            "scale_pos_weight": scale_pos_weight,
        }
    )

    log.info(
        "Wide features: %s | X_train: %s, X_test: %s, scale_pos_weight: %s",
        n_wide_features,
        X_train_pd.shape,
        X_test_pd.shape,
        scale_pos_weight,
    )

    return X_train_pd, X_test_pd, y_train, y_test, feature_names, data_info
