"""
Shared MLflow logging utilities for churn model training notebooks.
"""

import json
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    accuracy_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

logger = logging.getLogger(__name__)

TRACKING_URI = os.environ.get(
    "MLFLOW_TRACKING_URI", "sqlite:///training/mlflow.db"
)


def configure_training_logging(level: int = logging.INFO) -> None:
    """Send timestamped progress lines to the console (Jupyter / terminal)."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


# ---------------------------------------------------------------------------
# Run management
# ---------------------------------------------------------------------------


@contextmanager
def start_run(experiment_name, run_name=None, params=None, tags=None):
    """Set experiment and start a tracked run. Use as a context manager.

    Usage::

        with start_run("churn-rf", run_name="baseline", params={...}) as run:
            model.fit(X_train, y_train)
            log_classification_results(model, X_test, y_test)
    """
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=run_name) as run:
        logger.info(
            "MLflow run started | experiment=%r run_name=%r run_id=%s",
            experiment_name,
            run_name,
            run.info.run_id,
        )
        if params:
            mlflow.log_params(params)
        if tags:
            mlflow.set_tags(tags)
        yield run


# ---------------------------------------------------------------------------
# Dataset logging
# ---------------------------------------------------------------------------


def log_dataset_info(data_info: dict) -> None:
    """Log dataset metadata as MLflow params for full traceability.

    *data_info* is produced by ``data_processing.load_and_prepare_data``.
    All values are converted to strings (MLflow param requirement).
    """
    payload = {k: str(v) for k, v in data_info.items() if not k.startswith("_")}
    logger.info("Dataset info: %s", payload)
    mlflow.log_params(payload)


# ---------------------------------------------------------------------------
# Classification results
# ---------------------------------------------------------------------------


def log_classification_results(model, X_test, y_test, model_flavor="sklearn"):
    """Compute and log classification metrics, artifacts, and the model.

    Returns the metrics dict.
    """
    logger.info("Scoring test set...")
    y_pred = model.predict(X_test)
    y_proba = (
        model.predict_proba(X_test)[:, 1]
        if hasattr(model, "predict_proba")
        else None
    )

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
    }
    if y_proba is not None:
        metrics["roc_auc"] = roc_auc_score(y_test, y_proba)
        metrics["avg_precision"] = average_precision_score(y_test, y_proba)

    mlflow.log_metrics(metrics)
    logger.info("Metrics logged: %s", metrics)

    with tempfile.TemporaryDirectory() as tmp:
        # Confusion matrix
        fig, ax = plt.subplots(figsize=(6, 5))
        ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax, cmap="Blues")
        ax.set_title("Confusion Matrix")
        cm_path = os.path.join(tmp, "confusion_matrix.png")
        fig.savefig(cm_path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        mlflow.log_artifact(cm_path)

        # Classification report
        report = classification_report(y_test, y_pred, zero_division=0)
        report_path = os.path.join(tmp, "classification_report.txt")
        with open(report_path, "w") as f:
            f.write(report)
        mlflow.log_artifact(report_path)

    # Log model
    logger.info("Logging model artifact (flavor=%s)...", model_flavor)
    flavor = _get_mlflow_flavor(model_flavor)
    flavor.log_model(model, artifact_path="model")

    return metrics


# ---------------------------------------------------------------------------
# Feature importance
# ---------------------------------------------------------------------------


def log_feature_importance(model, feature_names, top_n=30):
    """Log feature importance as a bar chart and CSV artifact."""
    logger.info("Logging feature importance (top_n=%s)...", top_n)
    if hasattr(model, "feature_importances_"):
        importances = model.feature_importances_
    elif hasattr(model, "coef_"):
        importances = np.abs(model.coef_).flatten()
    else:
        return

    imp_df = (
        pd.DataFrame({"feature": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "feature_importance.csv")
        imp_df.to_csv(csv_path, index=False)
        mlflow.log_artifact(csv_path)

        plot_df = imp_df.head(top_n)
        fig, ax = plt.subplots(figsize=(8, max(6, len(plot_df) * 0.3)))
        ax.barh(plot_df["feature"][::-1], plot_df["importance"][::-1])
        ax.set_xlabel("Importance")
        ax.set_title(f"Top {len(plot_df)} Features")
        fig.tight_layout()
        chart_path = os.path.join(tmp, "feature_importance.png")
        fig.savefig(chart_path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        mlflow.log_artifact(chart_path)


# ---------------------------------------------------------------------------
# Hyperparameter tuning artifact
# ---------------------------------------------------------------------------


def log_hyperparameter_tuning(
    tuning_df: pd.DataFrame,
    metric_col: str = "f1",
    max_rows_to_log: int = 5000,
    max_plot_points: int = 500,
):
    """Log a parameter-tuning results table as a CSV + bar-chart artifact.

    *tuning_df* should have one row per candidate, with columns for each
    hyperparameter and the evaluation metric(s).
    """
    logger.info("Logging hyperparameter tuning artifact...")
    n_rows = len(tuning_df)
    if n_rows == 0:
        logger.info("Tuning dataframe is empty; skipping artifact logging.")
        return

    # Large threshold sweeps can contain tens/hundreds of thousands of rows.
    # Downsample for artifacts to keep logging responsive.
    if n_rows > max_rows_to_log:
        idx = np.linspace(0, n_rows - 1, num=max_rows_to_log, dtype=int)
        tuning_csv_df = tuning_df.iloc[idx].copy()
        logger.info(
            "Downsampled tuning CSV from %s to %s rows for fast logging.",
            n_rows,
            len(tuning_csv_df),
        )
    else:
        tuning_csv_df = tuning_df

    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "tuning_results.csv")
        tuning_csv_df.to_csv(csv_path, index=False)
        mlflow.log_artifact(csv_path)

        if metric_col in tuning_df.columns and n_rows > 1:
            if n_rows > max_plot_points:
                plot_idx = np.linspace(0, n_rows - 1, num=max_plot_points, dtype=int)
                tuning_plot_df = tuning_df.iloc[plot_idx].copy()
            else:
                tuning_plot_df = tuning_df

            fig, ax = plt.subplots(figsize=(9, 5))
            if "threshold" in tuning_plot_df.columns:
                ax.plot(
                    tuning_plot_df["threshold"].to_numpy(),
                    tuning_plot_df[metric_col].to_numpy(),
                    linewidth=1.25,
                )
                ax.set_xlabel("threshold")
            else:
                ax.plot(
                    np.arange(len(tuning_plot_df)),
                    tuning_plot_df[metric_col].to_numpy(),
                    linewidth=1.25,
                )
                ax.set_xlabel("candidate index")
            ax.set_ylabel(metric_col)
            ax.set_title("Hyperparameter Tuning Results")
            ax.grid(alpha=0.2)
            fig.tight_layout()
            chart_path = os.path.join(tmp, "tuning_results.png")
            fig.savefig(chart_path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            mlflow.log_artifact(chart_path)


# ---------------------------------------------------------------------------
# Local model saving (for explainability / reuse)
# ---------------------------------------------------------------------------


def save_model_local(model, save_dir: str, metadata: dict) -> str:
    """Save model to disk with a JSON sidecar for later reuse.

    Parameters
    ----------
    model : fitted estimator
    save_dir : str
        Directory where model files are written.
    metadata : dict
        Arbitrary metadata (run_id, experiment, feature_names, data_info, ...).
        Serialised as ``metadata.json`` alongside the model.

    Returns
    -------
    str : path to *save_dir* (absolute).
    """
    import joblib

    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)

    model_file = save_path / "model.joblib"
    joblib.dump(model, model_file)
    logger.info("Model saved to %s", model_file)

    meta_file = save_path / "metadata.json"
    with open(meta_file, "w") as f:
        json.dump(metadata, f, indent=2, default=str)
    logger.info("Metadata saved to %s", meta_file)

    return str(save_path.resolve())


def load_model_local(save_dir: str):
    """Load a model + metadata previously saved by ``save_model_local``.

    Returns (model, metadata_dict).
    """
    import joblib

    save_path = Path(save_dir)
    model = joblib.load(save_path / "model.joblib")
    with open(save_path / "metadata.json") as f:
        metadata = json.load(f)
    return model, metadata


# ---------------------------------------------------------------------------
# XGBoost progress callback
# ---------------------------------------------------------------------------


def xgboost_progress_callback(period: int = 25):
    """Return an XGBoost TrainingCallback that logs every *period* trees."""
    import xgboost as xgb

    class _LogEvery(xgb.callback.TrainingCallback):
        def after_iteration(self, model, epoch, evals_log):
            if (epoch + 1) % period == 0:
                logger.info("XGBoost progress: %s trees completed", epoch + 1)
            return False

    return _LogEvery()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_mlflow_flavor(flavor_name):
    """Return the appropriate mlflow model flavor module."""
    flavors = {
        "sklearn": mlflow.sklearn,
        "xgboost": mlflow.xgboost,
        "lightgbm": mlflow.lightgbm,
    }
    if flavor_name not in flavors:
        raise ValueError(f"Unknown flavor '{flavor_name}'. Use one of {list(flavors)}")
    return flavors[flavor_name]
