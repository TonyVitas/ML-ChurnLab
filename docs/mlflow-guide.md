# MLflow - Setup, Usage & Logging Guide

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

This installs `mlflow` alongside `scikit-learn`, `xgboost`, `lightgbm`, `shap`, and `matplotlib`.

### 2. Start the MLflow tracking server

From the **project root** directory:

```bash
python -m training.mlflow_ui
```

Or directly via the CLI:

```bash
mlflow server \
  --backend-store-uri sqlite:///training/mlflow.db \
  --default-artifact-root training/mlartifacts \
  --host 127.0.0.1 \
  --port 5000
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000) in a browser.

The SQLite database (`training/mlflow.db`) and artifact directory (`training/mlartifacts/`)
are in `.gitignore`, so local experiments won't be committed.

### 3. (Optional) Remote tracking server

To point notebooks at a remote MLflow server, set the environment variable before running:

```bash
set MLFLOW_TRACKING_URI=http://your-mlflow-server:5000
```

If not set, runs are stored locally in `training/mlflow.db`.

---

## Core Concepts

| Concept        | What it is                                                        |
| -------------- | ----------------------------------------------------------------- |
| **Experiment** | A named group of runs (e.g. `churn-random-forest`)                |
| **Run**        | A single training execution with its own params, metrics, artifacts |
| **Parameters** | Input settings (hyperparameters, target name, test size, etc.)    |
| **Metrics**    | Numeric results (accuracy, F1, ROC-AUC, etc.)                    |
| **Artifacts**  | Files produced by the run (plots, CSVs, serialised models)        |
| **Model**      | A logged artifact with a specific flavor (sklearn, xgboost, etc.) |
| **Tags**       | Free-form key-value metadata on a run                             |

---

## Project Structure

```
training/
├── mlflow_ui.py                # MLflow server launcher (SQLite backend)
├── mlflow_utils.py             # Shared MLflow logging helpers
├── data_processing.py          # Shared data loading & preparation
├── xgboost.ipynb               # XGBoost training
├── random_forest.ipynb         # Random Forest baseline
├── explainability_xgboost.ipynb      # XGBoost explainability (SHAP, etc.)
└── explainability_random_forest.ipynb # RF explainability (SHAP, etc.)
```

All notebooks import from `mlflow_utils.py` and `data_processing.py` to keep code consistent.

---

## How the Notebooks Use MLflow

Every notebook follows the same pattern:

```python
from data_processing import load_and_prepare_data
from mlflow_utils import (
    start_run, log_dataset_info, log_classification_results,
    log_feature_importance, save_model_local,
)

# 1. Load data
X_train, X_test, y_train, y_test, feature_names, data_info = load_and_prepare_data(DATA_CONFIG)

# 2. Train & log
with start_run("churn-xgboost", run_name="xgb-baseline", params=params) as run:
    log_dataset_info(data_info)
    model.fit(X_train, y_train)
    metrics = log_classification_results(model, X_test, y_test, model_flavor="xgboost")
    log_feature_importance(model, feature_names)

# 3. Save locally for explainability
save_model_local(model, "saved_models/xgboost", metadata={...})
```

### `start_run(experiment_name, run_name, params, tags)`

- Sets the MLflow experiment (creates it if it doesn't exist).
- Opens a run context manager.
- Immediately logs all `params` and `tags`.

### `log_dataset_info(data_info)`

Logs as **parameters**:

| Param                | Example |
| -------------------- | ------- |
| `data_path`          | `\\path\to\data.parquet` |
| `n_rows_raw`         | 8881267 |
| `n_cards_train`      | 294767  |
| `n_cards_test`       | 248104  |
| `n_features`         | 2016    |
| `train_positive_rate`| 0.1066  |
| `test_positive_rate` | 0.0579  |
| `train_date_range`   | `2023-02 to 2025-01` |
| `test_date_range`    | `2025-02 onward` |
| `majority_reduction_pct` | 0.10 |
| `n_lags`             | 12      |

### `log_classification_results(model, X_test, y_test, model_flavor)`

Logs as **metrics**:

| Metric          | Description                                |
| --------------- | ------------------------------------------ |
| `accuracy`      | Overall accuracy                           |
| `precision`     | Precision for the positive class           |
| `recall`        | Recall for the positive class              |
| `f1`            | F1 score for the positive class            |
| `roc_auc`       | Area under ROC curve (if probabilities)    |
| `avg_precision`  | Area under Precision-Recall curve          |

Logs as **artifacts**:

| Artifact                    | Format |
| --------------------------- | ------ |
| `confusion_matrix.png`      | Image  |
| `classification_report.txt` | Text   |
| `model/`                    | MLflow model (sklearn / xgboost / lightgbm flavor) |

### `log_feature_importance(model, feature_names, top_n=30)`

Logs as **artifacts**:

| Artifact                   | Format |
| -------------------------- | ------ |
| `feature_importance.csv`   | CSV with all features and scores |
| `feature_importance.png`   | Horizontal bar chart (top N)     |

### `save_model_local(model, path, metadata)`

Saves the trained model to disk alongside a JSON metadata file containing
run_id, experiment name, feature names, data info, and hyperparameters.
This enables the explainability notebooks to reload models without MLflow.

---

## Comparing Runs in the UI

1. Open `http://127.0.0.1:5000`.
2. Select an experiment from the sidebar (e.g. `churn-xgboost`).
3. Check the runs you want to compare.
4. Click **Compare**.
5. Use the **Parallel Coordinates** or **Scatter Plot** views to visualise the relationship between parameters and metrics.

To compare across experiments (e.g. RF vs XGBoost), use the **Search** bar:

```
metrics.f1 > 0.5
```

---

## Adding a New Model Notebook

1. Copy any existing notebook (e.g. `random_forest.ipynb`).
2. Change the import and model class.
3. Set `EXPERIMENT` to a new name (e.g. `churn-catboost`).
4. Update `params` with the new model's hyperparameters.
5. Set `model_flavor` in `log_classification_results` to the right MLflow flavor.

---

## Tips

- **Naming conventions**: use lowercase, hyphen-separated experiment names (`churn-random-forest`, `churn-xgboost`).
- **Run names**: describe the variant (`rf-baseline`, `xgb-tuned-v2`, `lgbm-no-winsor`).
- **Tags**: use tags for metadata that isn't a hyperparameter, e.g. `{"data_version": "2026-03", "author": "daniel"}`.
- **Reproducibility**: always log `random_state` and `test_size` so runs can be recreated.
- **Don't log large DataFrames**: log summary statistics (shapes, class balance) rather than full datasets.
