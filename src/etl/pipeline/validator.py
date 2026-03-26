"""
Wrapper for pandera validation with logging and Prefect artifact support.

Usage:
    validator = DataValidator(logger)
    df, report_md = validator.validate(df, SomeSchema, "step name")
"""

import logging
from typing import Tuple

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors
from prefect import get_run_logger


class DataValidator:
    """Validates DataFrames against pandera schemas, logs results, and
    produces Markdown reports suitable for Prefect artifacts."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def _prefect_logger(self) -> logging.Logger:
        try:
            return get_run_logger()
        except Exception:
            return self.logger

    def validate(
        self,
        df: pd.DataFrame,
        schema: pa.DataFrameSchema,
        step_name: str,
    ) -> Tuple[pd.DataFrame, str]:
        """
        Validate *df* against *schema*.

        Returns ``(validated_df, markdown_report)``.

        The markdown report summarises shape, null stats, and any
        validation failures.  On failure the original DataFrame is
        returned so the pipeline can continue.
        """
        prefect_log = self._prefect_logger()
        stats_md = self._stats_markdown(df, step_name)
        self._log_stats(df, step_name)

        try:
            validated = schema.validate(df, lazy=True)
            self.logger.info(
                "[%s] Validation PASSED (%s)", step_name, schema.name
            )
            prefect_log.info(
                "[%s] Validation PASSED (%s)", step_name, schema.name
            )
            report = (
                f"## {step_name}\n\n"
                f"{stats_md}\n\n"
                f"**Validation:** PASSED ({schema.name})\n"
            )
            return validated, report

        except SchemaErrors as exc:
            n_errors = len(exc.failure_cases)
            self.logger.warning(
                "[%s] Validation FAILED (%s) - %d error(s)",
                step_name,
                schema.name,
                n_errors,
            )
            prefect_log.warning(
                "[%s] Validation FAILED (%s) - %d error(s)",
                step_name,
                schema.name,
                n_errors,
            )

            failure_summary = (
                exc.failure_cases
                .groupby("column")
                .size()
                .sort_values(ascending=False)
            )
            for col_name, count in failure_summary.items():
                self.logger.info(
                    "  column=%-30s  %d failure(s)", col_name, count
                )
                prefect_log.info(
                    "  column=%-30s  %d failure(s)", col_name, count
                )

            deduped = exc.failure_cases.groupby("column").first().reset_index()
            error_rows = []
            for _, row in deduped.iterrows():
                col = row.get("column", "dataframe")
                chk = row.get("check", "")
                val = row.get("failure_case", "")
                n_col = int(failure_summary.get(col, 1))
                error_rows.append(
                    f"| {col} | {chk} | {val} | {n_col:,} |"
                )

            error_table = (
                "| Column | Check | Example Failure | Total Failures |\n"
                "|--------|-------|-----------------|----------------|\n"
                + "\n".join(error_rows)
            )

            report = (
                f"## {step_name}\n\n"
                f"{stats_md}\n\n"
                f"**Validation:** FAILED ({schema.name}) - "
                f"{n_errors} error(s)\n\n"
                f"*Showing one example per column:*\n\n"
                f"{error_table}\n"
            )
            return df, report

    def _stats_markdown(self, df: pd.DataFrame, step_name: str) -> str:
        """Build a markdown summary of shape and null percentages."""
        rows, cols = df.shape
        lines = [f"**Shape:** {rows:,} rows x {cols} columns"]

        if rows == 0:
            lines.append("**Warning:** DataFrame is empty")
            return "\n".join(lines)

        null_pct = (df.isnull().sum() / rows * 100).sort_values(ascending=False)
        non_zero = null_pct[null_pct > 0]

        if non_zero.empty:
            lines.append("**Nulls:** none detected")
        else:
            lines.append(f"**Columns with nulls:** {len(non_zero)} / {cols}")
            top = non_zero.head(10)
            null_table = (
                "| Column | Null % |\n|--------|--------|\n"
                + "\n".join(
                    f"| {col} | {pct:.2f}% |"
                    for col, pct in top.items()
                )
            )
            lines.append(null_table)

        return "\n\n".join(lines)

    def _log_stats(self, df: pd.DataFrame, step_name: str) -> None:
        """Log row/column counts and top-N null percentages."""
        rows, cols = df.shape
        self.logger.info(
            "[%s] DataFrame shape: %d rows x %d columns", step_name, rows, cols
        )

        if rows == 0:
            self.logger.warning("[%s] DataFrame is empty", step_name)
            return

        null_pct = (df.isnull().sum() / rows * 100).sort_values(ascending=False)
        non_zero = null_pct[null_pct > 0]

        if non_zero.empty:
            self.logger.info("[%s] No null values detected", step_name)
            return

        self.logger.info(
            "[%s] Columns with nulls: %d / %d", step_name, len(non_zero), cols
        )
        for col_name, pct in non_zero.head(10).items():
            self.logger.debug("  %-35s %6.2f%% null", col_name, pct)
