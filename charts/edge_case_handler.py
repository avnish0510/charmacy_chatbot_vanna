"""
charts/edge_case_handler.py

Handle edge cases BEFORE chart generation.

Checks (in order):
    1. df is None or empty (0 rows)         → warning + show SQL, stop
    2. All values NULL                       → warning, stop
    3. All values identical                  → render as table
    4. Single cell (1×1, numeric)            → fast-path KPI card
    5. >5000 rows                            → pre-aggregate (top N + "Other")

Public API:
    from charts.edge_case_handler import handle_edge_cases, EdgeCaseResult
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from charts.chart_type_selector import get_thresholds

logger = logging.getLogger(__name__)


@dataclass
class EdgeCaseResult:
    """
    Result of edge-case handling.

    Attributes:
        should_stop:      If True, caller should NOT proceed to chart generation.
        warning_message:  Human-readable warning (shown in UI).
        df:               Possibly modified DataFrame (e.g. pre-aggregated).
        force_chart_type: If set, override chart_type_selector's decision.
        kpi_value:        If is a single-cell KPI, the formatted value.
        kpi_label:        Column name / label for KPI display.
    """
    should_stop: bool = False
    warning_message: Optional[str] = None
    df: Optional[pd.DataFrame] = None
    force_chart_type: Optional[str] = None
    kpi_value: Optional[str] = None
    kpi_label: Optional[str] = None


def _format_number(value) -> str:
    """Format a numeric value for display (KPI cards)."""
    if pd.isna(value):
        return "N/A"
    try:
        v = float(value)
    except (ValueError, TypeError):
        return str(value)

    abs_v = abs(v)
    if abs_v >= 1_00_00_000:  # 1 crore
        return f"₹{v / 1_00_00_000:,.2f} Cr"
    if abs_v >= 1_00_000:  # 1 lakh
        return f"₹{v / 1_00_000:,.2f} L"
    if abs_v >= 1_000:
        return f"₹{v:,.2f}"
    if v == int(v):
        return f"{int(v):,}"
    return f"{v:,.2f}"


def _pre_aggregate(
    df: pd.DataFrame,
    top_n: int = 30,
) -> pd.DataFrame:
    """
    Pre-aggregate a large DataFrame to top_n rows + an "Other" bucket.

    Strategy:
        - Find the first categorical column and the first numeric column.
        - Group by the categorical column, sum the numeric column.
        - Keep top_n, roll the rest into "Other".
        - If no categorical column exists, just return head(top_n).
    """
    # Find columns by type
    cat_cols = df.select_dtypes(include=["object", "category", "string"]).columns.tolist()
    num_cols = df.select_dtypes(include="number").columns.tolist()

    if not cat_cols or not num_cols:
        logger.debug("Pre-aggregate: no cat/num pair found, returning head(%d).", top_n)
        return df.head(top_n).copy()

    cat_col = cat_cols[0]
    num_col = num_cols[0]

    grouped = (
        df.groupby(cat_col, dropna=False)[num_col]
        .sum()
        .reset_index()
        .sort_values(num_col, ascending=False)
    )

    if len(grouped) <= top_n:
        return grouped.copy()

    top = grouped.head(top_n).copy()
    other_sum = grouped.iloc[top_n:][num_col].sum()
    other_row = pd.DataFrame({cat_col: ["Other"], num_col: [other_sum]})
    result = pd.concat([top, other_row], ignore_index=True)

    logger.info(
        "Pre-aggregated %d rows → %d rows (top %d + Other) on '%s'.",
        len(df), len(result), top_n, cat_col,
    )
    return result


# ── Main public function ─────────────────────────────────────────────────────

def handle_edge_cases(
    df: Optional[pd.DataFrame],
    sql: str = "",
) -> EdgeCaseResult:
    """
    Check for edge cases and return an EdgeCaseResult.

    Args:
        df:  The DataFrame from vn.run_sql(). May be None.
        sql: The SQL that produced the DataFrame (shown in warnings).

    Returns:
        EdgeCaseResult with instructions for the caller.
    """
    thresholds = get_thresholds()
    large_threshold = thresholds.get("large_dataset_rows", 5000)
    pre_agg_top_n = thresholds.get("pre_aggregate_top_n", 30)

    # ── 1. Empty / None ──────────────────────────────────────────────────
    if df is None or df.empty:
        logger.info("Edge case: DataFrame is empty or None.")
        msg = "⚠️ The query returned **0 rows**. No data to visualise."
        if sql:
            msg += f"\n\n**SQL executed:**\n```sql\n{sql}\n```"
        return EdgeCaseResult(should_stop=True, warning_message=msg, df=df)

    # ── 2. All NULL ──────────────────────────────────────────────────────
    if df.isnull().all().all():
        logger.info("Edge case: All values are NULL.")
        return EdgeCaseResult(
            should_stop=True,
            warning_message="⚠️ All values in the result are **NULL**. "
                            "Check filters or column references.",
            df=df,
        )

    # ── 3. All identical ─────────────────────────────────────────────────
    if df.shape[0] > 1 and all(df[c].nunique(dropna=True) <= 1 for c in df.columns):
        logger.info("Edge case: All values identical — rendering as table.")
        return EdgeCaseResult(
            should_stop=False,
            warning_message="ℹ️ All values are identical — showing as a table.",
            df=df,
            force_chart_type="table",
        )

    # ── 4. Single cell (1 row × 1 col, numeric) → KPI ───────────────────
    if df.shape == (1, 1):
        col_name = df.columns[0]
        raw_value = df.iloc[0, 0]
        logger.info("Edge case: Single cell KPI — %s = %s", col_name, raw_value)
        return EdgeCaseResult(
            should_stop=False,
            df=df,
            force_chart_type="kpi_card",
            kpi_value=_format_number(raw_value),
            kpi_label=str(col_name).replace("_", " ").title(),
        )

    # ── 4b. Single row, multiple numeric cols → KPI card ─────────────────
    if df.shape[0] == 1:
        num_cols = df.select_dtypes(include="number").columns
        if len(num_cols) >= 1:
            logger.info("Edge case: Single row with %d numeric cols → KPI.", len(num_cols))
            return EdgeCaseResult(
                should_stop=False,
                df=df,
                force_chart_type="kpi_card",
            )

    # ── 5. Large dataset → pre-aggregate ─────────────────────────────────
    if df.shape[0] > large_threshold:
        logger.info(
            "Edge case: %d rows exceeds threshold %d — pre-aggregating.",
            df.shape[0], large_threshold,
        )
        aggregated = _pre_aggregate(df, top_n=pre_agg_top_n)
        return EdgeCaseResult(
            should_stop=False,
            warning_message=f"ℹ️ Result had **{df.shape[0]:,}** rows — "
                            f"showing top {pre_agg_top_n} + Other.",
            df=aggregated,
        )

    # ── No edge case ─────────────────────────────────────────────────────
    return EdgeCaseResult(should_stop=False, df=df)


__all__ = ["handle_edge_cases", "EdgeCaseResult"]