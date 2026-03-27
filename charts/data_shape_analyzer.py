"""
charts/data_shape_analyzer.py

Analyse the shape and column types of a pandas DataFrame returned by
vn.run_sql().  The output drives chart_type_selector.py.

Public API:
    from charts.data_shape_analyzer import analyze_data_shape, DataShape

    shape: DataShape = analyze_data_shape(df, question)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Keywords that hint at specific data patterns ─────────────────────────────
_TEMPORAL_KEYWORDS = {"trend", "over time", "monthly", "weekly", "daily",
                      "yearly", "by month", "by date", "by year", "by week",
                      "time series", "growth", "month_year"}
_RANKING_KEYWORDS = {"top", "bottom", "best", "worst", "highest", "lowest",
                     "most", "least", "rank", "ranking"}
_DISTRIBUTION_KEYWORDS = {"distribution", "histogram", "spread", "frequency",
                          "range"}
_PART_WHOLE_KEYWORDS = {"share", "percentage", "proportion", "breakdown",
                        "composition", "split", "pie", "donut",
                        "contribution", "mix"}
_COMPARISON_KEYWORDS = {"compare", "comparison", "vs", "versus", "difference",
                        "against"}
_GEO_KEYWORDS = {"state", "city", "region", "geography", "location", "map",
                 "country", "pin code", "postal"}

# ── Temporal dtype detection ─────────────────────────────────────────────────
_TEMPORAL_DTYPES = {"datetime64[ns]", "datetime64", "date", "dbdate",
                    "datetime64[ns, utc]"}
_TEMPORAL_COL_NAMES = {"order_date", "invoice_date", "date", "month_year",
                       "month", "year", "week", "day", "created_at",
                       "updated_at"}


@dataclass
class ColumnProfile:
    """Profile of a single DataFrame column."""
    name: str
    dtype_raw: str
    role: str  # "numeric", "temporal", "categorical", "boolean", "unknown"
    n_unique: int = 0
    null_pct: float = 0.0
    sample_values: list = field(default_factory=list)


@dataclass
class DataShape:
    """Complete shape analysis result for a DataFrame."""
    n_rows: int
    n_cols: int
    columns: List[ColumnProfile]

    # Counts by role
    n_numeric: int = 0
    n_temporal: int = 0
    n_categorical: int = 0
    n_boolean: int = 0

    # Primary column references (first of each type found)
    temporal_col: Optional[str] = None
    numeric_cols: List[str] = field(default_factory=list)
    categorical_cols: List[str] = field(default_factory=list)
    boolean_cols: List[str] = field(default_factory=list)

    # Categorical cardinality of the FIRST categorical column
    n_categories: int = 0

    # Detected data pattern
    data_pattern: str = "general"
    # One of: single_value, time_series, ranking, distribution,
    #         comparison, part_of_whole, correlation, matrix, geo, general

    # Flags
    is_empty: bool = False
    is_single_value: bool = False
    all_null: bool = False
    all_identical: bool = False


# ── Column classification ────────────────────────────────────────────────────

def _classify_column(col: pd.Series) -> str:
    """
    Classify a single column into: numeric, temporal, categorical, boolean,
    or unknown.
    """
    col_name_lower = col.name.lower() if isinstance(col.name, str) else ""
    dtype_str = str(col.dtype).lower()

    # Boolean
    if dtype_str == "bool" or (col.dropna().isin([0, 1, True, False]).all()
                               and col.nunique() <= 2
                               and len(col.dropna()) > 0):
        return "boolean"

    # Temporal — by dtype
    if any(t in dtype_str for t in ("datetime", "date", "timestamp", "period")):
        return "temporal"

    # Temporal — by column name (handles string-typed date columns like month_year)
    if col_name_lower in _TEMPORAL_COL_NAMES:
        # Verify it looks date-ish: try parsing a sample
        if dtype_str.startswith(("object", "str")):
            sample = col.dropna().head(5)
            try:
                pd.to_datetime(sample, infer_datetime_format=True)
                return "temporal"
            except (ValueError, TypeError):
                # month_year like "Jan 2026" — treat as temporal for ordering
                if col_name_lower == "month_year":
                    return "temporal"
        return "temporal"

    # Numeric
    if pd.api.types.is_numeric_dtype(col):
        return "numeric"

    # Categorical (string / object)
    if dtype_str.startswith(("object", "str", "category", "string")):
        return "categorical"

    return "unknown"


def _profile_column(col: pd.Series) -> ColumnProfile:
    """Build a ColumnProfile for a single column."""
    role = _classify_column(col)
    n_unique = int(col.nunique())
    null_pct = float(col.isnull().mean()) * 100.0
    sample = col.dropna().head(5).tolist()
    return ColumnProfile(
        name=str(col.name),
        dtype_raw=str(col.dtype),
        role=role,
        n_unique=n_unique,
        null_pct=round(null_pct, 2),
        sample_values=sample,
    )


# ── Pattern detection ────────────────────────────────────────────────────────

def _detect_pattern(
    shape: DataShape,
    question: str,
) -> str:
    """
    Determine the data pattern based on column types, cardinality, and
    question keywords.  Returns one of the canonical pattern strings.
    """
    q_lower = question.lower() if question else ""

    # Single value
    if shape.n_rows == 1 and shape.n_cols == 1:
        return "single_value"
    if shape.n_rows == 1:
        return "single_value"

    # Keyword-driven hints (checked before structural rules so user intent wins)
    if any(kw in q_lower for kw in _TEMPORAL_KEYWORDS) and shape.n_temporal >= 1:
        return "time_series"
    if any(kw in q_lower for kw in _PART_WHOLE_KEYWORDS):
        return "part_of_whole"
    if any(kw in q_lower for kw in _RANKING_KEYWORDS):
        return "ranking"
    if any(kw in q_lower for kw in _DISTRIBUTION_KEYWORDS):
        return "distribution"
    if any(kw in q_lower for kw in _COMPARISON_KEYWORDS):
        return "comparison"
    if any(kw in q_lower for kw in _GEO_KEYWORDS):
        return "geo"

    # Structural detection
    if shape.n_temporal >= 1 and shape.n_numeric >= 1:
        return "time_series"

    if (shape.n_categorical == 2 and shape.n_numeric == 1
            and shape.n_temporal == 0):
        # Could be a matrix/heatmap if both categoricals have moderate cardinality
        cat_cols = shape.categorical_cols
        if len(cat_cols) == 2:
            return "matrix"

    if (shape.n_numeric == 2 and shape.n_categorical == 0
            and shape.n_temporal == 0):
        return "correlation"

    if (shape.n_categorical >= 1 and shape.n_numeric >= 1
            and shape.n_temporal == 0):
        if shape.n_categories <= 8:
            return "part_of_whole"
        return "ranking"

    if shape.n_numeric == 1 and shape.n_categorical == 0 and shape.n_rows > 20:
        return "distribution"

    return "general"


# ── Main public function ─────────────────────────────────────────────────────

def analyze_data_shape(
    df: pd.DataFrame,
    question: str = "",
) -> DataShape:
    """
    Analyse a DataFrame and return a DataShape describing its structure.

    Args:
        df: The query result DataFrame.
        question: The original user question (used for pattern detection).

    Returns:
        DataShape with column profiles, counts, pattern, and flags.
    """
    if df is None or df.empty:
        logger.info("DataFrame is empty or None.")
        return DataShape(
            n_rows=0, n_cols=0, columns=[], is_empty=True,
            data_pattern="single_value",
        )

    n_rows, n_cols = df.shape
    logger.debug("Analyzing DataFrame: %d rows × %d cols", n_rows, n_cols)

    # Profile every column
    columns: list[ColumnProfile] = [_profile_column(df[c]) for c in df.columns]

    # Group by role
    numeric_cols = [c.name for c in columns if c.role == "numeric"]
    temporal_cols = [c.name for c in columns if c.role == "temporal"]
    categorical_cols = [c.name for c in columns if c.role == "categorical"]
    boolean_cols = [c.name for c in columns if c.role == "boolean"]

    # Category count from first categorical column
    n_categories = 0
    if categorical_cols:
        n_categories = int(df[categorical_cols[0]].nunique())

    # Flags
    all_null = all(df[c].isnull().all() for c in df.columns)
    all_identical = (
        n_rows > 1
        and all(df[c].nunique(dropna=True) <= 1 for c in df.columns)
    )
    is_single_value = (n_rows == 1 and n_cols == 1)

    shape = DataShape(
        n_rows=n_rows,
        n_cols=n_cols,
        columns=columns,
        n_numeric=len(numeric_cols),
        n_temporal=len(temporal_cols),
        n_categorical=len(categorical_cols),
        n_boolean=len(boolean_cols),
        temporal_col=temporal_cols[0] if temporal_cols else None,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        boolean_cols=boolean_cols,
        n_categories=n_categories,
        is_empty=False,
        is_single_value=is_single_value,
        all_null=all_null,
        all_identical=all_identical,
    )

    # Detect data pattern
    shape.data_pattern = _detect_pattern(shape, question)
    logger.info(
        "DataShape: %d rows, %d cols | numeric=%d temporal=%d cat=%d | "
        "pattern=%s | categories=%d",
        n_rows, n_cols, shape.n_numeric, shape.n_temporal,
        shape.n_categorical, shape.data_pattern, shape.n_categories,
    )

    return shape


__all__ = ["analyze_data_shape", "DataShape", "ColumnProfile"]