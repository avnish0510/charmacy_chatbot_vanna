"""
charts/insight_annotator.py

Add insight annotations to Vega-Lite specs:
    - Max / min callouts (rule + text layers)
    - Trend direction detection (via numpy linear regression)
    - Summary text generation

Public API:
    from charts.insight_annotator import annotate_insights, InsightSummary
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class InsightSummary:
    """Human-readable insight summary for the UI."""
    max_label: Optional[str] = None
    max_value: Optional[float] = None
    min_label: Optional[str] = None
    min_value: Optional[float] = None
    trend_direction: Optional[str] = None  # "up", "down", "flat"
    trend_pct_change: Optional[float] = None
    summary_text: str = ""


def _detect_trend(values: pd.Series) -> tuple[str, float]:
    """
    Detect trend direction using linear regression on numeric values.

    Returns:
        (direction, pct_change)
        direction: "up", "down", or "flat"
        pct_change: percentage change from first to last fitted value
    """
    clean = values.dropna().reset_index(drop=True)
    if len(clean) < 3:
        return "flat", 0.0

    try:
        y = clean.astype(float).values
        x = np.arange(len(y), dtype=float)
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]

        fitted_start = coeffs[1]
        fitted_end = coeffs[0] * (len(y) - 1) + coeffs[1]

        if abs(fitted_start) < 1e-10:
            pct_change = 0.0
        else:
            pct_change = ((fitted_end - fitted_start) / abs(fitted_start)) * 100.0

        # Threshold: ±2% is "flat"
        if abs(pct_change) < 2.0:
            direction = "flat"
        elif slope > 0:
            direction = "up"
        else:
            direction = "down"

        return direction, round(pct_change, 1)

    except (ValueError, TypeError, np.linalg.LinAlgError) as exc:
        logger.debug("Trend detection failed: %s", exc)
        return "flat", 0.0


def _build_summary_text(insight: InsightSummary, chart_type: str) -> str:
    """Build a human-readable summary sentence."""
    parts: list[str] = []

    if insight.max_label is not None and insight.max_value is not None:
        parts.append(f"Highest: **{insight.max_label}** ({insight.max_value:,.2f})")

    if insight.min_label is not None and insight.min_value is not None:
        parts.append(f"Lowest: **{insight.min_label}** ({insight.min_value:,.2f})")

    if insight.trend_direction and insight.trend_direction != "flat":
        arrow = "📈" if insight.trend_direction == "up" else "📉"
        pct = insight.trend_pct_change or 0
        parts.append(f"Trend: {arrow} {insight.trend_direction} ({pct:+.1f}%)")
    elif insight.trend_direction == "flat":
        parts.append("Trend: ➡️ flat (no significant change)")

    return " · ".join(parts) if parts else ""


def _add_max_min_rule_layers(
    spec: Dict[str, Any],
    df: pd.DataFrame,
    numeric_col: str,
    label_col: Optional[str],
) -> tuple[Optional[str], Optional[float], Optional[str], Optional[float]]:
    """
    Add Vega-Lite rule + text annotation layers for max and min values.

    Only modifies the spec if it uses a "layer" or can be converted to one.

    Returns:
        (max_label, max_value, min_label, min_value)
    """
    if numeric_col not in df.columns:
        return None, None, None, None

    col_data = df[numeric_col].dropna()
    if col_data.empty:
        return None, None, None, None

    max_idx = col_data.idxmax()
    min_idx = col_data.idxmin()
    max_val = float(col_data.loc[max_idx])
    min_val = float(col_data.loc[min_idx])

    max_label_val = str(df.loc[max_idx, label_col]) if label_col and label_col in df.columns else str(max_idx)
    min_label_val = str(df.loc[min_idx, label_col]) if label_col and label_col in df.columns else str(min_idx)

    if max_val == min_val:
        return max_label_val, max_val, None, None

    # Build annotation layers (Vega-Lite rule marks)
    max_rule = {
        "mark": {"type": "rule", "color": "#ef4444", "strokeDash": [4, 4], "strokeWidth": 1.5},
        "encoding": {
            "y": {"datum": max_val, "type": "quantitative"},
        },
    }
    max_text = {
        "mark": {
            "type": "text",
            "align": "left",
            "dx": 5,
            "dy": -8,
            "fontSize": 11,
            "fontWeight": 600,
            "color": "#ef4444",
        },
        "encoding": {
            "y": {"datum": max_val, "type": "quantitative"},
            "text": {"value": f"Max: {max_val:,.0f}"},
        },
    }
    min_rule = {
        "mark": {"type": "rule", "color": "#3b82f6", "strokeDash": [4, 4], "strokeWidth": 1.5},
        "encoding": {
            "y": {"datum": min_val, "type": "quantitative"},
        },
    }
    min_text = {
        "mark": {
            "type": "text",
            "align": "left",
            "dx": 5,
            "dy": 12,
            "fontSize": 11,
            "fontWeight": 600,
            "color": "#3b82f6",
        },
        "encoding": {
            "y": {"datum": min_val, "type": "quantitative"},
            "text": {"value": f"Min: {min_val:,.0f}"},
        },
    }

    # Inject into spec — convert to layered spec if needed
    if "layer" in spec:
        spec["layer"].extend([max_rule, max_text, min_rule, min_text])
    elif "mark" in spec:
        # Convert single-mark spec to layered
        base_layer = {
            "mark": spec.pop("mark"),
            "encoding": spec.pop("encoding", {}),
        }
        if "selection" in spec:
            base_layer["selection"] = spec.pop("selection")
        if "params" in spec:
            base_layer["params"] = spec.pop("params")
        spec["layer"] = [base_layer, max_rule, max_text, min_rule, min_text]

    return max_label_val, max_val, min_label_val, min_val


# ── Main public function ─────────────────────────────────────────────────────

def annotate_insights(
    spec: Dict[str, Any],
    df: pd.DataFrame,
    chart_type: str,
    numeric_col: Optional[str] = None,
    label_col: Optional[str] = None,
) -> tuple[Dict[str, Any], InsightSummary]:
    """
    Add insight annotations to a Vega-Lite spec and generate a summary.

    Annotations added:
        - Max / min horizontal rule lines + text labels
        - Trend detection (for line / area charts)

    Args:
        spec:         Vega-Lite spec dict (mutated in-place).
        df:           The underlying DataFrame.
        chart_type:   The selected chart type string.
        numeric_col:  Name of the primary numeric column (auto-detected if None).
        label_col:    Name of the label / x-axis column (auto-detected if None).

    Returns:
        (spec, InsightSummary) — the annotated spec and a summary object.
    """
    insight = InsightSummary()

    if df is None or df.empty or chart_type in ("table", "kpi_card"):
        return spec, insight

    # Auto-detect columns if not provided
    num_cols = df.select_dtypes(include="number").columns.tolist()
    cat_cols = df.select_dtypes(include=["object", "category", "string"]).columns.tolist()

    if numeric_col is None and num_cols:
        numeric_col = num_cols[0]
    if label_col is None:
        if cat_cols:
            label_col = cat_cols[0]
        else:
            # Use first temporal-ish column name
            for c in df.columns:
                if c.lower() in ("month_year", "order_date", "date"):
                    label_col = c
                    break

    # ── Max / Min annotations ────────────────────────────────────────────
    if numeric_col and chart_type in (
        "bar_vertical", "bar_horizontal", "line", "multi_line", "area",
        "grouped_bar",
    ):
        max_lbl, max_val, min_lbl, min_val = _add_max_min_rule_layers(
            spec, df, numeric_col, label_col
        )
        insight.max_label = max_lbl
        insight.max_value = max_val
        insight.min_label = min_lbl
        insight.min_value = min_val

    # ── Trend detection ──────────────────────────────────────────────────
    if numeric_col and chart_type in ("line", "multi_line", "area"):
        direction, pct_change = _detect_trend(df[numeric_col])
        insight.trend_direction = direction
        insight.trend_pct_change = pct_change

    # ── Build summary text ───────────────────────────────────────────────
    insight.summary_text = _build_summary_text(insight, chart_type)

    logger.debug("Insights: %s", insight.summary_text or "(none)")
    return spec, insight


__all__ = ["annotate_insights", "InsightSummary"]