"""
charts/chart_spec_generator.py

Generate complete Vega-Lite JSON specifications from a DataFrame,
DataShape, and selected chart type.

Each chart type has a dedicated builder function that constructs the
spec programmatically (no template JSON files required, though the
templates/ directory can hold reference examples).

Public API:
    from charts.chart_spec_generator import generate_chart_spec
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from charts.data_shape_analyzer import DataShape
from charts.theme_engine import apply_theme, get_color_palette

logger = logging.getLogger(__name__)

# ── Common helpers ───────────────────────────────────────────────────────────

def _vl_base(
    title: str = "",
    width: int = 600,
    height: int = 380,
) -> Dict[str, Any]:
    """Return a base Vega-Lite spec dict."""
    spec: Dict[str, Any] = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "width": width,
        "height": height,
        "data": {"values": []},
    }
    if title:
        spec["title"] = title
    return spec


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DataFrame to list of dicts (Vega-Lite inline data format)."""
    # Handle NaN → None for JSON serialisation
    return df.where(df.notna(), None).to_dict(orient="records")


def _safe_col(shape: DataShape, role: str, idx: int = 0) -> Optional[str]:
    """Safely get a column name by role and index."""
    if role == "numeric":
        cols = shape.numeric_cols
    elif role == "temporal":
        cols = [shape.temporal_col] if shape.temporal_col else []
    elif role == "categorical":
        cols = shape.categorical_cols
    else:
        cols = []
    return cols[idx] if idx < len(cols) else None


def _auto_title(chart_type: str, cols: List[str]) -> str:
    """Generate a reasonable auto-title from chart type and column names."""
    readable = [c.replace("_", " ").title() for c in cols if c]
    if not readable:
        return chart_type.replace("_", " ").title()
    return " vs ".join(readable[:3])


# ── Chart type builders ──────────────────────────────────────────────────────

def _build_bar_vertical(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    cat = _safe_col(shape, "categorical") or df.columns[0]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    spec = _vl_base(title=_auto_title("bar_vertical", [cat, num]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "bar"}
    spec["encoding"] = {
        "x": {
            "field": cat,
            "type": "nominal",
            "sort": "-y",
            "axis": {"labelAngle": -30},
        },
        "y": {
            "field": num,
            "type": "quantitative",
            "title": num.replace("_", " ").title(),
        },
        "color": {"field": cat, "type": "nominal", "legend": None},
        "tooltip": [
            {"field": cat, "type": "nominal"},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_bar_horizontal(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    cat = _safe_col(shape, "categorical") or df.columns[0]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    n_cats = df[cat].nunique()
    spec = _vl_base(
        title=_auto_title("bar_horizontal", [cat, num]),
        height=max(380, n_cats * 24),
    )
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "bar"}
    spec["encoding"] = {
        "y": {
            "field": cat,
            "type": "nominal",
            "sort": "-x",
            "axis": {"labelLimit": 200},
        },
        "x": {
            "field": num,
            "type": "quantitative",
            "title": num.replace("_", " ").title(),
        },
        "color": {"field": cat, "type": "nominal", "legend": None},
        "tooltip": [
            {"field": cat, "type": "nominal"},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_line(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    temporal = _safe_col(shape, "temporal") or df.columns[0]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    spec = _vl_base(title=_auto_title("line", [temporal, num]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "line", "point": True}

    # Determine temporal type
    temporal_type = "temporal"
    if temporal == "month_year":
        temporal_type = "ordinal"

    spec["encoding"] = {
        "x": {
            "field": temporal,
            "type": temporal_type,
            "title": temporal.replace("_", " ").title(),
        },
        "y": {
            "field": num,
            "type": "quantitative",
            "title": num.replace("_", " ").title(),
        },
        "tooltip": [
            {"field": temporal, "type": temporal_type},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_multi_line(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    temporal = _safe_col(shape, "temporal") or df.columns[0]
    cat = _safe_col(shape, "categorical")
    num = _safe_col(shape, "numeric") or df.columns[-1]

    temporal_type = "temporal"
    if temporal == "month_year":
        temporal_type = "ordinal"

    spec = _vl_base(title=_auto_title("multi_line", [temporal, num]))
    spec["data"]["values"] = _df_to_records(df)

    if cat:
        # Grouped multi-line (colour by category)
        spec["mark"] = {"type": "line", "point": True}
        spec["encoding"] = {
            "x": {"field": temporal, "type": temporal_type},
            "y": {"field": num, "type": "quantitative"},
            "color": {"field": cat, "type": "nominal"},
            "tooltip": [
                {"field": temporal, "type": temporal_type},
                {"field": cat, "type": "nominal"},
                {"field": num, "type": "quantitative", "format": ",.2f"},
            ],
        }
    else:
        # Multiple numeric columns → fold into long format spec
        num_cols = shape.numeric_cols
        spec["transform"] = [
            {"fold": num_cols, "as": ["Metric", "Value"]}
        ]
        spec["mark"] = {"type": "line", "point": True}
        spec["encoding"] = {
            "x": {"field": temporal, "type": temporal_type},
            "y": {"field": "Value", "type": "quantitative"},
            "color": {"field": "Metric", "type": "nominal"},
            "tooltip": [
                {"field": temporal, "type": temporal_type},
                {"field": "Metric", "type": "nominal"},
                {"field": "Value", "type": "quantitative", "format": ",.2f"},
            ],
        }
    return spec


def _build_area(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    temporal = _safe_col(shape, "temporal") or df.columns[0]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    temporal_type = "temporal" if temporal != "month_year" else "ordinal"

    spec = _vl_base(title=_auto_title("area", [temporal, num]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "area", "line": True, "opacity": 0.35}
    spec["encoding"] = {
        "x": {"field": temporal, "type": temporal_type},
        "y": {"field": num, "type": "quantitative"},
        "tooltip": [
            {"field": temporal, "type": temporal_type},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_donut(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    cat = _safe_col(shape, "categorical") or df.columns[0]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    spec = _vl_base(title=_auto_title("donut", [cat, num]), width=400, height=400)
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "arc", "innerRadius": 55}
    spec["encoding"] = {
        "theta": {"field": num, "type": "quantitative", "stack": True},
        "color": {
            "field": cat,
            "type": "nominal",
            "legend": {"title": cat.replace("_", " ").title()},
        },
        "tooltip": [
            {"field": cat, "type": "nominal"},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_scatter(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    num_x = shape.numeric_cols[0] if len(shape.numeric_cols) > 0 else df.columns[0]
    num_y = shape.numeric_cols[1] if len(shape.numeric_cols) > 1 else df.columns[-1]
    spec = _vl_base(title=_auto_title("scatter", [num_x, num_y]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "point", "filled": True, "opacity": 0.7}
    encoding: Dict[str, Any] = {
        "x": {"field": num_x, "type": "quantitative"},
        "y": {"field": num_y, "type": "quantitative"},
        "tooltip": [
            {"field": num_x, "type": "quantitative", "format": ",.2f"},
            {"field": num_y, "type": "quantitative", "format": ",.2f"},
        ],
    }
    # Add color by first categorical if available
    cat = _safe_col(shape, "categorical")
    if cat:
        encoding["color"] = {"field": cat, "type": "nominal"}
        encoding["tooltip"].append({"field": cat, "type": "nominal"})
    spec["encoding"] = encoding
    return spec


def _build_bubble(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    num_x = shape.numeric_cols[0] if len(shape.numeric_cols) > 0 else df.columns[0]
    num_y = shape.numeric_cols[1] if len(shape.numeric_cols) > 1 else df.columns[1]
    num_size = shape.numeric_cols[2] if len(shape.numeric_cols) > 2 else num_y
    spec = _vl_base(title=_auto_title("bubble", [num_x, num_y, num_size]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "point", "filled": True, "opacity": 0.6}
    encoding: Dict[str, Any] = {
        "x": {"field": num_x, "type": "quantitative"},
        "y": {"field": num_y, "type": "quantitative"},
        "size": {"field": num_size, "type": "quantitative"},
        "tooltip": [
            {"field": num_x, "type": "quantitative", "format": ",.2f"},
            {"field": num_y, "type": "quantitative", "format": ",.2f"},
            {"field": num_size, "type": "quantitative", "format": ",.2f"},
        ],
    }
    cat = _safe_col(shape, "categorical")
    if cat:
        encoding["color"] = {"field": cat, "type": "nominal"}
    spec["encoding"] = encoding
    return spec


def _build_histogram(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    num = _safe_col(shape, "numeric") or df.columns[0]
    spec = _vl_base(title=_auto_title("histogram", [num]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "bar"}
    spec["encoding"] = {
        "x": {"bin": True, "field": num, "type": "quantitative"},
        "y": {"aggregate": "count", "type": "quantitative", "title": "Count"},
        "tooltip": [
            {"bin": True, "field": num, "type": "quantitative"},
            {"aggregate": "count", "type": "quantitative"},
        ],
    }
    return spec


def _build_heatmap(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    cat_x = shape.categorical_cols[0] if len(shape.categorical_cols) > 0 else df.columns[0]
    cat_y = shape.categorical_cols[1] if len(shape.categorical_cols) > 1 else df.columns[1]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    spec = _vl_base(title=_auto_title("heatmap", [cat_x, cat_y, num]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "rect"}
    spec["encoding"] = {
        "x": {"field": cat_x, "type": "nominal"},
        "y": {"field": cat_y, "type": "nominal"},
        "color": {
            "field": num,
            "type": "quantitative",
            "scale": {"scheme": "blues"},
            "title": num.replace("_", " ").title(),
        },
        "tooltip": [
            {"field": cat_x, "type": "nominal"},
            {"field": cat_y, "type": "nominal"},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_grouped_bar(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    cat_x = shape.categorical_cols[0] if len(shape.categorical_cols) > 0 else df.columns[0]
    cat_color = shape.categorical_cols[1] if len(shape.categorical_cols) > 1 else df.columns[1]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    spec = _vl_base(title=_auto_title("grouped_bar", [cat_x, cat_color, num]))
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "bar"}
    spec["encoding"] = {
        "x": {"field": cat_x, "type": "nominal"},
        "xOffset": {"field": cat_color, "type": "nominal"},
        "y": {"field": num, "type": "quantitative"},
        "color": {"field": cat_color, "type": "nominal"},
        "tooltip": [
            {"field": cat_x, "type": "nominal"},
            {"field": cat_color, "type": "nominal"},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_diverging_bar(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    cat = _safe_col(shape, "categorical") or df.columns[0]
    num = _safe_col(shape, "numeric") or df.columns[-1]
    spec = _vl_base(
        title=_auto_title("diverging_bar", [cat, num]),
        height=max(380, df[cat].nunique() * 24),
    )
    spec["data"]["values"] = _df_to_records(df)
    spec["mark"] = {"type": "bar"}
    spec["encoding"] = {
        "y": {"field": cat, "type": "nominal", "sort": "-x"},
        "x": {"field": num, "type": "quantitative"},
        "color": {
            "field": num,
            "type": "quantitative",
            "scale": {"scheme": "redblue", "domainMid": 0},
        },
        "tooltip": [
            {"field": cat, "type": "nominal"},
            {"field": num, "type": "quantitative", "format": ",.2f"},
        ],
    }
    return spec


def _build_kpi_card(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    """
    Build a KPI card spec.

    For single-cell results: large centered number.
    For single-row multi-column: side-by-side KPI tiles (using concat).
    """
    if df.shape == (1, 1):
        col_name = df.columns[0]
        value = df.iloc[0, 0]
        display_val = f"{value:,.2f}" if isinstance(value, (int, float)) else str(value)
        label = col_name.replace("_", " ").title()

        spec = _vl_base(title="", width=300, height=120)
        spec["data"] = {"values": [{"label": label, "value": display_val}]}
        spec["layer"] = [
            {
                "mark": {
                    "type": "text",
                    "fontSize": 42,
                    "fontWeight": "bold",
                    "color": "#1a1a2e",
                },
                "encoding": {
                    "text": {"field": "value", "type": "nominal"},
                },
            },
            {
                "mark": {
                    "type": "text",
                    "fontSize": 14,
                    "dy": 35,
                    "color": "#6b7280",
                },
                "encoding": {
                    "text": {"field": "label", "type": "nominal"},
                },
            },
        ]
        return spec

    # Multi-metric KPI: horizontal concat of mini cards
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        num_cols = list(df.columns)

    cards = []
    for col in num_cols:
        raw_val = df.iloc[0][col]
        display_val = f"{raw_val:,.2f}" if isinstance(raw_val, (int, float)) else str(raw_val)
        label = col.replace("_", " ").title()
        card = {
            "width": 180,
            "height": 100,
            "data": {"values": [{"label": label, "value": display_val}]},
            "layer": [
                {
                    "mark": {"type": "text", "fontSize": 32, "fontWeight": "bold", "color": "#1a1a2e"},
                    "encoding": {"text": {"field": "value", "type": "nominal"}},
                },
                {
                    "mark": {"type": "text", "fontSize": 12, "dy": 28, "color": "#6b7280"},
                    "encoding": {"text": {"field": "label", "type": "nominal"}},
                },
            ],
        }
        cards.append(card)

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "hconcat": cards,
    }
    return spec


def _build_table(df: pd.DataFrame, shape: DataShape) -> Dict[str, Any]:
    """
    Return a minimal spec placeholder.
    The actual table rendering is handled by Streamlit's st.dataframe().
    This spec is returned for consistency in the pipeline.
    """
    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "_chart_type": "table",
        "_message": "Render using st.dataframe() — Vega-Lite table not used.",
        "data": {"values": _df_to_records(df.head(100))},
    }


# ── Builder registry ────────────────────────────────────────────────────────
_BUILDERS = {
    "bar_vertical": _build_bar_vertical,
    "bar_horizontal": _build_bar_horizontal,
    "line": _build_line,
    "multi_line": _build_multi_line,
    "area": _build_area,
    "donut": _build_donut,
    "pie": _build_donut,  # alias
    "scatter": _build_scatter,
    "bubble": _build_bubble,
    "histogram": _build_histogram,
    "heatmap": _build_heatmap,
    "grouped_bar": _build_grouped_bar,
    "diverging_bar": _build_diverging_bar,
    "kpi_card": _build_kpi_card,
    "table": _build_table,
}


# ── Main public function ─────────────────────────────────────────────────────

def generate_chart_spec(
    df: pd.DataFrame,
    shape: DataShape,
    chart_type: str,
) -> Dict[str, Any]:
    """
    Generate a themed Vega-Lite specification.

    Args:
        df:         The query result DataFrame.
        shape:      DataShape from data_shape_analyzer.
        chart_type: Chart type string from chart_type_selector.

    Returns:
        A complete Vega-Lite JSON-serialisable dict, with theme applied.
    """
    builder = _BUILDERS.get(chart_type, _build_table)
    if chart_type not in _BUILDERS:
        logger.warning(
            "Unknown chart type '%s' — falling back to table.", chart_type
        )

    try:
        spec = builder(df, shape)
    except Exception as exc:
        logger.error(
            "Chart spec generation failed for '%s': %s — falling back to table.",
            chart_type, exc, exc_info=True,
        )
        spec = _build_table(df, shape)

    # Apply theme (skip for table placeholders)
    if chart_type != "table":
        spec = apply_theme(spec)

    logger.debug("Generated Vega-Lite spec for chart_type='%s'.", chart_type)
    return spec


__all__ = ["generate_chart_spec"]