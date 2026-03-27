"""
streamlit_app/components/kpi_card.py

Render KPI cards — single-value and multi-metric displays.

Design:
    - Glassmorphism-inspired cards with subtle shadows
    - Large bold numbers with proper formatting (₹, commas, Cr/L)
    - Muted labels below values
    - Responsive grid for multi-metric cards
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


def _format_value(value: Any, col_name: str = "") -> str:
    """
    Format a numeric value for KPI display.

    Rules:
        - Currency columns (MRP, revenue, price, etc.) → ₹ prefix
        - Large numbers → abbreviated (Cr, L, K)
        - Percentages → % suffix (if col name hints at it)
        - Integers → no decimals
        - Floats → 2 decimal places
    """
    if pd.isna(value):
        return "N/A"

    col_lower = col_name.lower() if col_name else ""

    try:
        v = float(value)
    except (ValueError, TypeError):
        return str(value)

    # Check if percentage
    is_pct = any(kw in col_lower for kw in ("rate", "pct", "percent", "percentage", "ratio"))
    if is_pct:
        return f"{v:.1f}%"

    # Check if currency
    is_currency = any(
        kw in col_lower
        for kw in ("mrp", "revenue", "price", "amount", "value", "asp", "aov",
                    "sales", "cost", "total_revenue", "net_revenue")
    )

    abs_v = abs(v)
    prefix = "₹" if is_currency else ""

    if abs_v >= 1_00_00_000:  # 1 Crore
        return f"{prefix}{v / 1_00_00_000:,.2f} Cr"
    elif abs_v >= 1_00_000:  # 1 Lakh
        return f"{prefix}{v / 1_00_000:,.2f} L"
    elif abs_v >= 1_000:
        if v == int(v):
            return f"{prefix}{int(v):,}"
        return f"{prefix}{v:,.2f}"
    else:
        if v == int(v):
            return f"{prefix}{int(v):,}"
        return f"{prefix}{v:,.2f}"


# ── Single KPI card HTML ────────────────────────────────────────────────────
_SINGLE_KPI_HTML = """
<div style="
    display: flex;
    justify-content: center;
    padding: 12px 0;
">
    <div style="
        text-align: center;
        padding: 32px 48px;
        background: linear-gradient(135deg, #f8fafc 0%, #ffffff 100%);
        border-radius: 16px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05),
                    0 2px 4px -2px rgba(0, 0, 0, 0.03);
        min-width: 240px;
    ">
        <div style="
            font-size: 48px;
            font-weight: 700;
            color: #1a1a2e;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            line-height: 1.2;
            letter-spacing: -0.02em;
        ">{value}</div>
        <div style="
            font-size: 14px;
            font-weight: 500;
            color: #9ca3af;
            margin-top: 8px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        ">{label}</div>
    </div>
</div>
"""

# ── Multi-metric card HTML ──────────────────────────────────────────────────
_MULTI_KPI_CARD_HTML = """
<div style="
    text-align: center;
    padding: 24px 16px;
    background: linear-gradient(135deg, {bg_start} 0%, #ffffff 100%);
    border-radius: 14px;
    border: 1px solid #e2e8f0;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04);
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
">
    <div style="
        font-size: {font_size};
        font-weight: 700;
        color: #1a1a2e;
        font-family: 'Inter', sans-serif;
        line-height: 1.2;
        letter-spacing: -0.01em;
    ">{value}</div>
    <div style="
        font-size: 12px;
        font-weight: 500;
        color: #9ca3af;
        margin-top: 6px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    ">{label}</div>
</div>
"""

# Subtle background tints for multi-card variety
_CARD_BG_TINTS = [
    "#f8fafc",  # slate
    "#fef3c7",  # amber
    "#d1fae5",  # emerald
    "#ede9fe",  # violet
    "#fce7f3",  # pink
    "#e0f2fe",  # sky
    "#fef9c3",  # yellow
    "#f1f5f9",  # gray
]


def render_kpi_card(
    df: Optional[pd.DataFrame],
    edge_result: Any = None,
) -> None:
    """
    Render KPI card(s) based on the DataFrame.

    Handles:
        - Single cell (1×1) → one large centered card
        - Single row, multiple numeric cols → grid of cards
        - edge_result.kpi_value/kpi_label shortcut

    Args:
        df:          The query result DataFrame.
        edge_result: EdgeCaseResult from edge_case_handler (optional).
    """
    # ── Fast path from edge case handler ─────────────────────────────────
    if edge_result and edge_result.kpi_value:
        html = _SINGLE_KPI_HTML.format(
            value=edge_result.kpi_value,
            label=edge_result.kpi_label or "",
        )
        st.markdown(html, unsafe_allow_html=True)
        return

    # ── Validate DataFrame ───────────────────────────────────────────────
    if df is None or df.empty:
        st.info("No data to display.")
        return

    # ── Single cell → big KPI ────────────────────────────────────────────
    if df.shape == (1, 1):
        col_name = str(df.columns[0])
        raw_value = df.iloc[0, 0]
        formatted = _format_value(raw_value, col_name)
        label = col_name.replace("_", " ").title()

        html = _SINGLE_KPI_HTML.format(value=formatted, label=label)
        st.markdown(html, unsafe_allow_html=True)
        return

    # ── Single row, multiple columns → multi-card grid ───────────────────
    if df.shape[0] == 1:
        num_cols = df.select_dtypes(include="number").columns.tolist()
        if not num_cols:
            num_cols = list(df.columns)

        # Determine grid layout (max 4 per row)
        n_cards = len(num_cols)
        n_per_row = min(n_cards, 4)
        font_size = "36px" if n_cards <= 2 else "28px" if n_cards <= 4 else "24px"

        cols = st.columns(n_per_row)

        for i, col_name in enumerate(num_cols):
            raw_value = df.iloc[0][col_name]
            formatted = _format_value(raw_value, str(col_name))
            label = str(col_name).replace("_", " ").title()
            bg_tint = _CARD_BG_TINTS[i % len(_CARD_BG_TINTS)]

            html = _MULTI_KPI_CARD_HTML.format(
                value=formatted,
                label=label,
                font_size=font_size,
                bg_start=bg_tint,
            )

            with cols[i % n_per_row]:
                st.markdown(html, unsafe_allow_html=True)

        return

    # ── Fallback: shouldn't reach here, but handle gracefully ────────────
    st.dataframe(df, use_container_width=True)


def render_kpi_metric(
    value: Any,
    label: str,
    delta: Optional[str] = None,
    delta_color: str = "normal",
) -> None:
    """
    Render a single KPI using Streamlit's native st.metric.

    Simpler alternative to the HTML cards — useful for quick inline metrics.
    """
    formatted = _format_value(value, label)
    st.metric(label=label, value=formatted, delta=delta, delta_color=delta_color)


__all__ = ["render_kpi_card", "render_kpi_metric"]