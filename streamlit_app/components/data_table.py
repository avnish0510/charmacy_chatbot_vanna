"""
streamlit_app/components/data_table.py

Render pandas DataFrames as beautiful, formatted Streamlit tables.

Features:
    - Auto-format numeric columns (commas, 2 decimal places)
    - Auto-format date columns
    - Row count display
    - Compact mode for inline use
    - Download button (CSV export)
    - Styled container with custom CSS
"""

from __future__ import annotations

import logging
from io import BytesIO
from typing import Optional

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

# ── Custom CSS for tables ────────────────────────────────────────────────────
_TABLE_CSS = """
<style>
    /* Streamlit dataframe styling overrides */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
    .stDataFrame [data-testid="stDataFrameResizable"] {
        border: 1px solid #e2e8f0;
        border-radius: 8px;
    }
</style>
"""


def _build_column_config(df: pd.DataFrame) -> dict:
    """
    Build st.column_config for auto-formatting.

    Numeric columns → comma-separated with 2 decimals.
    Date columns → formatted date string.
    """
    config = {}

    for col in df.columns:
        col_label = str(col).replace("_", " ").title()

        if pd.api.types.is_float_dtype(df[col]):
            # Check if values look like currency (MRP, revenue, price)
            col_lower = str(col).lower()
            is_currency = any(
                kw in col_lower
                for kw in ("mrp", "revenue", "price", "amount", "value", "asp", "aov")
            )
            if is_currency:
                config[col] = st.column_config.NumberColumn(
                    label=col_label,
                    format="₹%.2f",
                )
            else:
                config[col] = st.column_config.NumberColumn(
                    label=col_label,
                    format="%.2f",
                )

        elif pd.api.types.is_integer_dtype(df[col]):
            col_lower = str(col).lower()
            is_currency = any(
                kw in col_lower
                for kw in ("mrp", "revenue", "price", "amount")
            )
            if is_currency:
                config[col] = st.column_config.NumberColumn(
                    label=col_label,
                    format="₹%d",
                )
            else:
                config[col] = st.column_config.NumberColumn(
                    label=col_label,
                    format="%d",
                )

        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            config[col] = st.column_config.DateColumn(
                label=col_label,
                format="DD MMM YYYY",
            )

        else:
            config[col] = st.column_config.TextColumn(
                label=col_label,
            )

    return config


def render_data_table(
    df: Optional[pd.DataFrame],
    title: str = "📊 Data",
    max_rows: int = 500,
    show_row_count: bool = True,
    show_download: bool = True,
    compact: bool = False,
    use_container_width: bool = True,
    key_suffix: str = "",
) -> None:
    """
    Render a pandas DataFrame as a formatted Streamlit table.

    Args:
        df:                  The DataFrame to render.
        title:               Header text above the table.
        max_rows:            Maximum rows to display (full data still downloadable).
        show_row_count:      Show "N rows × M columns" caption.
        show_download:       Show CSV download button.
        compact:             If True, use minimal styling (for inline use).
        use_container_width: Stretch table to full container width.
    """
    if df is None or df.empty:
        if title:
            st.markdown(f"**{title}**")
        st.info("No data to display.")
        return

    # Inject custom CSS
    if not compact:
        st.markdown(_TABLE_CSS, unsafe_allow_html=True)

    # Title
    if title:
        st.markdown(
            f"<p style='font-weight:600; color:#1a1a2e; font-size:15px; "
            f"margin-bottom:4px;'>{title}</p>",
            unsafe_allow_html=True,
        )

    # Row count
    if show_row_count:
        total_rows = len(df)
        display_rows = min(total_rows, max_rows)
        if total_rows > max_rows:
            st.caption(
                f"Showing {display_rows:,} of {total_rows:,} rows · "
                f"{len(df.columns)} columns"
            )
        else:
            st.caption(f"{total_rows:,} rows · {len(df.columns)} columns")

    # Build column config
    column_config = _build_column_config(df)

    # Display
    display_df = df.head(max_rows)
    st.dataframe(
        display_df,
        column_config=column_config,
        use_container_width=use_container_width,
        hide_index=True,
    )

    # Download button
    if show_download and not compact:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")
        csv_bytes = csv_buffer.getvalue()

        download_key = (
            f"download_{hash(str(df.columns.tolist()))}_{len(df)}_{key_suffix}"
            if key_suffix else
            f"download_{hash(str(df.columns.tolist()))}_{len(df)}"
        )

        st.download_button(
            label="📥 Download CSV",
            data=csv_bytes,
            file_name="query_results.csv",
            mime="text/csv",
            # key=f"download_{hash(str(df.columns.tolist()))}_{len(df)}",
            key=download_key,
        )


__all__ = ["render_data_table"]