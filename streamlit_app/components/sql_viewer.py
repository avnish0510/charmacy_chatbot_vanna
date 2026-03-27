"""
streamlit_app/components/sql_viewer.py

Render SQL in a collapsible, syntax-highlighted viewer.

Features:
    - Collapsible expander (closed by default)
    - Copy-friendly code block
    - Auto-fix indicator when SQL was modified by validator
    - Optional diff view (original vs fixed)
"""

from __future__ import annotations

import logging
from typing import Optional

import streamlit as st

logger = logging.getLogger(__name__)


def render_sql_viewer(
    sql: str,
    title: str = "🔍 View SQL",
    expanded: bool = False,
    original_sql: Optional[str] = None,
    auto_fixed: bool = False,
) -> None:
    """
    Display SQL in a collapsible expander with syntax highlighting.

    Args:
        sql:           The SQL to display (may be auto-fixed version).
        title:         Expander title text.
        expanded:      Whether the expander starts open.
        original_sql:  If provided AND different from sql, show a diff note.
        auto_fixed:    If True, show a subtle indicator that SQL was modified.
    """
    if not sql or not sql.strip():
        return

    # Build title with indicator
    display_title = title
    if auto_fixed:
        display_title += " *(auto-corrected)*"

    with st.expander(display_title, expanded=expanded):
        # Show auto-fix notice
        if auto_fixed:
            st.caption(
                "ℹ️ SQL was automatically modified by the validator "
                "(e.g. `TOP 1000` added to unbounded `SELECT *`)."
            )

        # Main SQL display
        st.code(sql.strip(), language="sql")

        # Show original if different
        if original_sql and original_sql.strip() != sql.strip():
            with st.expander("📝 Original SQL (before auto-fix)", expanded=False):
                st.code(original_sql.strip(), language="sql")

        # Copy button (Streamlit's code block has built-in copy,
        # but we add a manual option for older versions)
        col1, col2 = st.columns([6, 1])
        with col2:
            if st.button("📋", key=f"copy_sql_{hash(sql)}", help="Copy SQL"):
                st.toast("SQL copied to clipboard area above ☝️")


def render_sql_error(
    sql: str,
    errors: list[str],
    title: str = "❌ Failed SQL",
) -> None:
    """
    Display a failed SQL query with associated error messages.

    Args:
        sql:    The SQL that failed.
        errors: List of error/violation messages.
        title:  Expander title.
    """
    with st.expander(title, expanded=True):
        st.code(sql.strip(), language="sql")

        if errors:
            st.markdown("**Errors:**")
            for i, err in enumerate(errors, 1):
                st.warning(f"{i}. {err}")


__all__ = ["render_sql_viewer", "render_sql_error"]