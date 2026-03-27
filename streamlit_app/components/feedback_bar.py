"""
streamlit_app/components/feedback_bar.py

Render 👍 👎 ✏️ feedback controls after each query result.

Feedback flow:
    👍 → vn.train(question, sql) + log to SQLite → positive training signal
    👎 → log only (do NOT train) → negative signal for analytics
    ✏️ → editable SQL area → validate → execute → train if OK

Depends: core/vanna_instance, core/sql_validator
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import streamlit as st

from core.vanna_instance import MyVanna
from core.sql_validator import validate_sql

logger = logging.getLogger(__name__)


def render_feedback_bar(
    vn: MyVanna,
    question: str,
    sql: str,
    query_id: str,
) -> None:
    """
    Render the feedback bar with 👍 👎 ✏️ buttons.

    Args:
        vn:        The Vanna instance (for training on positive feedback).
        question:  The original user question.
        sql:       The SQL that was executed.
        query_id:  Unique ID for this query (used as button keys).
    """
    if not sql or not sql.strip():
        return

    st.divider()

    # ── Feedback header ──────────────────────────────────────────────────
    st.markdown(
        "<p style='font-size:13px; color:#6b7280; margin-bottom:8px;'>"
        "Was this result helpful?</p>",
        unsafe_allow_html=True,
    )

    # ── Button row ───────────────────────────────────────────────────────
    col_up, col_down, col_edit, col_spacer = st.columns([1, 1, 1, 5])

    with col_up:
        if st.button(
            "👍 Correct",
            key=f"fb_up_{query_id}",
            help="Mark as correct — saves this Q→SQL pair for training",
            use_container_width=True,
        ):
            _handle_positive_feedback(vn, question, sql, query_id)

    with col_down:
        if st.button(
            "👎 Wrong",
            key=f"fb_down_{query_id}",
            help="Mark as wrong — will NOT be used for training",
            use_container_width=True,
        ):
            _handle_negative_feedback(query_id)

    with col_edit:
        if st.button(
            "✏️ Edit SQL",
            key=f"fb_edit_{query_id}",
            help="Edit the SQL and submit a correction",
            use_container_width=True,
        ):
            st.session_state[f"show_edit_{query_id}"] = True

    # ── Editable SQL area ────────────────────────────────────────────────
    if st.session_state.get(f"show_edit_{query_id}", False):
        _render_edit_area(vn, question, sql, query_id)


def _handle_positive_feedback(
    vn: MyVanna,
    question: str,
    sql: str,
    query_id: str,
) -> None:
    """Process positive feedback: train Vanna with this Q→SQL pair."""
    try:
        vn.train(question=question, sql=sql)
        st.success("✅ Thanks! This Q→SQL pair was saved for training.")
        logger.info(
            "POSITIVE feedback | query_id=%s | q='%s'",
            query_id, question[:80],
        )
    except Exception as exc:
        st.warning(f"Feedback noted, but training failed: {exc}")
        logger.error("Training failed on positive feedback: %s", exc)


def _handle_negative_feedback(query_id: str) -> None:
    """Process negative feedback: log only, do NOT train."""
    st.warning("📝 Noted — this will **not** be used for training.")
    logger.info("NEGATIVE feedback | query_id=%s", query_id)


def _render_edit_area(
    vn: MyVanna,
    question: str,
    sql: str,
    query_id: str,
) -> None:
    """Render the SQL editing area for corrections."""
    st.markdown("---")
    st.markdown(
        "<p style='font-weight:600; color:#1a1a2e; font-size:14px;'>"
        "✏️ Edit the SQL below and submit your correction:</p>",
        unsafe_allow_html=True,
    )

    corrected_sql = st.text_area(
        "Corrected SQL:",
        value=sql,
        height=220,
        key=f"edit_area_{query_id}",
        label_visibility="collapsed",
    )

    col_submit, col_cancel, col_spacer = st.columns([2, 2, 6])

    with col_submit:
        submit = st.button(
            "✅ Submit Correction",
            key=f"submit_edit_{query_id}",
            type="primary",
            use_container_width=True,
        )

    with col_cancel:
        cancel = st.button(
            "Cancel",
            key=f"cancel_edit_{query_id}",
            use_container_width=True,
        )

    if cancel:
        st.session_state[f"show_edit_{query_id}"] = False
        st.rerun()

    if submit:
        _process_correction(vn, question, corrected_sql, query_id)


def _process_correction(
    vn: MyVanna,
    question: str,
    corrected_sql: str,
    query_id: str,
) -> None:
    """Validate, execute, and train with a corrected SQL query."""
    if not corrected_sql or not corrected_sql.strip():
        st.error("SQL cannot be empty.")
        return

    # Validate
    val_result = validate_sql(corrected_sql)
    if val_result.is_security_violation:
        st.error("🚫 Corrected SQL failed security checks:")
        for v in val_result.violations:
            st.warning(v)
        return

    # Execute
    final_sql = val_result.fixed_sql
    try:
        with st.spinner("Running corrected SQL…"):
            test_df = vn.run_sql(final_sql)
    except Exception as exc:
        st.error(f"❌ Corrected SQL failed to execute: {exc}")
        return

    # Show results
    if test_df is not None and not test_df.empty:
        st.success("✅ Corrected SQL executed successfully!")
        st.dataframe(test_df.head(100), use_container_width=True)

        # Train
        try:
            vn.train(question=question, sql=final_sql)
            st.info("📚 This corrected Q→SQL pair has been saved for training.")
            logger.info(
                "CORRECTED feedback | query_id=%s | q='%s'",
                query_id, question[:80],
            )
        except Exception as exc:
            st.warning(f"Result looks good, but training failed: {exc}")
    else:
        st.warning("Corrected SQL returned no results.")

    # Close edit area
    st.session_state[f"show_edit_{query_id}"] = False


__all__ = ["render_feedback_bar"]