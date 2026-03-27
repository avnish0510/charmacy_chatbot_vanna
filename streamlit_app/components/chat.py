"""
streamlit_app/components/chat.py

Chat interface component — renders the conversation thread and handles
the full query pipeline for each user message.

Used by:  streamlit_app/pages/01_chat.py
Depends:  core/vanna_instance, core/sql_validator, core/error_recovery,
          charts/*, streamlit_app/components/*
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from core.vanna_instance import MyVanna
from core.sql_validator import validate_sql
# from core.error_recovery import error_recovery
from core.error_recovery import run_with_recovery, RecoveryResult

from charts.edge_case_handler import handle_edge_cases
from charts.data_shape_analyzer import analyze_data_shape
from charts.chart_type_selector import select_chart_type
from charts.chart_spec_generator import generate_chart_spec
from charts.insight_annotator import annotate_insights

from streamlit_app.components.chart_renderer import render_chart
from streamlit_app.components.sql_viewer import render_sql_viewer
from streamlit_app.components.data_table import render_data_table
from streamlit_app.components.feedback_bar import render_feedback_bar
from streamlit_app.components.kpi_card import render_kpi_card

logger = logging.getLogger(__name__)

# ── Chart hint extraction ────────────────────────────────────────────────────
_CHART_HINT_PATTERN = re.compile(
    r"(?:show\s+(?:as|in)\s+(?:a\s+)?|display\s+(?:as|in)\s+(?:a\s+)?|"
    r"as\s+(?:a\s+)?|in\s+(?:a\s+)?|use\s+(?:a\s+)?)"
    r"(bar\s*chart|line\s*chart|pie\s*chart|donut\s*chart|scatter\s*plot|"
    r"bubble\s*chart|heatmap|heat\s*map|histogram|area\s*chart|table|"
    r"kpi|card|bar|line|pie|donut|scatter|bubble|area|"
    r"horizontal\s*bar|vertical\s*bar|grouped\s*bar|multi[- ]?line|diverging)",
    re.IGNORECASE,
)


def _extract_chart_hint(question: str) -> str:
    """Extract chart type hint from the user's question."""
    match = _CHART_HINT_PATTERN.search(question)
    return match.group(1).strip().lower() if match else ""


def _clean_question_for_sql(question: str) -> str:
    """Remove chart hint phrases so they don't confuse the LLM."""
    cleaned = _CHART_HINT_PATTERN.sub("", question).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.")
    return cleaned if cleaned else question


# ── Session state defaults ───────────────────────────────────────────────────
SESSION_DEFAULTS: Dict[str, Any] = {
    "vn": None,
    "messages": [],
    "last_sql": "",
    "last_df": None,
    "last_question": "",
    "last_query_id": "",
    "last_chart_spec": None,
}


def init_chat_session_state() -> None:
    """Initialise all session state keys used by the chat pipeline."""
    for key, default in SESSION_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default


def clear_chat_history() -> None:
    """Reset the chat thread and associated state."""
    st.session_state["messages"] = []
    st.session_state["last_sql"] = ""
    st.session_state["last_df"] = None
    st.session_state["last_question"] = ""
    st.session_state["last_query_id"] = ""
    st.session_state["last_chart_spec"] = None


def render_chat_history() -> None:
    """Display all previous messages in the chat thread."""
    for msg in st.session_state.get("messages", []):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])


# # ── Main query pipeline ─────────────────────────────────────────────────────

# def run_query_pipeline(vn: MyVanna, question: str) -> None:
#     """
#     Execute the full text-to-SQL → chart pipeline.

#     Steps:
#         1. Preprocess (extract chart hint, clean question)
#         2. Generate SQL via Vanna
#         3. Validate SQL (security + sanity)
#         4. Error recovery if needed
#         5. Execute SQL
#         6. Edge case handling
#         7. Data shape → chart type → spec generation
#         8. Insight annotations
#         9. Render (KPI / chart / table)
#         10. Feedback bar
#     """
#     query_id = str(uuid.uuid4())
#     st.session_state["last_query_id"] = query_id
#     st.session_state["last_question"] = question

#     # ── 1. Preprocess ────────────────────────────────────────────────────
#     chart_hint = _extract_chart_hint(question)
#     clean_question = _clean_question_for_sql(question)
#     if chart_hint:
#         logger.info("Chart hint detected: '%s'", chart_hint)

#     # ── 2. Generate SQL ──────────────────────────────────────────────────
#     with st.spinner("🧠 Generating SQL…"):
#         try:
#             sql = vn.generate_sql(question=clean_question)
#         except Exception as exc:
#             st.error(f"❌ SQL generation failed: {exc}")
#             logger.error("SQL generation error: %s", exc, exc_info=True)
#             return

#     if not sql or not sql.strip():
#         st.error("❌ Empty SQL returned. Please rephrase your question.")
#         return

#     st.session_state["last_sql"] = sql

#     # ── 3. Validate SQL ──────────────────────────────────────────────────
#     val_result = validate_sql(sql)

#     if val_result.is_security_violation:
#         st.error("🚫 **Security violation** — query blocked.")
#         for v in val_result.violations:
#             st.warning(v)
#         logger.warning("Security violation for query_id=%s", query_id)
#         return

#     # ── 4. Error recovery for sanity issues ──────────────────────────────
#     df: Optional[pd.DataFrame] = None

#     if val_result.is_sanity_violation:
#         st.warning("⚠️ SQL had issues — attempting auto-correction…")
#         try:
#             sql, df = error_recovery(
#                 vn=vn,
#                 question=clean_question,
#                 initial_sql=sql,
#                 initial_errors=val_result.violations,
#                 max_retries=2,
#             )
#         except Exception as exc:
#             st.error(f"❌ Error recovery failed: {exc}")
#             _show_failure(sql, val_result.violations)
#             return
#     else:
#         sql = val_result.fixed_sql

#     st.session_state["last_sql"] = sql

#     # ── 5. Execute SQL ───────────────────────────────────────────────────
#     if df is None:
#         with st.spinner("⚡ Running query…"):
#             try:
#                 df = vn.run_sql(sql)
#             except Exception as exc:
#                 st.warning("⚠️ Execution failed — attempting recovery…")
#                 try:
#                     sql, df = error_recovery(
#                         vn=vn,
#                         question=clean_question,
#                         initial_sql=sql,
#                         initial_errors=[str(exc)],
#                         max_retries=2,
#                     )
#                     st.session_state["last_sql"] = sql
#                 except Exception as retry_exc:
#                     st.error(f"❌ All attempts failed: {retry_exc}")
#                     _show_failure(sql, [str(exc)])
#                     return

#     if df is not None:
#         df = df.head(10_000)
#     st.session_state["last_df"] = df

#     # ── 6. Show SQL ──────────────────────────────────────────────────────
#     render_sql_viewer(sql)

#     # ── 7. Edge case handling ────────────────────────────────────────────
#     edge = handle_edge_cases(df, sql)

#     if edge.warning_message:
#         st.info(edge.warning_message)

#     if edge.should_stop:
#         return

#     working_df = edge.df if edge.df is not None else df

#     # ── 8. Chart pipeline ────────────────────────────────────────────────
#     force_type = edge.force_chart_type
#     if force_type:
#         chart_type = force_type
#         shape = analyze_data_shape(working_df, question)
#     else:
#         shape = analyze_data_shape(working_df, question)
#         chart_type = select_chart_type(shape, chart_hint=chart_hint)

#     # ── 9. Render ────────────────────────────────────────────────────────
#     if chart_type == "kpi_card":
#         render_kpi_card(working_df, edge)

#     elif chart_type == "table":
#         render_data_table(working_df, title="Query Results")

#     else:
#         spec = generate_chart_spec(working_df, shape, chart_type)

#         num_col = shape.numeric_cols[0] if shape.numeric_cols else None
#         label_col = (
#             shape.categorical_cols[0] if shape.categorical_cols
#             else shape.temporal_col
#         )

#         spec, insights = annotate_insights(
#             spec, working_df, chart_type,
#             numeric_col=num_col, label_col=label_col,
#         )

#         st.session_state["last_chart_spec"] = spec

#         render_chart(spec, chart_type=chart_type)

#         if insights.summary_text:
#             st.caption(f"💡 {insights.summary_text}")

#         with st.expander("📊 View Data", expanded=False):
#             render_data_table(working_df, title="")

#     # ── 10. Feedback ─────────────────────────────────────────────────────
#     render_feedback_bar(vn, question, sql, query_id)



def run_query_pipeline(vn: MyVanna, question: str) -> None:
    """
    Execute the full text-to-SQL → chart pipeline.

    Steps:
        1. Preprocess (extract chart hint, clean question)
        2–5. Generate → Validate → Execute via run_with_recovery (with retries)
        6. Show SQL viewer
        7. Edge case handling
        8. Data shape → chart type → spec generation
        9. Insight annotations
        10. Render (KPI / chart / table)
        11. Feedback bar
    """
    query_id = str(uuid.uuid4())
    st.session_state["last_query_id"] = query_id
    st.session_state["last_question"] = question

    # ── 1. Preprocess ────────────────────────────────────────────────────
    chart_hint = _extract_chart_hint(question)
    clean_question = _clean_question_for_sql(question)
    if chart_hint:
        logger.info("Chart hint detected: '%s'", chart_hint)

    # ── 2–5. Generate → Validate → Execute (with auto-retry) ─────────────
    #
    # run_with_recovery() owns the entire generate/validate/execute cycle
    # including all retries and correction-context building.
    # It never raises — all errors are captured inside RecoveryResult.
    # ─────────────────────────────────────────────────────────────────────
    with st.spinner("🧠 Generating SQL and running query…"):
        result: RecoveryResult = run_with_recovery(vn, clean_question)

    # ── Security violation → hard stop ───────────────────────────────────
    if result.is_security:
        st.error("🚫 **Security violation** — query blocked.")
        if result.last_attempt and result.last_attempt.validation_result:
            for v in result.last_attempt.validation_result.violations:
                st.warning(v)
        logger.warning("Security violation for query_id=%s", query_id)
        return

    # ── All retries exhausted ─────────────────────────────────────────────
    if not result.success:
        st.error(result.user_facing_error())
        return

    # ── Pipeline succeeded ────────────────────────────────────────────────
    sql = result.sql
    df  = result.df

    if df is not None:
        df = df.head(10_000)

    st.session_state["last_sql"] = sql
    st.session_state["last_df"] = df

    # ── 6. Show SQL ──────────────────────────────────────────────────────
    render_sql_viewer(sql)

    # ── 7. Edge case handling ────────────────────────────────────────────
    edge = handle_edge_cases(df, sql)

    if edge.warning_message:
        st.info(edge.warning_message)

    if edge.should_stop:
        return

    working_df = edge.df if edge.df is not None else df

    # ── 8. Chart pipeline ────────────────────────────────────────────────
    force_type = edge.force_chart_type
    if force_type:
        chart_type = force_type
        shape = analyze_data_shape(working_df, question)
    else:
        shape = analyze_data_shape(working_df, question)
        chart_type = select_chart_type(shape, chart_hint=chart_hint)

    # ── 9 & 10. Render ───────────────────────────────────────────────────
    if chart_type == "kpi_card":
        render_kpi_card(working_df, edge)

    elif chart_type == "table":
        render_data_table(working_df, title="Query Results")

    else:
        spec = generate_chart_spec(working_df, shape, chart_type)

        num_col = shape.numeric_cols[0] if shape.numeric_cols else None
        label_col = (
            shape.categorical_cols[0] if shape.categorical_cols
            else shape.temporal_col
        )

        spec, insights = annotate_insights(
            spec, working_df, chart_type,
            numeric_col=num_col, label_col=label_col,
        )

        st.session_state["last_chart_spec"] = spec

        render_chart(spec, chart_type=chart_type)

        if insights.summary_text:
            st.caption(f"💡 {insights.summary_text}")

        with st.expander("📊 View Data", expanded=False):
            render_data_table(working_df, title="")

    # ── 11. Feedback ─────────────────────────────────────────────────────
    render_feedback_bar(vn, question, sql, query_id)



# def _show_failure(sql: str, errors: list[str]) -> None:
#     """Show failed SQL and error messages."""
#     st.error("❌ Could not generate a valid query after all retries.")
#     with st.expander("Failed SQL", expanded=True):
#         st.code(sql, language="sql")
#     for err in errors:
#         st.warning(err)


# ── Sidebar ──────────────────────────────────────────────────────────────────

_SAMPLE_QUESTIONS = [
    "What is the total revenue by platform?",
    "Show monthly revenue trend",
    "Top 10 products by units sold",
    "Revenue by state as a bar chart",
    "What is the cancellation rate?",
    "Compare B2B vs B2C revenue",
    "Top 5 article types by revenue as donut",
    "Revenue trend for Amazon",
    "Which states have highest orders?",
    "Average selling price by platform",
]


def render_sidebar(vn: MyVanna) -> None:
    """Render the sidebar with training status, sample questions, and controls."""
    with st.sidebar:
        # ── Logo / branding ──────────────────────────────────────────────
        st.markdown(
            """
            <div style="text-align:center; padding:8px 0 16px 0;">
                <span style="font-size:28px;">💄</span>
                <h2 style="margin:0; color:#1a1a2e; font-weight:700;">
                    Charmacy Milano
                </h2>
                <p style="margin:2px 0 0 0; color:#6b7280; font-size:13px;">
                    AI-Powered Sales Analytics
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.divider()

        # ── Training status ──────────────────────────────────────────────
        try:
            summary = vn.training_summary()
            total = summary.get("total", 0)
            sql_count = summary.get("sql", 0)
            ddl_count = summary.get("ddl", 0)
            doc_count = summary.get("documentation", 0)

            st.markdown("**📚 Training Status**")
            c1, c2, c3 = st.columns(3)
            c1.metric("Q→SQL", sql_count)
            c2.metric("DDL", ddl_count)
            c3.metric("Docs", doc_count)

            if not vn.has_minimum_training():
                st.warning(
                    f"⚠️ Only **{sql_count}** Q→SQL examples. "
                    f"Add at least **20** for reliable results."
                )
        except Exception:
            st.info("Training data status unavailable.")

        st.divider()

        # ── Sample questions ─────────────────────────────────────────────
        st.markdown("**💡 Try asking…**")
        for i, sample in enumerate(_SAMPLE_QUESTIONS):
            if st.button(
                sample,
                key=f"sidebar_sample_{i}",
                use_container_width=True,
            ):
                st.session_state["pending_question"] = sample

        st.divider()

        # ── Controls ─────────────────────────────────────────────────────
        if st.button("🗑️ Clear Chat", use_container_width=True):
            clear_chat_history()
            st.rerun()

        st.caption("Built with Vanna AI · Ollama · Streamlit")


__all__ = [
    "init_chat_session_state",
    "clear_chat_history",
    "render_chat_history",
    "run_query_pipeline",
    "render_sidebar",
]