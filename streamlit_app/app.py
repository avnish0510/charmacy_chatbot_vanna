"""
streamlit_app/app.py

Main entry point & home page for the Charmacy Milano Text-to-SQL Chatbot.

Run with:
    streamlit run streamlit_app/app.py

This file is the orchestrator — it owns the full query pipeline but
delegates every rendering task to components and every persistence task
to the dedicated modules:

    Rendering  → components/chart_renderer, kpi_card, sql_viewer,
                  data_table, (feedback inline with feedback_collector)
    SQL        → core/vanna_instance, sql_validator, error_recovery
    Charts     → charts/edge_case_handler, data_shape_analyzer,
                  chart_type_selector, chart_spec_generator,
                  insight_annotator
    Logging    → persistence/sqlite_store
    Feedback   → feedback/feedback_collector (→ vn.train + SQLite)

Pipeline per user question:
    1.  Preprocess (extract chart hint, clean question for LLM)
    2.  Generate SQL (Vanna — embed → RAG → Ollama → SQL string)
    3.  Validate SQL (security block / sanity auto-fix)
    4.  Error recovery if validation or execution fails (max 2 retries)
    5.  Execute SQL (Vanna run_sql → pandas DataFrame)
    6.  Log query to SQLite (timing, success, rows, chart type)
    7.  Edge case handling (empty, null, identical, single-cell, >5K rows)
    8.  Data shape analysis → chart type selection → Vega-Lite spec
    9.  Insight annotations (max/min rules, trend detection)
    10. Render (KPI card / Vega-Lite chart / data table) via components
    11. Feedback collection (👍→train+log  👎→log  ✏️→validate→train+log)
"""

from __future__ import annotations

import logging
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

import pandas as pd
import streamlit as st

# ── Project root on sys.path ────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Core ────────────────────────────────────────────────────────────────────
from core.vanna_instance import get_vanna, MyVanna
from core.sql_validator import validate_sql, ValidationResult

if TYPE_CHECKING:
    from persistence.sqlite_store import SQLiteStore

try:
    # from core.error_recovery import error_recovery as _error_recovery
    from core.error_recovery import run_with_recovery, RecoveryResult

except ImportError:
    _error_recovery = None

# ── Chart pipeline ──────────────────────────────────────────────────────────
from charts.edge_case_handler import handle_edge_cases, EdgeCaseResult
from charts.data_shape_analyzer import analyze_data_shape, DataShape
from charts.chart_type_selector import select_chart_type
from charts.chart_spec_generator import generate_chart_spec
from charts.insight_annotator import annotate_insights, InsightSummary

# ── UI components ───────────────────────────────────────────────────────────
from streamlit_app.components.chart_renderer import render_chart
from streamlit_app.components.sql_viewer import render_sql_viewer
from streamlit_app.components.data_table import render_data_table
from streamlit_app.components.kpi_card import render_kpi_card

try:
    from persistence.sqlite_store import get_sqlite_store, SQLiteStore
    _HAS_PERSISTENCE = True
    print("+++++++++++++++++++++++++++++")

except ImportError as e:
    import traceback
    traceback.print_exc()   # prints the full chain including root cause
    _HAS_PERSISTENCE = False
    print("[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]")
    # SQLiteStore = None
    def get_sqlite_store(**_):
        return None
# ── Feedback (graceful — app works without it) ──────────────────────────────
try:
    from feedback.feedback_collector import (
        record_positive_feedback,
        record_negative_feedback,
        record_corrected_feedback,
    )
    _HAS_FEEDBACK = True
except ImportError:
    _HAS_FEEDBACK = False

# ── Page config (MUST be the first Streamlit command) ───────────────────────
st.set_page_config(
    page_title="Charmacy Milano — AI Analytics",
    page_icon="💄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Custom CSS ──────────────────────────────────────────────────────────────
_CUSTOM_CSS = """
<style>
    /* ── Global typography ─────────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont,
                     'Segoe UI', Roboto, sans-serif;
    }

    /* ── Hide Streamlit branding ───────────────────────────────────── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* ── Chat message refinements ──────────────────────────────────── */
    [data-testid="stChatMessage"] {
        border-radius: 12px;
        border: 1px solid #f1f5f9;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }

    /* ── Sidebar polish ────────────────────────────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #fafbfc 0%, #ffffff 100%);
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        font-size: 14px;
    }

    /* ── Expander borders ──────────────────────────────────────────── */
    .streamlit-expanderHeader {
        font-weight: 600 !important;
        color: #374151 !important;
    }

    /* ── Chat input styling ────────────────────────────────────────── */
    [data-testid="stChatInput"] textarea {
        border-radius: 12px !important;
    }

    /* ── Metric cards inside sidebar ───────────────────────────────── */
    [data-testid="stSidebar"] [data-testid="stMetricValue"] {
        font-size: 22px !important;
    }

    /* ── Divider softness ──────────────────────────────────────────── */
    hr {
        border-color: #f1f5f9 !important;
    }
</style>
"""
st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# CACHED RESOURCES
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner="Loading AI engine…")
def _load_vanna() -> MyVanna:
    """Initialise and cache the Vanna instance (created once across reruns)."""
    return get_vanna()


@st.cache_resource(show_spinner=False)
def _load_store() -> Optional[SQLiteStore]:
    """Initialise and cache the SQLite persistence store."""
    if not _HAS_PERSISTENCE:
        return None
    try:
        return get_sqlite_store()
    except Exception as exc:
        logger.warning("SQLite store init failed (app will work without it): %s", exc)
        return None


# ═════════════════════════════════════════════════════════════════════════════
# CHART HINT EXTRACTION
# ═════════════════════════════════════════════════════════════════════════════

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
    """Extract chart type hint from the user's question (e.g. 'as a bar chart')."""
    match = _CHART_HINT_PATTERN.search(question)
    return match.group(1).strip().lower() if match else ""


def _clean_question_for_sql(question: str) -> str:
    """Remove chart hint phrases so they don't confuse the LLM during SQL generation."""
    cleaned = _CHART_HINT_PATTERN.sub("", question).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.")
    return cleaned if cleaned else question


# ═════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═════════════════════════════════════════════════════════════════════════════

_SESSION_DEFAULTS: Dict[str, Any] = {
    "vn":               None,
    "sqlite_store":     None,
    "messages":         [],
    "last_sql":         "",
    "last_df":          None,
    "last_question":    "",
    "last_query_id":    "",
    "last_chart_spec":  None,
    "last_chart_type":  "",
}


def _init_session_state() -> None:
    """Set default values for all session-state keys if they don't exist yet."""
    for key, default in _SESSION_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default


# ═════════════════════════════════════════════════════════════════════════════
# SAMPLE QUESTIONS
# ═════════════════════════════════════════════════════════════════════════════

_SAMPLE_QUESTIONS: list[str] = [
    "What is the total revenue by platform?",
    "Show monthly revenue trend",
    "Top 10 products by units sold",
    "Revenue by state as a bar chart",
    "What is the cancellation rate?",
    "Compare B2B vs B2C revenue",
    "Top 5 article types by revenue as donut",
    "Average selling price by platform",
    "Which states have the highest orders?",
    "Nykaa discount percentage",
]


# ═════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════

def _render_sidebar(vn: MyVanna, store: Optional[SQLiteStore]) -> None:
    """Render the sidebar: branding, training status, persistence stats, samples."""
    with st.sidebar:
        # ── Branding ─────────────────────────────────────────────────────
        st.markdown(
            """
            <div style="text-align:center; padding:4px 0 14px 0;">
                <span style="font-size:32px;">💄</span>
                <h2 style="margin:2px 0 0 0; color:#1a1a2e; font-weight:700;
                           font-size:22px; letter-spacing:-0.02em;">
                    Charmacy Milano
                </h2>
                <p style="margin:2px 0 0 0; color:#9ca3af; font-size:12px;">
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
            sql_count = summary.get("sql", 0)
            ddl_count = summary.get("ddl", 0)
            doc_count = summary.get("documentation", 0)

            st.markdown("**📚 Training Data**")
            c1, c2, c3 = st.columns(3)
            c1.metric("Q→SQL", sql_count)
            c2.metric("DDL", ddl_count)
            c3.metric("Docs", doc_count)

            if not vn.has_minimum_training():
                st.warning(
                    f"⚠️ Only **{sql_count}** Q→SQL examples. "
                    f"Add ≥ 20 for reliable results."
                )
        except Exception:
            st.caption("Training status unavailable.")

        # ── Persistence stats ────────────────────────────────────────────
        if store is not None:
            try:
                stats = store.get_stats()
                total_q = stats.get("total_queries", 0)
                if total_q > 0:
                    success_q = stats.get("successful_queries", 0)
                    rate = round((success_q / total_q) * 100, 1) if total_q else 0
                    st.divider()
                    st.markdown("**📈 Usage**")
                    p1, p2 = st.columns(2)
                    p1.metric("Queries", total_q)
                    p2.metric("Success", f"{rate}%")
            except Exception:
                pass

        st.divider()

        # ── Sample questions ─────────────────────────────────────────────
        st.markdown("**💡 Try asking…**")
        for i, sample in enumerate(_SAMPLE_QUESTIONS):
            if st.button(sample, key=f"sidebar_q_{i}", use_container_width=True):
                st.session_state["pending_question"] = sample

        st.divider()

        # ── Controls ─────────────────────────────────────────────────────
        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state["messages"] = []
            st.session_state["last_sql"] = ""
            st.session_state["last_df"] = None
            st.session_state["last_chart_spec"] = None
            st.session_state["last_chart_type"] = ""
            st.rerun()

        st.caption("Vanna AI · Ollama qwen3:9b · Streamlit")


def _run_query_pipeline(
    vn: MyVanna,
    question: str,
    store: Optional[SQLiteStore],
) -> None:
    """
    Execute the full 11-step text-to-SQL → chart pipeline.

    All rendering is delegated to component modules.
    All persistence is delegated to sqlite_store / feedback_collector.
    """
    query_id = str(uuid.uuid4())
    st.session_state["last_query_id"] = query_id
    st.session_state["last_question"] = question

    pipeline_start = time.perf_counter()
    retries_used = 0
    chart_type = ""
    error_message = ""

    # ══════════════════════════════════════════════════════════════════════
    # 1. PREPROCESS
    # ══════════════════════════════════════════════════════════════════════
    chart_hint = _extract_chart_hint(question)
    clean_question = _clean_question_for_sql(question)
    if chart_hint:
        logger.info("Chart hint: '%s'", chart_hint)

    # ══════════════════════════════════════════════════════════════════════
    # 2–5. GENERATE → VALIDATE → EXECUTE  (all retries handled internally)
    #
    # run_with_recovery() is the single entry point for the full pipeline.
    # It calls vn.generate_sql(), validate_sql(), and vn.run_sql() for up to
    # MAX_ATTEMPTS times, building richer correction context on each retry.
    # It returns a RecoveryResult — never raises.
    # ══════════════════════════════════════════════════════════════════════
    gen_start = time.perf_counter()
    with st.spinner("🧠 Generating SQL and running query…"):
        result: RecoveryResult = run_with_recovery(vn, clean_question)
    gen_time = time.perf_counter() - gen_start

    retries_used = max(0, result.attempt_count - 1)

    # ── Security violation → hard stop (never retry) ─────────────────────
    if result.is_security:
        st.error("🚫 **Security violation** — this query was blocked.")
        if result.last_attempt and result.last_attempt.validation_result:
            for v in result.last_attempt.validation_result.violations:
                st.warning(v)
        error_message = result.failure_reason
        logger.warning("SECURITY | query_id=%s | %s", query_id, error_message)
        _log_query(store, query_id, question, "", False, error_message)
        return

    # ── All retries exhausted ─────────────────────────────────────────────
    if not result.success:
        st.error(result.user_facing_error())
        last_sql = result.last_attempt.display_sql if result.last_attempt else ""
        error_message = result.failure_reason
        _log_query(
            store, query_id, question, last_sql, False, error_message,
            retries=retries_used,
        )
        return

    # ── Pipeline succeeded ────────────────────────────────────────────────
    sql = result.sql          # always use fixed_sql from ValidationResult
    df  = result.df
    exec_time = result.total_elapsed_ms / 1000.0   # ms → seconds for caption

    # Cap rows per spec
    if df is not None:
        df = df.head(10_000)

    st.session_state["last_sql"] = sql
    st.session_state["last_df"] = df
    rows_returned = len(df) if df is not None else 0

    # ══════════════════════════════════════════════════════════════════════
    # 6. SHOW SQL (collapsible viewer)
    # ══════════════════════════════════════════════════════════════════════
    # Recover auto-fix metadata from the successful attempt if available
    _last_attempt = result.last_attempt
    _vr = _last_attempt.validation_result if _last_attempt else None
    _auto_fixed = bool(_vr and _vr.auto_fixed)

    render_sql_viewer(
        sql=sql,
        original_sql=_vr.original_sql if _auto_fixed else None,
        auto_fixed=_auto_fixed,
    )

    # ══════════════════════════════════════════════════════════════════════
    # 7. EDGE CASE HANDLING
    # ══════════════════════════════════════════════════════════════════════
    edge: EdgeCaseResult = handle_edge_cases(df, sql)

    if edge.warning_message:
        st.info(edge.warning_message)

    if edge.should_stop:
        _log_query(store, query_id, question, sql, True, "",
                   rows_returned=0, chart_type="none",
                   exec_ms=int((time.perf_counter() - pipeline_start) * 1000),
                   retries=retries_used)
        return

    working_df = edge.df if edge.df is not None else df

    # ══════════════════════════════════════════════════════════════════════
    # 8. CHART PIPELINE (analyse → select → generate → annotate)
    # ══════════════════════════════════════════════════════════════════════
    if edge.force_chart_type:
        chart_type = edge.force_chart_type
        shape = analyze_data_shape(working_df, question)
    else:
        shape = analyze_data_shape(working_df, question)
        chart_type = select_chart_type(shape, chart_hint=chart_hint)

    st.session_state["last_chart_type"] = chart_type

    # ══════════════════════════════════════════════════════════════════════
    # 9 & 10. RENDER
    # ══════════════════════════════════════════════════════════════════════
    if chart_type == "kpi_card":
        render_kpi_card(working_df, edge_result=edge)

    elif chart_type == "table":
        render_data_table(working_df, title="Query Results")

    else:
        # Generate Vega-Lite spec
        spec = generate_chart_spec(working_df, shape, chart_type)

        # Determine primary columns for annotation
        num_col = shape.numeric_cols[0] if shape.numeric_cols else None
        label_col = (
            shape.categorical_cols[0] if shape.categorical_cols
            else shape.temporal_col
        )

        # Insight annotations (max/min lines, trend detection)
        spec, insights = annotate_insights(
            spec, working_df, chart_type,
            numeric_col=num_col, label_col=label_col,
        )

        st.session_state["last_chart_spec"] = spec

        # Render the chart via vega-embed component
        render_chart(spec, chart_type=chart_type)

        # Insight summary caption
        if insights.summary_text:
            st.caption(f"💡 {insights.summary_text}")

        # Data table below chart (collapsed)
        with st.expander("📊 View Data", expanded=False):
            render_data_table(
                working_df, title="", show_download=True, compact=True,
            )

    # ── Timing / meta caption ────────────────────────────────────────────
    total_ms = int((time.perf_counter() - pipeline_start) * 1000)
    meta_parts = []
    if gen_time:
        meta_parts.append(f"SQL in {gen_time:.1f}s")
    if exec_time:
        meta_parts.append(f"executed in {exec_time:.1f}s")
    meta_parts.append(f"{rows_returned:,} rows")
    if chart_type:
        meta_parts.append(chart_type.replace("_", " "))
    st.caption(f"⚡ {' · '.join(meta_parts)}")

    # ══════════════════════════════════════════════════════════════════════
    # 6 (deferred). LOG QUERY TO SQLITE
    # ══════════════════════════════════════════════════════════════════════
    _log_query(
        store, query_id, question, sql, True, "",
        rows_returned=rows_returned,
        exec_ms=total_ms,
        chart_type=chart_type,
        retries=retries_used,
    )

    # ══════════════════════════════════════════════════════════════════════
    # 11. FEEDBACK
    # ══════════════════════════════════════════════════════════════════════
    _render_feedback_section(vn, question, sql, query_id, store)

# ═════════════════════════════════════════════════════════════════════════════
# SQLITE LOGGING HELPER
# ═════════════════════════════════════════════════════════════════════════════

def _log_query(
    store: Optional[SQLiteStore],
    query_id: str,
    question: str,
    sql: str,
    success: bool,
    error_message: str,
    rows_returned: int = 0,
    exec_ms: int = 0,
    chart_type: str = "",
    retries: int = 0,
) -> None:
    """Log a query to SQLite.  No-op if store is None."""
    if store is None:
        return
    try:
        store.log_query(
            query_id=query_id,
            question=question,
            sql=sql,
            success=success,
            error_message=error_message,
            rows_returned=rows_returned,
            execution_ms=exec_ms,
            chart_type=chart_type,
            retries=retries,
        )
    except Exception as exc:
        logger.warning("SQLite query log failed (non-critical): %s", exc)


# ═════════════════════════════════════════════════════════════════════════════
# FEEDBACK SECTION
# ═════════════════════════════════════════════════════════════════════════════

def _render_feedback_section(
    vn: MyVanna,
    question: str,
    sql: str,
    query_id: str,
    store: Optional[SQLiteStore],
) -> None:
    """
    Render 👍 👎 ✏️ feedback buttons with full persistence integration.

    👍 → record_positive_feedback (vn.train + SQLite log)
    👎 → record_negative_feedback (SQLite log only — NO training)
    ✏️ → editable SQL area → validate → execute →
         record_corrected_feedback (vn.train + SQLite log)
    """
    if not sql or not sql.strip():
        return

    st.divider()
    st.markdown(
        "<p style='font-size:13px; color:#9ca3af; margin-bottom:6px;'>"
        "Was this result helpful?</p>",
        unsafe_allow_html=True,
    )

    col_up, col_down, col_edit, _ = st.columns([1, 1, 1, 5])

    # ── 👍 Positive ─────────────────────────────────────────────────────
    with col_up:
        if st.button("👍 Correct", key=f"fb_up_{query_id}",
                      help="Mark correct — saves Q→SQL for training",
                      use_container_width=True):
            if _HAS_FEEDBACK:
                result = record_positive_feedback(
                    vn, question, sql, query_id, sqlite_store=store,
                )
                if result.get("trained"):
                    st.success("✅ Q→SQL pair saved for training.")
                else:
                    st.info(result.get("message", "Feedback noted."))
            else:
                # Fallback: train directly without persistence
                try:
                    vn.train(question=question, sql=sql)
                    st.success("✅ Q→SQL pair saved for training.")
                except Exception as exc:
                    st.warning(f"Training failed: {exc}")
            logger.info("POSITIVE | query_id=%s", query_id)

    # ── 👎 Negative ─────────────────────────────────────────────────────
    with col_down:
        if st.button("👎 Wrong", key=f"fb_down_{query_id}",
                      help="Mark wrong — will NOT train on this",
                      use_container_width=True):
            if _HAS_FEEDBACK:
                record_negative_feedback(
                    question, sql, query_id, sqlite_store=store,
                )
            st.warning("📝 Noted — this will **not** be used for training.")
            logger.info("NEGATIVE | query_id=%s", query_id)

    # ── ✏️ Edit ─────────────────────────────────────────────────────────
    with col_edit:
        if st.button("✏️ Edit SQL", key=f"fb_edit_{query_id}",
                      help="Correct the SQL yourself",
                      use_container_width=True):
            st.session_state[f"show_edit_{query_id}"] = True

    # ── Editable SQL area ────────────────────────────────────────────────
    if st.session_state.get(f"show_edit_{query_id}", False):
        st.markdown("---")
        corrected_sql = st.text_area(
            "Corrected SQL:",
            value=sql,
            height=220,
            key=f"edit_area_{query_id}",
            label_visibility="collapsed",
        )

        c_submit, c_cancel, _ = st.columns([2, 2, 6])
        with c_submit:
            do_submit = st.button("✅ Submit", key=f"submit_{query_id}",
                                  type="primary", use_container_width=True)
        with c_cancel:
            do_cancel = st.button("Cancel", key=f"cancel_{query_id}",
                                  use_container_width=True)

        if do_cancel:
            st.session_state[f"show_edit_{query_id}"] = False
            st.rerun()

        if do_submit:
            _process_sql_correction(
                vn, question, sql, corrected_sql, query_id, store,
            )


def _process_sql_correction(
    vn: MyVanna,
    question: str,
    original_sql: str,
    corrected_sql: str,
    query_id: str,
    store: Optional[SQLiteStore],
) -> None:
    """Validate, execute, and train with user-corrected SQL."""
    if not corrected_sql or not corrected_sql.strip():
        st.error("SQL cannot be empty.")
        return

    # Validate
    val = validate_sql(corrected_sql)
    if val.is_security_violation:
        st.error("🚫 Corrected SQL failed security checks.")
        for v in val.violations:
            st.warning(v)
        return

    final_sql = val.fixed_sql

    # Execute
    if _HAS_FEEDBACK:
        result = record_corrected_feedback(
            vn, question, original_sql, final_sql, query_id,
            sqlite_store=store,
        )
        if result.get("success"):
            st.success("✅ Corrected SQL executed and trained!")
            if result.get("df") is not None and not result["df"].empty:
                st.dataframe(result["df"].head(100), use_container_width=True)
        else:
            st.error(result.get("message", "Correction failed."))
    else:
        # Fallback without feedback_collector
        try:
            with st.spinner("Running corrected SQL…"):
                test_df = vn.run_sql(final_sql)
            st.success("✅ Corrected SQL ran successfully!")
            vn.train(question=question, sql=final_sql)
            st.info("Corrected Q→SQL pair saved for training.")
            if test_df is not None and not test_df.empty:
                st.dataframe(test_df.head(100), use_container_width=True)
        except Exception as exc:
            st.error(f"❌ Corrected SQL failed: {exc}")

    st.session_state[f"show_edit_{query_id}"] = False


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Application entry point — initialise resources and run the chat loop."""
    _init_session_state()

    # ── Load resources ───────────────────────────────────────────────────
    try:
        vn = _load_vanna()
    except Exception as exc:
        st.error(f"❌ Failed to initialise AI engine: {exc}")
        st.info(
            "**Checklist:**\n"
            "1. Is Ollama running? (`ollama serve`)\n"
            "2. Is qwen3:9b pulled? (`ollama pull qwen3:9b`)\n"
            "3. Are DB credentials set in `.env`?\n"
            "4. Run `python scripts/test_connection.py` to diagnose."
        )
        logger.critical("Vanna init failed: %s", exc, exc_info=True)
        st.stop()

    store = _load_store()

    st.session_state["vn"] = vn
    st.session_state["sqlite_store"] = store

    # ── Sidebar ──────────────────────────────────────────────────────────
    _render_sidebar(vn, store)

    # ── Header ───────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="padding:4px 0 16px 0;">
            <h1 style="margin:0; color:#1a1a2e; font-weight:700;
                       font-size:28px; letter-spacing:-0.02em;">
                💬 Ask about your data
            </h1>
            <p style="margin:4px 0 0 0; color:#6b7280; font-size:14px;">
                Type a question in plain English.  The AI generates SQL,
                runs it on your database, and visualises the results.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Render chat history ──────────────────────────────────────────────
    for msg in st.session_state["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # ── Pending question (from sidebar sample click) ─────────────────────
    pending = st.session_state.pop("pending_question", None)

    # ── Chat input ───────────────────────────────────────────────────────
    user_input = st.chat_input(
        "Ask a question about Charmacy Milano sales data…"
    )

    question = pending or user_input

    if question:
        # Record user message
        st.session_state["messages"].append({
            "role": "user", "content": question,
        })
        with st.chat_message("user"):
            st.markdown(question)

        # Run pipeline inside assistant message container
        with st.chat_message("assistant"):
            _run_query_pipeline(vn, question, store)

        # Record assistant placeholder in history
        st.session_state["messages"].append({
            "role": "assistant",
            "content": f"*(Results for: {question})*",
        })


# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    main()