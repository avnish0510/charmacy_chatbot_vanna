
"""
streamlit_app/app.py

Main entry point & home page for the Charmacy Milano Text-to-SQL Chatbot.

Run with:
    streamlit run streamlit_app/app.py

This file is the orchestrator — it owns the full query pipeline but
delegates every rendering task to components and every persistence task
to the dedicated modules:

    Rendering → components/chart_renderer, kpi_card, sql_viewer,
                data_table, (feedback inline with feedback_collector)
    SQL       → core/vanna_instance, sql_validator, error_recovery
    Charts    → charts/edge_case_handler, data_shape_analyzer,
                chart_type_selector, chart_spec_generator,
                insight_annotator
    Logging   → persistence/sqlite_store
    Feedback  → feedback/feedback_collector (→ vn.train + SQLite)

Pipeline per user question:
    1. Preprocess (extract chart hint, clean question for LLM)
    2. Generate SQL (Vanna — embed → RAG → Ollama → SQL string)
    3. Validate SQL (security block / sanity auto-fix)
    4. Error recovery if validation or execution fails (max 2 retries)
    5. Execute SQL (Vanna run_sql → pandas DataFrame)
    6. Log query to SQLite (timing, success, rows, chart type)
    7. Edge case handling (empty, null, identical, single-cell, >5K rows)
    8. Data shape analysis → chart type selection → Vega-Lite spec
    9. Insight annotations (max/min rules, trend detection)
   10. Render (KPI card / Vega-Lite chart / data table) via components
   11. Feedback collection (👍→train+log  👎→log  ✏️→validate→train+log)
"""
from __future__ import annotations

import logging
import re
import sys
import time
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

import pandas as pd
import streamlit as st

# ── Project root on sys.path ──────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# At module level, after ROOT is defined
import yaml as _yaml
_APP_CFG = _yaml.safe_load(open(ROOT / "config" / "vanna_config.yaml")) or {}
_SUMMARY_MODEL = _APP_CFG.get("summary_model", "")

# ── Core ──────────────────────────────────────────────────────────────────
from core.vanna_instance import get_vanna, MyVanna
from core.sql_validator import validate_sql, ValidationResult

from core.vanna_instance import get_vanna, MyVanna, CANNED_RESPONSES
from core.response_formatter import generate_answer_summary


if TYPE_CHECKING:
    from persistence.sqlite_store import SQLiteStore

# try:
#     from core.error_recovery import error_recovery as _error_recovery
#     from core.error_recovery import run_with_recovery, RecoveryResult
# except ImportError:
#     _error_recovery = None
from core.error_recovery import run_with_recovery, RecoveryResult

# ── Chart pipeline ────────────────────────────────────────────────────────
from charts.edge_case_handler import handle_edge_cases, EdgeCaseResult
from charts.data_shape_analyzer import analyze_data_shape, DataShape
from charts.chart_type_selector import select_chart_type
from charts.chart_spec_generator import generate_chart_spec
from charts.insight_annotator import annotate_insights, InsightSummary

# ── UI components ─────────────────────────────────────────────────────────
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
    traceback.print_exc()
    _HAS_PERSISTENCE = False
    print("[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]")
    # SQLiteStore = None
    def get_sqlite_store(**_):
        return None

# ── Feedback (graceful — app works without it) ────────────────────────────
try:
    from feedback.feedback_collector import (
        record_positive_feedback,
        record_negative_feedback,
        record_corrected_feedback,
    )
    _HAS_FEEDBACK = True
except ImportError:
    _HAS_FEEDBACK = False

# ── Page config (MUST be the first Streamlit command) ─────────────────────
st.set_page_config(
    page_title="Charmacy Milano — AI Analytics",
    page_icon="💄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Custom CSS ────────────────────────────────────────────────────────────
_CUSTOM_CSS = """
<style>
    /* ── Font import ─────────────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* ── Global typography ───────────────────────────────────────── */
    html, body, .stApp {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont,
                     'Segoe UI', Roboto, sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }

    /* ── Hide Streamlit chrome ───────────────────────────────────── */
    #MainMenu  { visibility: hidden; }
    footer     { visibility: hidden; }
    header     { visibility: hidden; }

    /* ── Main container ──────────────────────────────────────────── */
    .main > .block-container {
        padding-top: 1.8rem;
        padding-bottom: 3rem;
        max-width: 1150px;
    }

    /* ── Sidebar ─────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(
            180deg, #F8FAFC 0%, #FFFFFF 55%, #FFF1F2 100%
        ) !important;
        border-right: 1px solid #E2E8F0 !important;
    }

    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 0.4rem;
    }

    section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        font-size: 13px;
    }

    /* Sidebar buttons (sample questions, clear chat) */
    section[data-testid="stSidebar"] .stButton > button {
        background: #F1F5F9 !important;
        color: #334155 !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 8px !important;
        font-size: 12.5px !important;
        font-weight: 500 !important;
        text-align: left !important;
        transition: all 0.15s ease !important;
        padding: 7px 12px !important;
        line-height: 1.45 !important;
    }

    section[data-testid="stSidebar"] .stButton > button:hover {
        background: #FFF1F2 !important;
        border-color: #FECDD3 !important;
        color: #E11D48 !important;
        transform: translateX(2px);
    }

    /* ── Chat messages ───────────────────────────────────────────── */
    div[data-testid="stChatMessage"] {
        border-radius: 14px !important;
        border: 1px solid #F1F5F9 !important;
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04) !important;
        margin-bottom: 10px !important;
    }

    /* ── Chat input ──────────────────────────────────────────────── */
    div[data-testid="stChatInput"] textarea {
        border-radius: 14px !important;
        border: 2px solid #E2E8F0 !important;
        font-size: 14px !important;
        transition: all 0.2s ease !important;
    }

    div[data-testid="stChatInput"] textarea:focus {
        border-color: #F43F5E !important;
        box-shadow: 0 0 0 3px rgba(244, 63, 94, 0.08) !important;
    }

    /* ── Metrics ─────────────────────────────────────────────────── */
    div[data-testid="stMetricValue"] {
        color: #0F172A !important;
        font-weight: 700 !important;
    }

    div[data-testid="stMetricLabel"] {
        color: #64748B !important;
        font-weight: 600 !important;
        font-size: 11px !important;
        text-transform: uppercase !important;
        letter-spacing: 0.04em !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stMetricValue"] {
        font-size: 20px !important;
        color: #E11D48 !important;
    }

    /* ── Expanders ───────────────────────────────────────────────── */
    .streamlit-expanderHeader {
        font-weight: 600 !important;
        color: #334155 !important;
        font-size: 13px !important;
    }

    details {
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
    }

    /* ── Primary buttons ─────────────────────────────────────────── */
    button[data-testid="baseButton-primary"] {
        background: linear-gradient(135deg, #E11D48, #F43F5E) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 2px 6px rgba(225, 29, 72, 0.2) !important;
    }

    button[data-testid="baseButton-primary"]:hover {
        background: linear-gradient(135deg, #BE123C, #E11D48) !important;
        box-shadow: 0 4px 14px rgba(225, 29, 72, 0.3) !important;
        transform: translateY(-1px);
    }

    /* ── Secondary / default buttons ─────────────────────────────── */
    button[data-testid="baseButton-secondary"] {
        border-radius: 8px !important;
        border: 1px solid #E2E8F0 !important;
        color: #475569 !important;
        font-weight: 500 !important;
        transition: all 0.15s ease !important;
    }

    button[data-testid="baseButton-secondary"]:hover {
        border-color: #FDA4AF !important;
        color: #E11D48 !important;
        background: #FFF1F2 !important;
    }

    /* ── Dividers ────────────────────────────────────────────────── */
    hr {
        border-color: #F1F5F9 !important;
        margin: 8px 0 !important;
    }

    /* ── Code blocks ─────────────────────────────────────────────── */
    pre {
        background: #F8FAFC !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
    }

    /* ── Alerts ──────────────────────────────────────────────────── */
    div[data-testid="stAlert"] {
        border-radius: 10px !important;
        font-size: 13px !important;
    }

    /* ── Dataframe container ─────────────────────────────────────── */
    div[data-testid="stDataFrame"] {
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
        overflow: hidden !important;
    }

    /* ── Scrollbar ───────────────────────────────────────────────── */
    ::-webkit-scrollbar       { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb {
        background: #CBD5E1;
        border-radius: 10px;
    }
    ::-webkit-scrollbar-thumb:hover { background: #94A3B8; }

    /* ── Text area (feedback edit) ────────────────────────────────── */
    textarea {
        border-radius: 10px !important;
        border: 1px solid #E2E8F0 !important;
        transition: border-color 0.2s ease !important;
    }

    textarea:focus {
        border-color: #F43F5E !important;
        box-shadow: 0 0 0 3px rgba(244, 63, 94, 0.08) !important;
    }
</style>
"""
st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# CACHED RESOURCES
# ═══════════════════════════════════════════════════════════════════════════
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


# ═══════════════════════════════════════════════════════════════════════════
# CHART HINT EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════
_CHART_HINT_PATTERN = re.compile(
    r"(?:show\s+(?:as|in)\s+(?:a\s+)?|display\s+(?:as|in)\s+(?:a\s+)?|"
    r"as\s+(?:a\s+)?|in\s+(?:a\s+)?|use\s+(?:a\s+)?)"
    r"(bar\schart|line\schart|pie\schart|donut\schart|scatter\splot|"
    r"bubble\schart|heatmap|heat\smap|histogram|area\schart|table|"
    r"kpi|card|bar|line|pie|donut|scatter|bubble|area|"
    r"horizontal\sbar|vertical\sbar|grouped\s*bar|multi[- ]?line|diverging)",
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


# ═══════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════
_SESSION_DEFAULTS: Dict[str, Any] = {
    "vn": None,
    "sqlite_store": None,
    "messages": [],
    "active_job": None,
    "last_sql": "",
    "last_df": None,
    "last_question": "",
    "last_query_id": "",
    "last_chart_spec": None,
    "last_chart_type": "",
}


def _init_session_state() -> None:
    """Set default values for all session-state keys if they don't exist yet."""
    for key, default in _SESSION_DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default


def _has_running_query() -> bool:
    """Return True when a background query job is currently running."""
    active_job = st.session_state.get("active_job")
    return bool(active_job and active_job.get("status") == "running")


# ═══════════════════════════════════════════════════════════════════════════
# SAMPLE QUESTIONS
# ═══════════════════════════════════════════════════════════════════════════
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


# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════
def _render_sidebar(vn: MyVanna, store: Optional[SQLiteStore]) -> None:
    """Render the sidebar: branding, training status, persistence stats, samples."""
    with st.sidebar:
        busy = _has_running_query()

        # ── Branding ───────────────────────────────────────────────────
        st.markdown(
            """
            <div style="text-align:center; padding:12px 0 20px 0;">
                <div style="
                    width:52px; height:52px; margin:0 auto 10px auto;
                    background: linear-gradient(135deg, #E11D48, #FB7185);
                    border-radius:14px; display:flex; align-items:center;
                    justify-content:center; font-size:26px;
                    box-shadow: 0 4px 14px rgba(225, 29, 72, 0.25);
                ">💄</div>
                <h2 style="
                    margin:0; color:#0F172A; font-weight:800;
                    font-size:19px; letter-spacing:-0.03em;
                ">Charmacy Milano</h2>
                <p style="
                    margin:3px 0 0 0; color:#94A3B8; font-size:11.5px;
                    font-weight:500; letter-spacing:0.03em;
                ">AI-Powered Sales Analytics</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.divider()

        # ── Training status ──────────────────────────────────────────
        try:
            summary = vn.training_summary()
            sql_count = summary.get("sql", 0)
            ddl_count = summary.get("ddl", 0)
            doc_count = summary.get("documentation", 0)

            st.markdown(
                "<p style='font-size:11px; color:#64748B; font-weight:700; "
                "text-transform:uppercase; letter-spacing:0.06em; "
                "margin:0 0 8px 0;'>📚 Training Data</p>",
                unsafe_allow_html=True,
            )
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

        # ── Persistence stats ────────────────────────────────────────
        if store is not None:
            try:
                stats = store.get_stats()
                total_q = stats.get("total_queries", 0)
                if total_q > 0:
                    success_q = stats.get("successful_queries", 0)
                    rate = round((success_q / total_q) * 100, 1) if total_q else 0
                    st.divider()
                    st.markdown(
                        "<p style='font-size:11px; color:#64748B; font-weight:700; "
                        "text-transform:uppercase; letter-spacing:0.06em; "
                        "margin:0 0 8px 0;'>📈 Usage</p>",
                        unsafe_allow_html=True,
                    )
                    p1, p2 = st.columns(2)
                    p1.metric("Queries", total_q)
                    p2.metric("Success", f"{rate}%")
            except Exception:
                pass

        st.divider()

        # ── Sample questions ─────────────────────────────────────────
        st.markdown(
            "<p style='font-size:11px; color:#64748B; font-weight:700; "
            "text-transform:uppercase; letter-spacing:0.06em; "
            "margin:0 0 8px 0;'>💡 Try asking</p>",
            unsafe_allow_html=True,
        )
        for i, sample in enumerate(_SAMPLE_QUESTIONS):
            if st.button(
                sample,
                key=f"sidebar_q_{i}",
                use_container_width=True,
                disabled=busy,
            ):
                st.session_state["pending_question"] = sample

        st.divider()

        if st.button("🗑️ Clear Chat", use_container_width=True, disabled=busy):
            st.session_state["messages"] = []
            st.session_state["active_job"] = None
            st.session_state["last_sql"] = ""
            st.session_state["last_df"] = None
            st.session_state["last_chart_spec"] = None
            st.session_state["last_chart_type"] = ""

            for key in list(st.session_state.keys()):
                if (
                    key.startswith("result_open_")
                    or key.startswith("show_edit_")
                    or key.startswith("edit_area_")
                ):
                    del st.session_state[key]

            st.rerun()

        if busy:
            st.caption("A query is running in the background.")


        st.markdown(
            "<p style='text-align:center; font-size:11px; color:#CBD5E1; "
            "margin-top:16px; font-weight:500;'>"
            "Vanna AI · Ollama qwen3:9b · Streamlit</p>",
            unsafe_allow_html=True,
        )



def _run_query_pipeline(
    vn: MyVanna,
    question: str,
    store: Optional[SQLiteStore],
) -> Dict[str, Any]:
    query_id = str(uuid.uuid4())
    pipeline_start = time.perf_counter()

    # ══════════════════════════════════════════════════════════════
    # SINGLE LLM CALL: classify + normalize + generate SQL
    # ══════════════════════════════════════════════════════════════
    meta = vn.generate_sql_with_classification(question)
    category        = meta["category"]
    normalized      = meta["normalized"]
    first_attempt_sql = meta["sql"]

    # ── Early exit for non-data categories ────────────────────────
    if category != "data_question":
        return {
            "role": "assistant",
            "kind": "result",
            "query_id": query_id,
            "question": question,
            "status": "early_exit",
            "category": category,
            "reply_text": CANNED_RESPONSES.get(category, "I cannot help with that."),
            "sql": "", "original_sql": None, "auto_fixed": False,
            "df": None, "edge_result": None, "chart_type": "",
            "chart_spec": None, "insight_text": "", "warning_message": "",
            "meta_text": "", "violations": [], "error_text": "", "summary": "",
        }

    # ── data_question: validate + execute first attempt SQL ────────
    chart_hint    = _extract_chart_hint(normalized)
    clean_question = _clean_question_for_sql(normalized)
    retries_used  = 0
    chart_type    = ""

    # Try first-attempt SQL directly if we got one
    result: RecoveryResult
    if first_attempt_sql and first_attempt_sql.strip():
        from core.sql_validator import validate_sql
        val = validate_sql(first_attempt_sql)
        if val.is_valid:
            # Try executing
            try:
                from core.error_recovery import _execute_with_timeout, QUERY_TIMEOUT_SECS, ROW_CAP
                df = _execute_with_timeout(vn, val.fixed_sql, QUERY_TIMEOUT_SECS)
                if df is not None and len(df) > ROW_CAP:
                    df = df.head(ROW_CAP)
                # Build a fake successful RecoveryResult
                from core.error_recovery import RecoveryResult, AttemptRecord, FailureKind
                rec = AttemptRecord(attempt_number=1, question_sent=clean_question)
                rec.sql_generated = first_attempt_sql
                rec.validation_result = val
                rec.success = True
                rec.rows_returned = len(df) if df is not None else 0
                rec._df  = df
                rec._sql = val.fixed_sql
                result = RecoveryResult(
                    success=True, df=df, sql=val.fixed_sql,
                    attempts=[rec], total_elapsed_ms=(time.perf_counter()-pipeline_start)*1000
                )
            except Exception:
                # First attempt execution failed — fall through to run_with_recovery
                result = run_with_recovery(vn, clean_question)
                retries_used = max(0, result.attempt_count - 1)
        else:
            # Validation failed — fall through to run_with_recovery for retries
            result = run_with_recovery(vn, clean_question)
            retries_used = max(0, result.attempt_count - 1)
    else:
        # No SQL returned — fall through to run_with_recovery
        result = run_with_recovery(vn, clean_question)
        retries_used = max(0, result.attempt_count - 1)

    # ── Security violation ─────────────────────────────────────────
    if result.is_security:
        error_message = result.failure_reason
        _log_query(store, query_id, question, "", False, error_message)
        return {
            "role": "assistant", "kind": "result", "query_id": query_id,
            "question": question, "status": "security", "sql": "",
            "original_sql": None, "auto_fixed": False, "df": None,
            "edge_result": None, "chart_type": "", "chart_spec": None,
            "insight_text": "", "warning_message": "", "meta_text": "",
            "violations": (
                result.last_attempt.validation_result.violations
                if result.last_attempt and result.last_attempt.validation_result else []
            ),
            "error_text": result.failure_reason, "summary": "",
        }

    # ── All retries exhausted ──────────────────────────────────────
    if not result.success:
        last_sql = result.last_attempt.display_sql if result.last_attempt else ""
        _log_query(store, query_id, question, last_sql, False,
                   result.failure_reason, retries=retries_used)
        return {
            "role": "assistant", "kind": "result", "query_id": query_id,
            "question": question, "status": "error", "sql": last_sql,
            "original_sql": None, "auto_fixed": False, "df": None,
            "edge_result": None, "chart_type": "", "chart_spec": None,
            "insight_text": "", "warning_message": "", "meta_text": "",
            "violations": [], "error_text": result.user_facing_error(), "summary": "",
        }

    # ── Success ────────────────────────────────────────────────────
    sql = result.sql
    df  = result.df
    exec_time = result.total_elapsed_ms / 1000.0
    if df is not None:
        df = df.head(10_000)
    rows_returned = len(df) if df is not None else 0

    _last_attempt = result.last_attempt
    _vr = _last_attempt.validation_result if _last_attempt else None
    _auto_fixed = bool(_vr and _vr.auto_fixed)


    # Generate plain-English summary (uses already-fetched df — no extra LLM call)
    answer_summary = generate_answer_summary(vn, normalized, df, chart_type, summary_model=_SUMMARY_MODEL )

    edge: EdgeCaseResult = handle_edge_cases(df, sql)
    if edge.should_stop:
        _log_query(store, query_id, question, sql, True, "",
                   rows_returned=0, chart_type="none",
                   exec_ms=int((time.perf_counter()-pipeline_start)*1000),
                   retries=retries_used)
        return {
            "role": "assistant", "kind": "result", "query_id": query_id,
            "question": question, "status": "success", "sql": sql,
            "original_sql": _vr.original_sql if _auto_fixed else None,
            "auto_fixed": _auto_fixed,
            "df": edge.df if edge.df is not None else df,
            "edge_result": edge, "chart_type": "none", "chart_spec": None,
            "insight_text": "", "warning_message": edge.warning_message or "",
            "meta_text": "", "violations": [], "error_text": "",
            "summary": answer_summary,
        }

    working_df = edge.df if edge.df is not None else df

    if edge.force_chart_type:
        chart_type = edge.force_chart_type
        shape = analyze_data_shape(working_df, normalized)
    else:
        shape = analyze_data_shape(working_df, normalized)
        chart_type = select_chart_type(shape, chart_hint=chart_hint)

    spec = None
    insight_text = ""
    if chart_type not in ("kpi_card", "table"):
        spec = generate_chart_spec(working_df, shape, chart_type)
        num_col   = shape.numeric_cols[0] if shape.numeric_cols else None
        label_col = (shape.categorical_cols[0] if shape.categorical_cols else shape.temporal_col)
        spec, insights = annotate_insights(spec, working_df, chart_type,
                                           numeric_col=num_col, label_col=label_col)
        insight_text = insights.summary_text or ""

    total_ms = int((time.perf_counter()-pipeline_start)*1000)
    meta_parts = [f"SQL in {exec_time:.1f}s", f"{rows_returned:,} rows"]
    if chart_type:
        meta_parts.append(chart_type.replace("_", " "))

    _log_query(store, query_id, question, sql, True, "",
               rows_returned=rows_returned, exec_ms=total_ms,
               chart_type=chart_type, retries=retries_used)

    return {
        "role": "assistant", "kind": "result", "query_id": query_id,
        "question": question, "status": "success", "sql": sql,
        "original_sql": _vr.original_sql if _auto_fixed else None,
        "auto_fixed": _auto_fixed, "df": working_df, "edge_result": edge,
        "chart_type": chart_type, "chart_spec": spec, "insight_text": insight_text,
        "warning_message": edge.warning_message or "",
        "meta_text": f"⚡ {' · '.join(meta_parts)}",
        "violations": [], "error_text": "", "summary": answer_summary,
    }



# def _run_query_pipeline(
#     vn: MyVanna,
#     question: str,
#     store: Optional[SQLiteStore],
# ) -> Dict[str, Any]:
#     """
#     Execute the full 11-step text-to-SQL → chart pipeline.

#     All rendering is delegated to component modules.
#     All persistence is delegated to sqlite_store / feedback_collector.
#     """
#     query_id = str(uuid.uuid4())

#     pipeline_start = time.perf_counter()
#     retries_used = 0
#     chart_type = ""
#     error_message = ""

#     # ══════════════════════════════════════════════════════════════════
#     # 1. PREPROCESS
#     # ══════════════════════════════════════════════════════════════════
#     chart_hint = _extract_chart_hint(question)
#     clean_question = _clean_question_for_sql(question)
#     if chart_hint:
#         logger.info("Chart hint: '%s'", chart_hint)

#     # ══════════════════════════════════════════════════════════════════
#     # 2–5. GENERATE → VALIDATE → EXECUTE (all retries handled internally)
#     # ══════════════════════════════════════════════════════════════════
#     gen_start = time.perf_counter()
#     result: RecoveryResult = run_with_recovery(vn, clean_question)
#     gen_time = time.perf_counter() - gen_start

#     retries_used = max(0, result.attempt_count - 1)

#     # ── Security violation → hard stop (never retry) ───────────────
#     if result.is_security:
#         error_message = result.failure_reason
#         logger.warning("SECURITY | query_id=%s | %s", query_id, error_message)
#         _log_query(store, query_id, question, "", False, error_message)
#         return {
#             "role": "assistant",
#             "kind": "result",
#             "query_id": query_id,
#             "question": question,
#             "status": "security",
#             "sql": "",
#             "original_sql": None,
#             "auto_fixed": False,
#             "df": None,
#             "edge_result": None,
#             "chart_type": "",
#             "chart_spec": None,
#             "insight_text": "",
#             "warning_message": "",
#             "meta_text": "",
#             "violations": (
#                 result.last_attempt.validation_result.violations
#                 if result.last_attempt and result.last_attempt.validation_result
#                 else []
#             ),
#             "error_text": error_message,
#         }


#     # ── All retries exhausted ───────────────────────────────────────
#     if not result.success:
#         last_sql = result.last_attempt.display_sql if result.last_attempt else ""
#         error_message = result.failure_reason
#         _log_query(
#             store, query_id, question, last_sql, False, error_message,
#             retries=retries_used,
#         )
#         return {
#             "role": "assistant",
#             "kind": "result",
#             "query_id": query_id,
#             "question": question,
#             "status": "error",
#             "sql": last_sql,
#             "original_sql": None,
#             "auto_fixed": False,
#             "df": None,
#             "edge_result": None,
#             "chart_type": "",
#             "chart_spec": None,
#             "insight_text": "",
#             "warning_message": "",
#             "meta_text": "",
#             "violations": [],
#             "error_text": result.user_facing_error(),
#         }


#     # ── Pipeline succeeded ──────────────────────────────────────────
#     sql = result.sql
#     df = result.df
#     exec_time = result.total_elapsed_ms / 1000.0

#     # Cap rows per spec
#     if df is not None:
#         df = df.head(10_000)

#     rows_returned = len(df) if df is not None else 0

#     # ══════════════════════════════════════════════════════════════════
#     # 6. SHOW SQL (collapsible viewer)
#     # ══════════════════════════════════════════════════════════════════
#     _last_attempt = result.last_attempt
#     _vr = _last_attempt.validation_result if _last_attempt else None
#     _auto_fixed = bool(_vr and _vr.auto_fixed)

#     edge: EdgeCaseResult = handle_edge_cases(df, sql)

#     if edge.should_stop:
#         _log_query(store, query_id, question, sql, True, "",
#                    rows_returned=0, chart_type="none",
#                    exec_ms=int((time.perf_counter() - pipeline_start) * 1000),
#                    retries=retries_used)
#         return {
#             "role": "assistant",
#             "kind": "result",
#             "query_id": query_id,
#             "question": question,
#             "status": "success",
#             "sql": sql,
#             "original_sql": _vr.original_sql if _auto_fixed else None,
#             "auto_fixed": _auto_fixed,
#             "df": edge.df if edge.df is not None else df,
#             "edge_result": edge,
#             "chart_type": "none",
#             "chart_spec": None,
#             "insight_text": "",
#             "warning_message": edge.warning_message or "",
#             "meta_text": "",
#             "violations": [],
#             "error_text": "",
#         }


#     working_df = edge.df if edge.df is not None else df

#     # ══════════════════════════════════════════════════════════════════
#     # 8. CHART PIPELINE (analyse → select → generate → annotate)
#     # ══════════════════════════════════════════════════════════════════
#     if edge.force_chart_type:
#         chart_type = edge.force_chart_type
#         shape = analyze_data_shape(working_df, question)
#     else:
#         shape = analyze_data_shape(working_df, question)
#         chart_type = select_chart_type(shape, chart_hint=chart_hint)

#     spec = None
#     insight_text = ""


#     # ══════════════════════════════════════════════════════════════════
#     # 9 & 10. BUILD RESULT PAYLOAD
#     # ══════════════════════════════════════════════════════════════════
#     if chart_type not in ("kpi_card", "table"):
#         # Generate Vega-Lite spec
#         spec = generate_chart_spec(working_df, shape, chart_type)

#         # Determine primary columns for annotation
#         num_col = shape.numeric_cols[0] if shape.numeric_cols else None
#         label_col = (
#             shape.categorical_cols[0] if shape.categorical_cols
#             else shape.temporal_col
#         )

#         # Insight annotations (max/min lines, trend detection)
#         spec, insights = annotate_insights(
#             spec, working_df, chart_type,
#             numeric_col=num_col, label_col=label_col,
#         )
#         insight_text = insights.summary_text or ""

#     # ── Timing / meta caption ──────────────────────────────────────
#     total_ms = int((time.perf_counter() - pipeline_start) * 1000)
#     meta_parts = []
#     if gen_time:
#         meta_parts.append(f"SQL in {gen_time:.1f}s")
#     if exec_time:
#         meta_parts.append(f"executed in {exec_time:.1f}s")
#     meta_parts.append(f"{rows_returned:,} rows")
#     if chart_type:
#         meta_parts.append(chart_type.replace("_", " "))

#     # ══════════════════════════════════════════════════════════════════
#     # 6 (deferred). LOG QUERY TO SQLITE
#     # ══════════════════════════════════════════════════════════════════
#     _log_query(
#         store, query_id, question, sql, True, "",
#         rows_returned=rows_returned,
#         exec_ms=total_ms,
#         chart_type=chart_type,
#         retries=retries_used,
#     )

#     return {
#         "role": "assistant",
#         "kind": "result",
#         "query_id": query_id,
#         "question": question,
#         "status": "success",
#         "sql": sql,
#         "original_sql": _vr.original_sql if _auto_fixed else None,
#         "auto_fixed": _auto_fixed,
#         "df": working_df,
#         "edge_result": edge,
#         "chart_type": chart_type,
#         "chart_spec": spec,
#         "insight_text": insight_text,
#         "warning_message": edge.warning_message or "",
#         "meta_text": f"⚡ {' · '.join(meta_parts)}",
#         "violations": [],
#         "error_text": "",
#     }


def _background_query_worker(
    job_state: Dict[str, Any],
    vn: MyVanna,
    question: str,
    store: Optional[SQLiteStore],
) -> None:
    """Run the full query pipeline off the Streamlit thread."""
    try:
        job_state["payload"] = _run_query_pipeline(vn, question, store)
    except Exception as exc:
        logger.exception("Background query crashed: %s", exc)
        job_state["payload"] = {
            "role": "assistant",
            "kind": "result",
            "query_id": str(uuid.uuid4()),
            "question": question,
            "status": "error",
            "sql": "",
            "original_sql": None,
            "auto_fixed": False,
            "df": None,
            "edge_result": None,
            "chart_type": "",
            "chart_spec": None,
            "insight_text": "",
            "warning_message": "",
            "meta_text": "",
            "violations": [],
            "error_text": f"Background worker crashed: {exc}",
        }
    finally:
        job_state["status"] = "done"


def _start_background_query(
    vn: MyVanna,
    question: str,
    store: Optional[SQLiteStore],
) -> None:
    """Submit a query to a daemon worker thread and track it in session state."""
    job_state: Dict[str, Any] = {
        "status": "running",
        "question": question,
        "submitted_at": time.time(),
        "payload": None,
    }

    worker = threading.Thread(
        target=_background_query_worker,
        args=(job_state, vn, question, store),
        daemon=True,
        name="query-worker",
    )
    st.session_state["active_job"] = job_state
    worker.start()


def _finalize_completed_query() -> bool:
    """Move a finished background job payload into chat history exactly once."""
    active_job = st.session_state.get("active_job")
    if not active_job or active_job.get("status") != "done":
        return False

    payload = active_job.get("payload")
    st.session_state["active_job"] = None
    if not payload:
        return False

    st.session_state["last_question"] = payload.get("question", "")
    st.session_state["last_query_id"] = payload.get("query_id", "")
    st.session_state["last_sql"] = payload.get("sql", "")
    st.session_state["last_df"] = payload.get("df")
    st.session_state["last_chart_spec"] = payload.get("chart_spec")
    st.session_state["last_chart_type"] = payload.get("chart_type", "")
    st.session_state[f"result_open_{payload['query_id']}"] = True
    st.session_state["messages"].append(payload)
    return True


def _render_active_job() -> None:
    """Show a non-blocking placeholder while a background query is running."""
    active_job = st.session_state.get("active_job")
    if not active_job or active_job.get("status") != "running":
        return

    elapsed = time.time() - active_job.get("submitted_at", time.time())
    st.info("Query is running in the background")
    st.caption(f"Working on: {active_job['question']} · {elapsed:.1f}s elapsed")



# ═══════════════════════════════════════════════════════════════════════════
# SQLITE LOGGING HELPER
# ═══════════════════════════════════════════════════════════════════════════
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
    """Log a query to SQLite. No-op if store is None."""
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


# ═══════════════════════════════════════════════════════════════════════════
# FEEDBACK SECTION
# ═══════════════════════════════════════════════════════════════════════════
def _render_feedback_section(
    vn: MyVanna,
    question: str,
    sql: str,
    query_id: str,
    store: Optional[SQLiteStore],
) -> None:
    """
    Render 👍 👎 ✏️ feedback buttons with full persistence integration.

    👍 → record_positive_feedback  (vn.train + SQLite log)
    👎 → record_negative_feedback  (SQLite log only — NO training)
    ✏️ → editable SQL area → validate → execute →
         record_corrected_feedback (vn.train + SQLite log)
    """
    if not sql or not sql.strip():
        return

    st.divider()
    st.markdown(
        "<p style='font-size:12.5px; color:#94A3B8; margin-bottom:8px; "
        "font-weight:500;'>Was this result helpful?</p>",
        unsafe_allow_html=True,
    )

    busy = _has_running_query()
    col_up, col_down, col_edit, _ = st.columns([1, 1, 1, 5])

    # ── 👍 Positive ───────────────────────────────────────────────
    with col_up:
        if st.button("👍 Correct", key=f"fb_up_{query_id}",
                      help="Mark correct — saves Q→SQL for training",
                      use_container_width=True,
                      disabled=busy):
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

    # ── 👎 Negative ───────────────────────────────────────────────
    with col_down:
        if st.button("👎 Wrong", key=f"fb_down_{query_id}",
                      help="Mark wrong — will NOT train on this",
                      use_container_width=True,
                      disabled=busy):
            if _HAS_FEEDBACK:
                record_negative_feedback(
                    question, sql, query_id, sqlite_store=store,
                )
            st.warning("📝 Noted — this will not be used for training.")
            logger.info("NEGATIVE | query_id=%s", query_id)

    # ── ✏️ Edit ───────────────────────────────────────────────────
    with col_edit:
        if st.button("✏️ Edit SQL", key=f"fb_edit_{query_id}",
                      help="Correct the SQL yourself",
                      use_container_width=True,
                      disabled=busy):
            st.session_state[f"show_edit_{query_id}"] = True

    # ── Editable SQL area ──────────────────────────────────────────
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
                                  type="primary", use_container_width=True,
                                  disabled=busy)
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



def _render_saved_result(
    vn: MyVanna,
    store: Optional[SQLiteStore],
    msg: Dict[str, Any],
) -> None:
    query_id = msg["query_id"]
    toggle_key = f"result_open_{query_id}"
    status = msg.get("status", "success")

    # ── Early exits (greeting / irrelevant / suspicious / ambiguous) ──
    # These have no toggle — just render the reply text directly
    if status == "early_exit":
        st.markdown(msg.get("reply_text", ""))
        return

    # ── For all pipeline results, show expand/collapse toggle ─────────
    if toggle_key not in st.session_state:
        st.session_state[toggle_key] = True

    head_col, ctrl_col = st.columns([8.5, 1.5])
    with head_col:
        st.markdown(f"**Results for:** {msg['question']}")
    with ctrl_col:
        is_open = st.session_state[toggle_key]
        button_label = "Expand" if is_open else "Collapse"
        if st.button(
            button_label,
            key=f"toggle_result_{query_id}",
            use_container_width=True,
        ):
            st.session_state[toggle_key] = not is_open

    if not st.session_state[toggle_key]:
        return

    # ── Security violation ────────────────────────────────────────────
    if status == "security":
        st.error("🚫 Security violation — this query was blocked.")
        for v in msg.get("violations", []):
            st.warning(v)
        return

    # ── Pipeline error ────────────────────────────────────────────────
    if status == "error":
        st.error(msg.get("error_text") or "Query failed.")
        if msg.get("sql"):
            render_sql_viewer(
                sql=msg["sql"],
                original_sql=msg.get("original_sql"),
                auto_fixed=msg.get("auto_fixed", False),
                key_suffix=query_id,
            )
        return

    # ── Success ───────────────────────────────────────────────────────

    # 1. Plain-English summary FIRST (before all structured outputs)
    summary = msg.get("summary", "")
    if summary:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #FFF1F2, #FFF8F9);
                border-left: 4px solid #E11D48;
                border-radius: 10px;
                padding: 14px 18px;
                margin-bottom: 16px;
                font-size: 14.5px;
                color: #0F172A;
                line-height: 1.7;
            ">
            {summary}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 2. Warning message (edge cases)
    if msg.get("warning_message"):
        st.info(msg["warning_message"])

    # 3. SQL viewer
    render_sql_viewer(
        sql=msg["sql"],
        original_sql=msg.get("original_sql"),
        auto_fixed=msg.get("auto_fixed", False),
        key_suffix=query_id,
    )

    chart_type = msg.get("chart_type", "")

    # 4. Chart / table / KPI
    if chart_type == "kpi_card":
        render_kpi_card(msg.get("df"), edge_result=msg.get("edge_result"))

    elif chart_type == "table":
        render_data_table(
            msg.get("df"),
            title="Query Results",
            key_suffix=query_id,
        )

    elif chart_type == "none":
        pass

    else:
        render_chart(msg["chart_spec"], chart_type=chart_type)

        if msg.get("insight_text"):
            st.caption(f"💡 {msg['insight_text']}")

        with st.expander("📊 View Data", expanded=False):
            render_data_table(
                msg.get("df"),
                title="",
                show_download=True,
                compact=True,
                key_suffix=f"inline_{query_id}",
            )

    # 5. Meta timing
    if msg.get("meta_text"):
        st.caption(msg["meta_text"])

    # 6. Feedback
    _render_feedback_section(vn, msg["question"], msg["sql"], query_id, store)




# def _render_saved_result(
#     vn: MyVanna,
#     store: Optional[SQLiteStore],
#     msg: Dict[str, Any],
# ) -> None:
#     query_id = msg["query_id"]
#     toggle_key = f"result_open_{query_id}"

#     if toggle_key not in st.session_state:
#         st.session_state[toggle_key] = True

#     # head_col, ctrl_col = st.columns([8, 1])
#     # with head_col:
#     #     st.markdown(f"**Results for:** {msg['question']}")
#     # with ctrl_col:
#     #     st.toggle("Open", key=toggle_key, label_visibility="collapsed")
#     head_col, ctrl_col = st.columns([8.5, 1.5])
#     with head_col:
#         st.markdown(f"**Results for:** {msg['question']}")
#     with ctrl_col:
#         is_open = st.session_state[toggle_key]
#         button_label = "Expand" if is_open else "Collapse"

#         if st.button(
#             button_label,
#             key=f"toggle_result_{query_id}",
#             use_container_width=True,
#         ):
#             st.session_state[toggle_key] = not is_open

#     if not st.session_state[toggle_key]:
#         return

#     status = msg.get("status", "success")

#     if status == "security":
#         st.error("🚫 Security violation — this query was blocked.")
#         for v in msg.get("violations", []):
#             st.warning(v)
#         return

#     if status == "error":
#         st.error(msg.get("error_text") or "Query failed.")
#         if msg.get("sql"):
#             render_sql_viewer(
#                 sql=msg["sql"],
#                 original_sql=msg.get("original_sql"),
#                 auto_fixed=msg.get("auto_fixed", False),
#                 key_suffix=query_id,
#             )
#         return

#     if msg.get("warning_message"):
#         st.info(msg["warning_message"])

#     render_sql_viewer(
#         sql=msg["sql"],
#         original_sql=msg.get("original_sql"),
#         auto_fixed=msg.get("auto_fixed", False),
#         key_suffix=query_id,
#     )

#     chart_type = msg.get("chart_type", "")

#     if chart_type == "kpi_card":
#         render_kpi_card(msg.get("df"), edge_result=msg.get("edge_result"))

#     elif chart_type == "table":
#         render_data_table(
#             msg.get("df"),
#             title="Query Results",
#             key_suffix=query_id,
#         )

#     elif chart_type == "none":
#         pass

#     else:
#         render_chart(msg["chart_spec"], chart_type=chart_type)

#         if msg.get("insight_text"):
#             st.caption(f"💡 {msg['insight_text']}")

#         with st.expander("📊 View Data", expanded=False):
#             render_data_table(
#                 msg.get("df"),
#                 title="",
#                 show_download=True,
#                 compact=True,
#                 key_suffix=f"inline_{query_id}",
#             )

#     if msg.get("meta_text"):
#         st.caption(msg["meta_text"])

#     _render_feedback_section(vn, msg["question"], msg["sql"], query_id, store)
 
# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main() -> None:
    """Application entry point — initialise resources and run the chat loop."""
    _init_session_state()

    # ── Load resources ─────────────────────────────────────────────
    try:
        vn = _load_vanna()
    except Exception as exc:
        st.error(f"❌ Failed to initialise AI engine: {exc}")
        st.info(
            "Checklist:\n"
            "1. Is Ollama running? (ollama serve)\n"
            "2. Is qwen3:9b pulled? (ollama pull qwen3:9b)\n"
            "3. Are DB credentials set in .env?\n"
            "4. Run python scripts/test_connection.py to diagnose."
        )
        logger.critical("Vanna init failed: %s", exc, exc_info=True)
        st.stop()

    store = _load_store()

    st.session_state["vn"] = vn
    st.session_state["sqlite_store"] = store
    _finalize_completed_query()

    # ── Sidebar ────────────────────────────────────────────────────
    _render_sidebar(vn, store)

    # ── Header ─────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="
            padding:0 0 20px 0; margin-bottom:20px;
            border-bottom:2px solid #FFF1F2;
        ">
            <div style="display:flex; align-items:center; gap:14px; margin-bottom:8px;">
                <div style="
                    width:44px; height:44px; min-width:44px;
                    background: linear-gradient(135deg, #E11D48, #FB7185);
                    border-radius:12px; display:flex; align-items:center;
                    justify-content:center; font-size:22px;
                    box-shadow: 0 3px 10px rgba(225, 29, 72, 0.2);
                ">💬</div>
                <h1 style="
                    margin:0; color:#0F172A; font-weight:800;
                    font-size:24px; letter-spacing:-0.03em; line-height:1.2;
                ">Ask about your data</h1>
            </div>
            <p style="
                margin:0; color:#64748B; font-size:13.5px;
                line-height:1.6; padding-left:58px;
            ">
                Type a question in plain English. The AI generates SQL,
                runs it on your database, and visualises the results.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Render chat history ────────────────────────────────────────

    for msg in st.session_state["messages"]:
        if msg.get("kind") == "result":
            with st.chat_message("assistant"):
                _render_saved_result(vn, store, msg)
        else:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
 
    # for msg in st.session_state["messages"]:
    #     with st.chat_message(msg["role"]):
    #         st.markdown(msg["content"])

    # ── Pending question (from sidebar sample click) ───────────────
    pending = st.session_state.pop("pending_question", None)

    # ── Chat input ─────────────────────────────────────────────────
    user_input = st.chat_input(
        "Ask a question about Charmacy Milano sales data…",
        disabled=_has_running_query(),
    )

    question = pending or user_input

    if question and not _has_running_query():
        # Record user message
        st.session_state["messages"].append({
            "role": "user", "kind": "text", "content": question,
        })
        _start_background_query(vn, question, store)
        st.rerun()

    active_job = st.session_state.get("active_job")
    if active_job and active_job.get("status") == "running":
        with st.chat_message("assistant"):
            _render_active_job()
        time.sleep(0.75)
        st.rerun()



# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    main()
