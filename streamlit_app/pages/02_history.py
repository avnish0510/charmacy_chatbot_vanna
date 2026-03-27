"""
streamlit_app/pages/02_history.py

Query history page — displays past queries from SQLite persistence.

Shows:
    - Timestamped list of past questions, SQL, and outcomes
    - Re-run any historical query
    - Filter by date, feedback status
    - Basic accuracy analytics

Note: This page reads from the SQLite store (persistence/queries.db).
      If the persistence layer isn't set up yet, it shows the in-memory
      session history as a fallback.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="History — Charmacy Milano AI",
    page_icon="📜",
    layout="wide",
)

# ── SQLite persistence (optional) ───────────────────────────────────────────
QUERIES_DB = ROOT / "persistence" / "queries.db"


def _load_sqlite_history() -> Optional[pd.DataFrame]:
    """
    Attempt to load query history from SQLite.
    Returns None if the DB or table doesn't exist.
    """
    if not QUERIES_DB.exists():
        return None

    try:
        import sqlite3
        conn = sqlite3.connect(str(QUERIES_DB))
        # Check if table exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='query_log'"
        )
        if not cursor.fetchone():
            conn.close()
            return None

        df = pd.read_sql_query(
            """
            SELECT
                id,
                timestamp,
                question,
                sql_generated,
                success,
                feedback,
                error_message,
                rows_returned
            FROM query_log
            ORDER BY timestamp DESC
            LIMIT 500
            """,
            conn,
        )
        conn.close()
        return df if not df.empty else None

    except Exception:
        return None


def _session_history_fallback() -> pd.DataFrame:
    """
    Build a history DataFrame from session state messages.
    Used when SQLite persistence isn't available.
    """
    messages = st.session_state.get("messages", [])
    if not messages:
        return pd.DataFrame()

    rows = []
    for i, msg in enumerate(messages):
        if msg["role"] == "user":
            rows.append({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "question": msg["content"],
                "sql_generated": st.session_state.get("last_sql", ""),
                "feedback": "-",
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def main() -> None:
    # ── Header ───────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="padding:8px 0 20px 0;">
            <h1 style="margin:0; color:#1a1a2e; font-weight:700; font-size:28px;">
                📜 Query History
            </h1>
            <p style="margin:4px 0 0 0; color:#6b7280; font-size:14px;">
                Browse past questions, SQL queries, and results.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Load history ─────────────────────────────────────────────────────
    sqlite_df = _load_sqlite_history()

    if sqlite_df is not None and not sqlite_df.empty:
        history_df = sqlite_df
        source = "SQLite"
    else:
        history_df = _session_history_fallback()
        source = "Session"

    if history_df.empty:
        st.info(
            "No query history yet. Ask some questions on the "
            "**💬 Chat** page to build history."
        )
        return

    st.caption(f"Source: {source} · {len(history_df)} queries")

    # ── Filters ──────────────────────────────────────────────────────────
    with st.expander("🔍 Filters", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            search_text = st.text_input(
                "Search questions:",
                placeholder="e.g. revenue, platform",
            )

        with col2:
            if "feedback" in history_df.columns:
                feedback_options = ["All"] + sorted(
                    history_df["feedback"].dropna().unique().tolist()
                )
                feedback_filter = st.selectbox("Feedback:", feedback_options)
            else:
                feedback_filter = "All"

        with col3:
            if "success" in history_df.columns:
                status_options = ["All", "Success", "Failed"]
                status_filter = st.selectbox("Status:", status_options)
            else:
                status_filter = "All"

    # ── Apply filters ────────────────────────────────────────────────────
    filtered = history_df.copy()

    if search_text:
        mask = filtered["question"].str.contains(
            search_text, case=False, na=False
        )
        filtered = filtered[mask]

    if feedback_filter != "All" and "feedback" in filtered.columns:
        filtered = filtered[filtered["feedback"] == feedback_filter]

    if status_filter != "All" and "success" in filtered.columns:
        if status_filter == "Success":
            filtered = filtered[filtered["success"] == 1]
        else:
            filtered = filtered[filtered["success"] == 0]

    # ── Stats ────────────────────────────────────────────────────────────
    st.divider()
    stat_cols = st.columns(4)

    with stat_cols[0]:
        st.metric("Total Queries", len(filtered))

    if "success" in filtered.columns:
        success_count = int(filtered["success"].sum())
        with stat_cols[1]:
            st.metric("Successful", success_count)

        with stat_cols[2]:
            rate = (success_count / len(filtered) * 100) if len(filtered) > 0 else 0
            st.metric("Success Rate", f"{rate:.1f}%")

    if "feedback" in filtered.columns:
        positive = int((filtered["feedback"] == "positive").sum())
        with stat_cols[3]:
            st.metric("👍 Positive Feedback", positive)

    # ── History table ────────────────────────────────────────────────────
    st.divider()

    display_cols = [c for c in [
        "timestamp", "question", "sql_generated", "feedback",
        "success", "rows_returned", "error_message"
    ] if c in filtered.columns]

    column_config = {}
    if "question" in display_cols:
        column_config["question"] = st.column_config.TextColumn(
            "Question", width="large"
        )
    if "sql_generated" in display_cols:
        column_config["sql_generated"] = st.column_config.TextColumn(
            "SQL", width="medium"
        )
    if "timestamp" in display_cols:
        column_config["timestamp"] = st.column_config.TextColumn(
            "Time", width="small"
        )

    st.dataframe(
        filtered[display_cols],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )

    # ── Re-run a query ───────────────────────────────────────────────────
    st.divider()
    st.markdown("**🔄 Re-run a past query**")
    if "question" in filtered.columns and len(filtered) > 0:
        selected_question = st.selectbox(
            "Select a past question:",
            options=filtered["question"].tolist(),
            index=0,
        )
        if st.button("Re-run this question →", type="primary"):
            st.session_state["pending_question"] = selected_question
            st.switch_page("pages/01_chat.py")


if __name__ == "__main__":
    main()
else:
    main()