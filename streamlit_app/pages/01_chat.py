"""
streamlit_app/pages/01_chat.py

Main Q&A chat page — the primary user-facing interface.

Runs the full pipeline:
    question → preprocess → SQL generation → validation → execution →
    edge cases → chart pipeline → render → feedback
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# ── Project root ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.vanna_instance import get_vanna, MyVanna
from streamlit_app.components.chat import (
    init_chat_session_state,
    render_chat_history,
    render_sidebar,
    run_query_pipeline,
)

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Chat — Charmacy Milano AI",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def _load_vanna() -> MyVanna:
    return get_vanna()


def main() -> None:
    init_chat_session_state()

    # ── Load Vanna ───────────────────────────────────────────────────────
    try:
        vn = _load_vanna()
    except Exception as exc:
        st.error(f"❌ Failed to initialise Vanna: {exc}")
        st.info(
            "Check your `.env` file and `config/database.yaml`. "
            "Run `python scripts/test_connection.py` to diagnose."
        )
        st.stop()

    st.session_state["vn"] = vn

    # ── Sidebar ──────────────────────────────────────────────────────────
    render_sidebar(vn)

    # ── Header ───────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="padding:8px 0 20px 0;">
            <h1 style="margin:0; color:#1a1a2e; font-weight:700; font-size:28px;">
                💬 Ask about your data
            </h1>
            <p style="margin:4px 0 0 0; color:#6b7280; font-size:14px;">
                Ask questions in plain English about Charmacy Milano sales data.
                The AI will generate SQL, run it, and visualise the results.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Chat history ─────────────────────────────────────────────────────
    render_chat_history()

    # ── Handle pending question from sidebar ─────────────────────────────
    pending = st.session_state.pop("pending_question", None)

    # ── Chat input ───────────────────────────────────────────────────────
    user_input = st.chat_input(
        "Ask a question about Charmacy Milano sales data…",
        key="chat_input_main",
    )

    question = pending or user_input

    if question:
        # Add user message to history
        st.session_state["messages"].append({
            "role": "user",
            "content": question,
        })
        with st.chat_message("user"):
            st.markdown(question)

        # Run the full pipeline
        with st.chat_message("assistant"):
            run_query_pipeline(vn, question)

        # Add assistant placeholder to history
        st.session_state["messages"].append({
            "role": "assistant",
            "content": f"*(Results for: {question})*",
        })


if __name__ == "__main__":
    main()
else:
    # When loaded as a Streamlit page (via pages/ directory)
    main()