
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

# ── Project root ───────────────────────────────────────────────────────────
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

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Chat — Charmacy Milano AI",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Page-level CSS (matches the main app palette: Slate + Rose) ────────────
_PAGE_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont,
                     'Segoe UI', Roboto, sans-serif;
        -webkit-font-smoothing: antialiased;
    }

    #MainMenu, footer, header { visibility: hidden; }

    .main > .block-container {
        padding-top: 1.8rem;
        padding-bottom: 3rem;
        max-width: 1100px;
    }

    section[data-testid="stSidebar"] {
        background: linear-gradient(
            180deg, #F8FAFC 0%, #FFFFFF 55%, #FFF1F2 100%
        ) !important;
        border-right: 1px solid #E2E8F0 !important;
    }

    section[data-testid="stSidebar"] > div:first-child {
        padding-top: 0.4rem;
    }

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

    div[data-testid="stChatMessage"] {
        border-radius: 14px !important;
        border: 1px solid #F1F5F9 !important;
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04) !important;
        margin-bottom: 10px !important;
    }

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

    .streamlit-expanderHeader {
        font-weight: 600 !important;
        color: #334155 !important;
        font-size: 13px !important;
    }

    details {
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
    }

    button[data-testid="baseButton-primary"] {
        background: linear-gradient(135deg, #E11D48, #F43F5E) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        box-shadow: 0 2px 6px rgba(225, 29, 72, 0.2) !important;
    }

    button[data-testid="baseButton-primary"]:hover {
        background: linear-gradient(135deg, #BE123C, #E11D48) !important;
        box-shadow: 0 4px 14px rgba(225, 29, 72, 0.3) !important;
    }

    button[data-testid="baseButton-secondary"] {
        border-radius: 8px !important;
        border: 1px solid #E2E8F0 !important;
        color: #475569 !important;
        font-weight: 500 !important;
    }

    button[data-testid="baseButton-secondary"]:hover {
        border-color: #FDA4AF !important;
        color: #E11D48 !important;
        background: #FFF1F2 !important;
    }

    hr { border-color: #F1F5F9 !important; }

    pre {
        background: #F8FAFC !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
        overflow: hidden !important;
    }

    ::-webkit-scrollbar       { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: #CBD5E1; border-radius: 10px; }
    ::-webkit-scrollbar-thumb:hover { background: #94A3B8; }

    textarea {
        border-radius: 10px !important;
        border: 1px solid #E2E8F0 !important;
    }
    textarea:focus {
        border-color: #F43F5E !important;
        box-shadow: 0 0 0 3px rgba(244, 63, 94, 0.08) !important;
    }
</style>
"""
st.markdown(_PAGE_CSS, unsafe_allow_html=True)


@st.cache_resource
def _load_vanna() -> MyVanna:
    return get_vanna()


def main() -> None:
    init_chat_session_state()

    # ── Load Vanna ─────────────────────────────────────────────────
    try:
        vn = _load_vanna()
    except Exception as exc:
        st.error(f"❌ Failed to initialise Vanna: {exc}")
        st.info(
            "Check your .env file and config/database.yaml. "
            "Run python scripts/test_connection.py to diagnose."
        )
        st.stop()

    st.session_state["vn"] = vn

    # ── Sidebar ────────────────────────────────────────────────────
    render_sidebar(vn)

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
                Ask questions in plain English about Charmacy Milano sales data.
                The AI will generate SQL, run it, and visualise the results.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Chat history ───────────────────────────────────────────────
    render_chat_history()

    # ── Handle pending question from sidebar ───────────────────────
    pending = st.session_state.pop("pending_question", None)

    # ── Chat input ─────────────────────────────────────────────────
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