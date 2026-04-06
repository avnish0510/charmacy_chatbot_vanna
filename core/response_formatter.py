"""
core/response_formatter.py

Generates a plain-English summary of query results using the LLM.
The summary is based on the actual data, not a generic template.

Usage:
    from core.response_formatter import generate_answer_summary
    summary = generate_answer_summary(vn, question, df, chart_type)
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Optional

import pandas as pd

if TYPE_CHECKING:
    from core.vanna_instance import MyVanna

logger = logging.getLogger(__name__)

# Max rows sent to LLM for summarization — keeps token usage low
_MAX_ROWS_FOR_SUMMARY = 20

_SYSTEM_PROMPT = """You are a helpful business analytics assistant for Charmacy Milano, a cosmetics brand.

Your job: Given a user's question and the actual query result, write a SHORT plain-English summary.

Rules:
- Base your answer ONLY on the data shown — never guess or add information
- Be specific: mention actual numbers, top items, trends, or key observations from the data
- Use simple, clear language that non-technical business users can understand
- Do NOT mention SQL, databases, queries, or any technical terms
- Do NOT start with phrases like "Based on the data" or "According to the results"
- Just state the finding directly and confidently
- Length: 2 to 4 sentences maximum
- Always use the ₹ symbol for currency values, not $ or USD
- If the data is empty, say so clearly and briefly
"""


def _df_to_text(df: pd.DataFrame, max_rows: int = _MAX_ROWS_FOR_SUMMARY) -> str:
    """Convert a DataFrame to a compact readable text for the LLM."""
    if df is None or df.empty:
        return "(no rows returned)"
    try:
        return df.head(max_rows).to_string(index=False)
    except Exception:
        return f"({len(df)} rows, columns: {', '.join(df.columns.tolist())})"


def generate_answer_summary(
    vn: "MyVanna",
    question: str,
    df: Optional[pd.DataFrame],
    chart_type: str = "",
    summary_model: str = "",
) -> str:
    """
    Generate a concise plain-English answer based on actual query results.

    Args:
        vn:         The MyVanna singleton (for Ollama access).
        question:   The original (or normalized) user question.
        df:         The pandas DataFrame returned by the SQL query.
        chart_type: The selected chart type (informational only, not used in prompt).

    Returns:
        A 2-4 sentence plain-English summary string.
        Falls back to a simple row-count message if LLM call fails.
    """
    if df is None or df.empty:
        return (
            "The query ran successfully but returned no matching data for your question. "
            "This may be because of active filters (e.g. date range, net sales exclusions) "
            "or the data simply doesn't exist for this combination."
        )

    rows = len(df)
    cols = df.columns.tolist()
    data_text = _df_to_text(df)

    user_prompt = (
        f"Question: {question}\n\n"
        f"Query result ({rows} rows, columns: {', '.join(cols)}):\n"
        f"{data_text}\n\n"
        f"Write a 2-4 sentence plain-English summary of what this data shows."
    )

    try:

        # Use smaller/faster model if provided, else fall back to main model
        model_to_use = summary_model if summary_model else vn.model

        response = vn.ollama_client.chat(
            model=model_to_use,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            stream=False,
            think=False,
            options={**vn.ollama_options, "num_predict": 150},
        )
        summary = response["message"]["content"].strip()

        # Strip any accidental think blocks
        summary = re.sub(r"<think>.*?</think>", "", summary, flags=re.DOTALL).strip()

        if summary:
            logger.info("Answer summary generated (%d chars).", len(summary))
            return summary

    except Exception as exc:
        logger.warning("Answer summary generation failed: %s", exc)

    # Safe fallback
    return (
        f"The query returned **{rows:,} row(s)** across "
        f"{len(cols)} column(s): {', '.join(cols[:5])}{'…' if len(cols) > 5 else ''}."
    )


__all__ = ["generate_answer_summary"]