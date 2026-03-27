"""
feedback/feedback_collector.py

Handles 👍 👎 ✏️ feedback from the Streamlit UI.

Responsibilities:
    - Log every feedback event to SQLite (persistence/sqlite_store.py)
    - On 👍 (positive): call vn.train(question, sql) + log
    - On 👎 (negative): log only — do NOT train
    - On ✏️ (corrected): validate + execute corrected SQL,
      then vn.train(question, corrected_sql) + log as "corrected"

This module is the bridge between the UI (feedback_bar.py) and the
persistence layer (sqlite_store.py) + training layer (vanna).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from core.vanna_instance import MyVanna
from core.sql_validator import validate_sql

logger = logging.getLogger(__name__)


# ── Feedback types ───────────────────────────────────────────────────────────
FEEDBACK_POSITIVE = "positive"
FEEDBACK_NEGATIVE = "negative"
FEEDBACK_CORRECTED = "corrected"


def record_positive_feedback(
    vn: MyVanna,
    question: str,
    sql: str,
    query_id: str,
    sqlite_store: Any = None,
) -> dict:
    """
    Process positive feedback (👍).

    Actions:
        1. Train Vanna with this Q→SQL pair
        2. Log to SQLite

    Args:
        vn:           Vanna instance for training
        question:     The user's question
        sql:          The SQL that was executed
        query_id:     Unique query identifier
        sqlite_store: SQLite store instance (optional)

    Returns:
        {"success": bool, "message": str, "trained": bool}
    """
    result = {"success": False, "message": "", "trained": False}

    # Train Vanna
    try:
        vn.train(question=question, sql=sql)
        result["trained"] = True
        logger.info(
            "Positive feedback trained | query_id=%s | q='%s'",
            query_id, question[:80],
        )
    except Exception as exc:
        logger.error("Training failed on positive feedback: %s", exc)
        result["message"] = f"Training failed: {exc}"

    # Log to SQLite
    if sqlite_store:
        try:
            sqlite_store.log_feedback(
                query_id=query_id,
                question=question,
                sql=sql,
                feedback=FEEDBACK_POSITIVE,
                trained=result["trained"],
            )
        except Exception as exc:
            logger.warning("SQLite feedback log failed: %s", exc)

    result["success"] = True
    result["message"] = (
        "Q→SQL pair saved for training." if result["trained"]
        else "Feedback recorded, but training failed."
    )
    return result


def record_negative_feedback(
    question: str,
    sql: str,
    query_id: str,
    reason: str = "",
    sqlite_store: Any = None,
) -> dict:
    """
    Process negative feedback (👎).

    Actions:
        1. Log to SQLite (do NOT train Vanna)

    Args:
        question:     The user's question
        sql:          The SQL that was executed
        query_id:     Unique query identifier
        reason:       Optional reason text from user
        sqlite_store: SQLite store instance (optional)

    Returns:
        {"success": bool, "message": str}
    """
    logger.info(
        "Negative feedback | query_id=%s | q='%s' | reason='%s'",
        query_id, question[:80], reason[:100],
    )

    if sqlite_store:
        try:
            sqlite_store.log_feedback(
                query_id=query_id,
                question=question,
                sql=sql,
                feedback=FEEDBACK_NEGATIVE,
                trained=False,
                notes=reason,
            )
        except Exception as exc:
            logger.warning("SQLite feedback log failed: %s", exc)

    return {
        "success": True,
        "message": "Feedback recorded. This will NOT be used for training.",
    }


def record_corrected_feedback(
    vn: MyVanna,
    question: str,
    original_sql: str,
    corrected_sql: str,
    query_id: str,
    sqlite_store: Any = None,
) -> dict:
    """
    Process corrected feedback (✏️).

    Actions:
        1. Validate corrected SQL (security check)
        2. Execute corrected SQL to verify it works
        3. Train Vanna with question + corrected_sql
        4. Log to SQLite as "corrected"

    Args:
        vn:            Vanna instance
        question:      The user's question
        original_sql:  The original SQL that was generated
        corrected_sql: The user-corrected SQL
        query_id:      Unique query identifier
        sqlite_store:  SQLite store instance (optional)

    Returns:
        {"success": bool, "message": str, "trained": bool, "df": DataFrame|None}
    """
    result = {"success": False, "message": "", "trained": False, "df": None}

    if not corrected_sql or not corrected_sql.strip():
        result["message"] = "Corrected SQL cannot be empty."
        return result

    # Step 1: Validate
    val_result = validate_sql(corrected_sql)
    if val_result.is_security_violation:
        result["message"] = "Security violation in corrected SQL: " + \
                            val_result.violation_summary()
        logger.warning(
            "Corrected SQL security violation | query_id=%s", query_id
        )
        return result

    final_sql = val_result.fixed_sql

    # Step 2: Execute
    try:
        df = vn.run_sql(final_sql)
        result["df"] = df
    except Exception as exc:
        result["message"] = f"Corrected SQL execution failed: {exc}"
        logger.error("Corrected SQL execution failed: %s", exc)
        return result

    # Step 3: Train
    try:
        vn.train(question=question, sql=final_sql)
        result["trained"] = True
        logger.info(
            "Corrected feedback trained | query_id=%s | q='%s'",
            query_id, question[:80],
        )
    except Exception as exc:
        logger.error("Training failed on corrected feedback: %s", exc)

    # Step 4: Log
    if sqlite_store:
        try:
            sqlite_store.log_feedback(
                query_id=query_id,
                question=question,
                sql=final_sql,
                feedback=FEEDBACK_CORRECTED,
                trained=result["trained"],
                original_sql=original_sql,
            )
        except Exception as exc:
            logger.warning("SQLite feedback log failed: %s", exc)

    result["success"] = True
    result["message"] = (
        "Corrected SQL trained successfully." if result["trained"]
        else "SQL executed OK, but training failed."
    )
    return result


__all__ = [
    "record_positive_feedback",
    "record_negative_feedback",
    "record_corrected_feedback",
    "FEEDBACK_POSITIVE",
    "FEEDBACK_NEGATIVE",
    "FEEDBACK_CORRECTED",
]