"""
core/error_recovery.py

Auto-retry orchestrator for the Charmacy Milano Text-to-SQL pipeline.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ROLE IN THE PIPELINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This module is the SINGLE entry point for the full query pipeline.

Caller (01_chat.py) does exactly this:

    from core.error_recovery import run_with_recovery

    result = run_with_recovery(vn, enriched_question)

    if result.success:
        df  = result.df            # pandas DataFrame → chart / table
        sql = result.sql           # validated T-SQL  → SQL viewer panel
    else:
        show_error_ui(result)      # error panel with all attempt details

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THREE-ATTEMPT STRATEGY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Attempt 1 — original question (no correction context)
    generate_sql(original_question) → validate → execute
    ↓ security fail  → HARD STOP, never retry, return immediately
    ↓ sanity fail    → build correction context → attempt 2
    ↓ execution fail → build execution error context → attempt 2
    ↓ success        → return RecoveryResult(success=True)

  Attempt 2 — correction context from attempt 1's failure
    generate_sql(correction_question) → validate → execute
    ↓ any fail  → build richer context + full 42-column list → attempt 3
    ↓ success   → return RecoveryResult(success=True)

  Attempt 3 — all context + all 42 column names (per spec)
    generate_sql(correction_question + column_list) → validate → execute
    ↓ any fail  → GIVE UP: RecoveryResult(success=False, all attempts logged)
    ↓ success   → return RecoveryResult(success=True)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FAILURE TAXONOMY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  FailureKind.SECURITY        — DML/DDL/xp_ detected → NEVER retry
  FailureKind.VALIDATION      — wrong table / unknown column → retryable
  FailureKind.EXECUTION       — pyodbc runtime SQL error → retryable
  FailureKind.TIMEOUT         — query took > 30s → retryable (simpler query)
  FailureKind.GENERATION      — Ollama failure / empty SQL → retryable
  FailureKind.EMPTY_RESULT    — 0-row DataFrame → valid success, NOT a failure

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA CONTRACTS WITH OTHER MODULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  RecoveryResult.sql          → feedback_collector.py (for vn.train())
                              → persistence/sqlite_store.py (query log)
  RecoveryResult.df           → charts/ pipeline + data_table.py
  RecoveryResult.attempts     → streamlit_app/components/chat.py (error UI)
  RecoveryResult.is_security  → chat.py (shows different rejection panel)
"""

from __future__ import annotations

import logging
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from core.sql_validator import (
    KNOWN_COLUMNS,
    ValidationResult,
    ViolationType,
    build_correction_context,
    validate_sql,
)

if TYPE_CHECKING:
    # Avoid circular import at runtime — vanna_instance imports nothing from here
    from core.vanna_instance import MyVanna

# ── Logging ───────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# Errors log (shared with other core modules)
_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

if not logging.getLogger("error_recovery.file").handlers:
    _fh = logging.FileHandler(_LOG_DIR / "errors.log", encoding="utf-8")
    _fh.setFormatter(
        logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    _file_logger = logging.getLogger("error_recovery.file")
    _file_logger.setLevel(logging.DEBUG)
    _file_logger.propagate = False
    _file_logger.addHandler(_fh)
else:
    _file_logger = logging.getLogger("error_recovery.file")

# ── Constants (from spec) ─────────────────────────────────────────────────────

MAX_ATTEMPTS: int       = 3      # 1 original + 2 retries
QUERY_TIMEOUT_SECS: int = 30     # SQL Server execution timeout
ROW_CAP: int            = 10_000 # df.head(10_000) per spec


# ── Failure taxonomy ──────────────────────────────────────────────────────────

class FailureKind(str, Enum):
    SECURITY   = "security"    # hard stop — never retry
    VALIDATION = "validation"  # sanity check failed — retryable
    EXECUTION  = "execution"   # pyodbc SQL error — retryable
    TIMEOUT    = "timeout"     # query took > 30s — retryable
    GENERATION = "generation"  # Ollama returned empty / raised — retryable
    EMPTY      = "empty"       # 0-row DataFrame — this is SUCCESS, not failure


# ── Per-attempt record ────────────────────────────────────────────────────────

@dataclass
class AttemptRecord:
    """
    Complete record of one generation→validate→execute cycle.

    Consumed by:
      - streamlit_app/components/chat.py  — error panel rendering
      - persistence/sqlite_store.py       — query history logging
      - feedback/analytics.py             — accuracy tracking
    """
    attempt_number:    int                    # 1, 2, or 3
    question_sent:     str                    # question / correction_question sent to Vanna
    sql_generated:     str | None     = None  # raw output from vn.generate_sql()
    validation_result: ValidationResult | None = None
    execution_error:   str | None     = None  # pyodbc / timeout error message
    failure_kind:      FailureKind | None = None
    elapsed_ms:        float          = 0.0
    success:           bool           = False
    rows_returned:     int | None     = None  # set on success

    @property
    def failure_summary(self) -> str:
        """Single readable line for UI display."""
        if self.success:
            return f"Success — {self.rows_returned} row(s) returned."
        if self.failure_kind == FailureKind.SECURITY:
            vr = self.validation_result
            msg = vr.violations[0] if (vr and vr.violations) else "Security violation."
            return f"[SECURITY] {msg}"
        if self.failure_kind == FailureKind.VALIDATION:
            vr = self.validation_result
            msgs = " | ".join(vr.violations) if (vr and vr.violations) else "Validation failed."
            return f"[VALIDATION] {msgs}"
        if self.failure_kind == FailureKind.EXECUTION:
            return f"[SQL ERROR] {self.execution_error or '(no message)'}"
        if self.failure_kind == FailureKind.TIMEOUT:
            return f"[TIMEOUT] Query exceeded {QUERY_TIMEOUT_SECS}s."
        if self.failure_kind == FailureKind.GENERATION:
            return f"[GENERATION] {self.execution_error or 'Ollama returned empty SQL.'}"
        return "Unknown failure."

    @property
    def display_sql(self) -> str:
        """SQL to show in the error panel — fixed version if available."""
        if self.validation_result and self.validation_result.fixed_sql:
            return self.validation_result.fixed_sql
        return self.sql_generated or "(no SQL generated)"


# ── Recovery result ───────────────────────────────────────────────────────────

@dataclass
class RecoveryResult:
    """
    The single return value of run_with_recovery().

    This is the contract between error_recovery.py and the rest of the system.

    ┌─────────────────┬────────────────────────────────────────────────────┐
    │ Field           │ Consumer                                           │
    ├─────────────────┼────────────────────────────────────────────────────┤
    │ success         │ 01_chat.py — branch to success or error UI         │
    │ df              │ charts/ pipeline, data_table.py                    │
    │ sql             │ sql_viewer.py, feedback_collector.py (vn.train())  │
    │                 │ sqlite_store.py (query log)                        │
    │ attempts        │ chat.py error panel, analytics.py                  │
    │ is_security     │ chat.py — shows "blocked" panel, not "error" panel │
    │ total_elapsed_ms│ sqlite_store.py (performance tracking)             │
    └─────────────────┴────────────────────────────────────────────────────┘
    """
    success:           bool
    df:                pd.DataFrame | None  = None
    sql:               str | None           = None   # ALWAYS use fixed_sql (validated)
    attempts:          list[AttemptRecord]  = field(default_factory=list)
    failure_reason:    str                  = ""     # human-readable for error UI
    is_security:       bool                 = False  # hard block — different UI panel
    total_elapsed_ms:  float                = 0.0

    @property
    def attempt_count(self) -> int:
        return len(self.attempts)

    @property
    def last_attempt(self) -> AttemptRecord | None:
        return self.attempts[-1] if self.attempts else None

    @property
    def all_errors(self) -> list[str]:
        """Flat list of all failure summaries across attempts. For SQLite logging."""
        return [a.failure_summary for a in self.attempts if not a.success]

    @property
    def all_sqls_tried(self) -> list[str]:
        """All SQL strings attempted, for the error panel's expandable history."""
        return [a.display_sql for a in self.attempts]

    def user_facing_error(self) -> str:
        """
        Compose the final error message shown to the user when all attempts fail.

        Format mirrors what the spec asks for:
        "show error + failed SQL + error messages to user"
        """
        if self.success:
            return ""

        if self.is_security:
            last = self.last_attempt
            detail = last.failure_summary if last else "Security violation."
            return (
                "⛔ This query was blocked for security reasons.\n\n"
                f"{detail}\n\n"
                "Only read-only SELECT queries against [dbo].[B2B_B2C] are permitted."
            )

        lines: list[str] = [
            f"❌ Could not generate a working SQL query after {self.attempt_count} attempt(s).\n"
        ]

        for rec in self.attempts:
            lines.append(f"── Attempt {rec.attempt_number} {'✓' if rec.success else '✗'} "
                         f"({rec.elapsed_ms:.0f}ms) ──")
            lines.append(f"   {rec.failure_summary}")
            if rec.sql_generated:
                lines.append(f"   SQL tried:\n{textwrap.indent(rec.display_sql, '   ')}")

        lines.append(
            "\nSuggestions:\n"
            "  • Rephrase the question more specifically\n"
            "  • Add more Q→SQL examples via the Admin page\n"
            "  • Check that the relevant columns exist in [dbo].[B2B_B2C]"
        )
        return "\n".join(lines)


# ── Correction-context builders ───────────────────────────────────────────────

def _build_execution_error_context(
    question: str,
    sql: str,
    error_message: str,
) -> str:
    """
    Build a correction_question for an execution failure (pyodbc error).

    Different from build_correction_context() in sql_validator.py which handles
    validator-detected issues.  This handles SQL Server runtime errors:
    - "Invalid column name 'revenue'"
    - "Cannot convert varchar to decimal"
    - "Incorrect syntax near 'LIMIT'"
    - "Object 'SalesOrders' does not exist"
    """
    return (
        f'The following SQL was generated for the question:\n'
        f'"{question}"\n\n'
        f"SQL:\n{sql}\n\n"
        f"This SQL failed to execute on SQL Server with the following error:\n"
        f"{error_message}\n\n"
        f"Please generate a corrected SQL query that:\n"
        f"- Fixes the error above\n"
        f"- Uses [dbo].[B2B_B2C] (the ONLY available view in this database)\n"
        f"- Uses T-SQL syntax: TOP N (not LIMIT), ISNULL() (not COALESCE), "
        f"GETDATE() (not NOW()), DATEADD/DATEDIFF for date math\n"
        f"- Applies the net sales filter (exclude Amazon Cancel/Refund/FreeReplacement, "
        f"Flipkart Cancellation/Return/RTO, Shopify unfulfilled)\n"
        f"- Starts with SELECT or WITH (read-only only)\n"
        f"- Uses ORDER BY MIN(order_date) for chronological sort — not ORDER BY month_year\n"
        f"- Returns ONLY the SQL query — no explanations, no markdown fences."
    )


def _build_timeout_context(question: str, sql: str) -> str:
    """
    Build a correction question when the query timed out (> 30s).
    Asks the model to simplify — add TOP, avoid cross-joins, etc.
    """
    return (
        f'The following SQL was generated for the question:\n'
        f'"{question}"\n\n'
        f"SQL:\n{sql}\n\n"
        f"This SQL timed out after {QUERY_TIMEOUT_SECS} seconds on SQL Server.\n\n"
        f"Please generate a simpler, faster SQL query that:\n"
        f"- Uses TOP N to limit rows (e.g. TOP 100 or TOP 1000)\n"
        f"- Avoids full table scans where possible\n"
        f"- Uses aggregation (GROUP BY + SUM/COUNT) instead of returning raw rows\n"
        f"- Still answers the original question: \"{question}\"\n"
        f"- Uses [dbo].[B2B_B2C] and T-SQL syntax\n"
        f"- Applies the net sales filter\n"
        f"- Returns ONLY the SQL query."
    )


def _build_generation_error_context(question: str, error_message: str) -> str:
    """
    Build a correction question when Ollama itself failed or returned empty SQL.
    Restates the question with extra structure hints.
    """
    return (
        f"The previous attempt to generate SQL for the following question failed:\n"
        f'"{question}"\n\n'
        f"Error: {error_message}\n\n"
        f"Please generate a SQL query for this question.\n"
        f"Target: [dbo].[B2B_B2C] on Microsoft SQL Server\n"
        f"Rules: TOP N (not LIMIT), ISNULL not COALESCE, GETDATE() not NOW(),\n"
        f"       apply net sales filter, ORDER BY MIN(order_date) for dates.\n"
        f"Return ONLY the SQL query."
    )


def _append_column_list(context: str) -> str:
    """
    Append the full 42-column list to any correction context.
    Called for attempt 3 (per spec: 'Attempt 2: also include full list of 42 column names').

    Note: spec says 'Attempt 2' but in 0-indexed retry terms that is the 3rd
    total attempt — the first retry that now gets extra help.  We append it
    starting from the second correction question (i.e. before attempt 3).
    """
    col_list = ", ".join(sorted(KNOWN_COLUMNS))
    return (
        context
        + f"\n\nAll available columns in [dbo].[B2B_B2C] "
        f"({len(KNOWN_COLUMNS)} total):\n{col_list}\n\n"
        f"Use ONLY these column names — do not invent column names."
    )


# ── Execution with timeout ────────────────────────────────────────────────────

def _execute_with_timeout(
    vn: "MyVanna",
    sql: str,
    timeout_secs: int = QUERY_TIMEOUT_SECS,
) -> pd.DataFrame:
    """
    Execute sql via vn.run_sql() with a hard timeout.

    vn.run_sql() is Vanna's internal execution method — it manages the pyodbc
    connection internally.  We cannot inject a SQL Server query timeout directly
    into Vanna's connection, so we wrap the call in a thread.

    Returns:
        DataFrame capped at ROW_CAP rows (spec: df.head(10_000))

    Raises:
        TimeoutError    — query exceeded timeout_secs
        Exception       — pyodbc / SQL Server runtime error (caller logs + retries)
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(vn.run_sql, sql)
        try:
            df = future.result(timeout=timeout_secs)
        except FuturesTimeout:
            future.cancel()
            raise TimeoutError(
                f"SQL Server query timed out after {timeout_secs}s."
            )
        # Any other exception (pyodbc.Error, RuntimeError, etc.) propagates naturally

    if df is None:
        # vn.run_sql() returns None on some Vanna versions when the result is empty
        return pd.DataFrame()

    # Row cap — prevent pandas memory issues with massive result sets
    if len(df) > ROW_CAP:
        logger.info("Result capped from %d to %d rows.", len(df), ROW_CAP)
        df = df.head(ROW_CAP)

    return df


# ── Single-attempt executor ───────────────────────────────────────────────────

def _run_single_attempt(
    vn: "MyVanna",
    attempt_number: int,
    question_sent: str,
    timeout_secs: int,
) -> AttemptRecord:
    """
    Execute ONE complete generate→validate→execute cycle.

    Returns an AttemptRecord — always, even on failure.
    Does NOT raise exceptions; all errors are captured into the record.

    The caller (run_with_recovery) reads the record and decides what to do next.
    """
    t0 = time.perf_counter()
    rec = AttemptRecord(
        attempt_number=attempt_number,
        question_sent=question_sent,
    )

    # ── Step 1: Generate SQL ──────────────────────────────────────────────────
    logger.info(
        "[Attempt %d/%d] Calling vn.generate_sql()…  "
        "question_preview=%r",
        attempt_number, MAX_ATTEMPTS,
        question_sent[:80],
    )

    try:
        raw_sql: str = vn.generate_sql(question_sent)
    except Exception as exc:
        rec.elapsed_ms    = (time.perf_counter() - t0) * 1000
        rec.failure_kind  = FailureKind.GENERATION
        rec.execution_error = str(exc)
        _file_logger.error(
            "[Attempt %d] generate_sql() raised: %s", attempt_number, exc, exc_info=True
        )
        logger.warning("[Attempt %d] generate_sql() raised: %s", attempt_number, exc)
        return rec

    if not raw_sql or not raw_sql.strip():
        rec.elapsed_ms    = (time.perf_counter() - t0) * 1000
        rec.failure_kind  = FailureKind.GENERATION
        rec.execution_error = "vn.generate_sql() returned empty string."
        logger.warning("[Attempt %d] generate_sql() returned empty SQL.", attempt_number)
        return rec

    rec.sql_generated = raw_sql.strip()
    logger.info("[Attempt %d] SQL generated (%d chars).", attempt_number, len(rec.sql_generated))

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    validation = validate_sql(rec.sql_generated)
    rec.validation_result = validation

    if not validation.is_valid:
        rec.elapsed_ms = (time.perf_counter() - t0) * 1000

        if validation.violation_type == ViolationType.SECURITY:
            rec.failure_kind = FailureKind.SECURITY
            logger.warning(
                "[Attempt %d] SECURITY violation — will not retry. %s",
                attempt_number, validation.violation_summary(),
            )
        else:
            rec.failure_kind = FailureKind.VALIDATION
            logger.warning(
                "[Attempt %d] SANITY violation — retryable. %s",
                attempt_number, validation.violation_summary(),
            )
        _file_logger.warning(
            "[Attempt %d] Validation failed (%s): %s  |  SQL: %s",
            attempt_number,
            rec.failure_kind,
            validation.violation_summary(),
            textwrap.shorten(rec.sql_generated, width=300, placeholder="…"),
        )
        return rec

    # Log auto-fix silently (not a failure, but worth tracking)
    if validation.auto_fixed:
        logger.info(
            "[Attempt %d] Auto-fix applied (SELECT * → SELECT TOP 1000 *).",
            attempt_number,
        )

    # ── Step 3: Execute ───────────────────────────────────────────────────────
    sql_to_run = validation.fixed_sql
    logger.info("[Attempt %d] Executing SQL (timeout=%ds)…", attempt_number, timeout_secs)

    try:
        df = _execute_with_timeout(vn, sql_to_run, timeout_secs)
    except TimeoutError as exc:
        rec.elapsed_ms    = (time.perf_counter() - t0) * 1000
        rec.failure_kind  = FailureKind.TIMEOUT
        rec.execution_error = str(exc)
        _file_logger.warning(
            "[Attempt %d] Timeout: %s  |  SQL: %s",
            attempt_number, exc,
            textwrap.shorten(sql_to_run, width=300, placeholder="…"),
        )
        logger.warning("[Attempt %d] Query timed out.", attempt_number)
        return rec
    except Exception as exc:
        rec.elapsed_ms    = (time.perf_counter() - t0) * 1000
        rec.failure_kind  = FailureKind.EXECUTION
        rec.execution_error = str(exc)
        _file_logger.error(
            "[Attempt %d] Execution error: %s  |  SQL: %s",
            attempt_number, exc,
            textwrap.shorten(sql_to_run, width=300, placeholder="…"),
            exc_info=True,
        )
        logger.warning("[Attempt %d] Execution error: %s", attempt_number, exc)
        return rec

    # ── Success ───────────────────────────────────────────────────────────────
    rec.elapsed_ms   = (time.perf_counter() - t0) * 1000
    rec.success      = True
    rec.rows_returned = len(df)
    rec.failure_kind = None

    # Note: 0-row result is valid (e.g. "any cancelled orders today?" → 0 rows)
    logger.info(
        "[Attempt %d] SUCCESS — %d rows × %d cols in %.0fms.",
        attempt_number, len(df),
        len(df.columns) if not df.empty else 0,
        rec.elapsed_ms,
    )

    # Attach df to the record temporarily so the caller can extract it.
    # We store it outside the dataclass to keep the dataclass serialisable.
    rec._df   = df           # type: ignore[attr-defined]
    rec._sql  = sql_to_run   # type: ignore[attr-defined]  ← always use fixed_sql
    return rec


# ── Correction question factory ───────────────────────────────────────────────

def _build_correction_question(
    original_question: str,
    failed_attempt: AttemptRecord,
    include_column_list: bool,
) -> str:
    """
    Choose the right correction context based on what kind of failure occurred,
    then optionally append the 42-column list.

    Args:
        original_question:   The original user question (never the correction text).
        failed_attempt:      The AttemptRecord from the attempt that just failed.
        include_column_list: True for attempt 3 (second retry).
    """
    kind = failed_attempt.failure_kind

    if kind == FailureKind.VALIDATION:
        vr = failed_attempt.validation_result
        if vr is not None:
            ctx = build_correction_context(vr, original_question)
        else:
            # Fallback — should not happen
            ctx = _build_generation_error_context(
                original_question, "Validation failed (no details)."
            )

    elif kind == FailureKind.EXECUTION:
        sql = failed_attempt.display_sql
        err = failed_attempt.execution_error or "SQL execution failed."
        ctx = _build_execution_error_context(original_question, sql, err)

    elif kind == FailureKind.TIMEOUT:
        sql = failed_attempt.display_sql
        ctx = _build_timeout_context(original_question, sql)

    elif kind == FailureKind.GENERATION:
        err = failed_attempt.execution_error or "SQL generation failed."
        ctx = _build_generation_error_context(original_question, err)

    else:
        # Unknown / unexpected failure — re-ask cleanly
        ctx = _build_generation_error_context(
            original_question,
            f"Attempt {failed_attempt.attempt_number} failed: {failed_attempt.failure_summary}",
        )

    if include_column_list:
        ctx = _append_column_list(ctx)

    return ctx


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def run_with_recovery(
    vn: "MyVanna",
    question: str,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    query_timeout_secs: int = QUERY_TIMEOUT_SECS,
) -> RecoveryResult:
    """
    Execute the full generate→validate→execute pipeline with automatic retry.

    This is the ONLY function that 01_chat.py needs to call.

    Args:
        vn:                  The MyVanna singleton (from get_vanna() / st.cache_resource).
        question:            The enriched, preprocessed question from the chat pipeline.
                             (date-resolved, follow-up context prepended, chart hints stripped)
        max_attempts:        Total attempts allowed. Default: 3 (spec).
        query_timeout_secs:  Per-execution timeout. Default: 30s (spec).

    Returns:
        RecoveryResult with:
          .success        → branch point for chat.py
          .df             → pass to charts/ pipeline
          .sql            → pass to sql_viewer.py + feedback_collector.py
          .attempts       → full audit trail for error panel
          .is_security    → show blocked UI instead of error UI
          .total_elapsed_ms → log to SQLite

    Never raises — all exceptions are captured into AttemptRecord.execution_error.

    Usage in 01_chat.py:
    ─────────────────────────────────────────────────────────────────
    result = run_with_recovery(vn, enriched_question)

    st.session_state["last_sql"]          = result.sql
    st.session_state["last_df"]           = result.df
    st.session_state["last_recovery"]     = result   # for error panel + feedback

    if result.success:
        # → chart pipeline, SQL viewer, data table
    else:
        # → error panel with result.user_facing_error()
        # → if result.is_security: show blocked panel
    ─────────────────────────────────────────────────────────────────
    """
    pipeline_start = time.perf_counter()
    attempts: list[AttemptRecord] = []
    original_question = question   # preserve for correction context building

    logger.info(
        "run_with_recovery() started | max_attempts=%d | timeout=%ds | "
        "question=%r",
        max_attempts, query_timeout_secs,
        question[:100],
    )

    for attempt_number in range(1, max_attempts + 1):

        # ── Build the question to send to Vanna ───────────────────────────────
        if attempt_number == 1:
            # First attempt: raw question (already enriched by chat pipeline)
            question_for_vanna = question

        elif attempt_number == 2:
            # First retry: correction context from attempt 1's failure
            # Column list NOT included yet (spec: "Attempt 2 includes column list"
            # means the second retry = attempt 3 in 1-indexed terms)
            question_for_vanna = _build_correction_question(
                original_question,
                failed_attempt=attempts[-1],
                include_column_list=False,
            )
            logger.info("[Attempt 2] Built correction context from attempt 1 failure.")

        else:
            # attempt_number >= 3: all context + full 42-column list (spec requirement)
            question_for_vanna = _build_correction_question(
                original_question,
                failed_attempt=attempts[-1],
                include_column_list=True,  # ← spec: "Attempt 2: also include 42 column names"
            )
            logger.info(
                "[Attempt %d] Built correction context WITH full column list.",
                attempt_number,
            )

        # ── Run the attempt ───────────────────────────────────────────────────
        rec = _run_single_attempt(
            vn            = vn,
            attempt_number = attempt_number,
            question_sent  = question_for_vanna,
            timeout_secs   = query_timeout_secs,
        )
        attempts.append(rec)

        # ── Evaluate outcome ──────────────────────────────────────────────────
        if rec.success:
            total_ms = (time.perf_counter() - pipeline_start) * 1000
            df  = rec._df   # type: ignore[attr-defined]
            sql = rec._sql  # type: ignore[attr-defined]

            _file_logger.info(
                "Pipeline SUCCESS | attempts=%d | rows=%d | total_ms=%.0f | sql=%s",
                attempt_number, len(df), total_ms,
                textwrap.shorten(sql, width=200, placeholder="…"),
            )
            logger.info(
                "run_with_recovery() SUCCEEDED on attempt %d/%d in %.0fms.",
                attempt_number, max_attempts, total_ms,
            )
            return RecoveryResult(
                success          = True,
                df               = df,
                sql              = sql,
                attempts         = attempts,
                total_elapsed_ms = total_ms,
            )

        # ── Hard stop: security violation — never retry ───────────────────────
        if rec.failure_kind == FailureKind.SECURITY:
            total_ms = (time.perf_counter() - pipeline_start) * 1000
            viol_summary = (
                rec.validation_result.violation_summary()
                if rec.validation_result
                else "Security violation."
            )
            _file_logger.warning(
                "Pipeline SECURITY BLOCK | attempts=1 | total_ms=%.0f | reason=%s",
                total_ms, viol_summary,
            )
            logger.warning("SECURITY violation — pipeline halted immediately.")
            return RecoveryResult(
                success          = False,
                df               = None,
                sql              = None,
                attempts         = attempts,
                failure_reason   = viol_summary,
                is_security      = True,
                total_elapsed_ms = total_ms,
            )

        # ── Soft failure: log and continue to next attempt ────────────────────
        logger.warning(
            "[Attempt %d/%d] FAILED (%s). %s",
            attempt_number, max_attempts,
            rec.failure_kind,
            rec.failure_summary,
        )

    # ── All attempts exhausted ────────────────────────────────────────────────
    total_ms = (time.perf_counter() - pipeline_start) * 1000
    last = attempts[-1]

    # Compose failure reason from the last attempt
    failure_reason = (
        f"All {max_attempts} attempts failed. Last error: {last.failure_summary}"
    )

    _file_logger.error(
        "Pipeline FAILED | all %d attempts exhausted | total_ms=%.0f | "
        "last_failure=%s | last_sql=%s",
        max_attempts, total_ms,
        last.failure_summary,
        textwrap.shorten(last.display_sql, width=300, placeholder="…"),
    )
    logger.error(
        "run_with_recovery() FAILED after %d attempts in %.0fms.",
        max_attempts, total_ms,
    )

    return RecoveryResult(
        success          = False,
        df               = None,
        sql              = None,
        attempts         = attempts,
        failure_reason   = failure_reason,
        is_security      = False,
        total_elapsed_ms = total_ms,
    )


# ── Convenience helpers used by other modules ─────────────────────────────────

def result_to_log_dict(
    question: str,
    result: RecoveryResult,
    query_id: str | None = None,
) -> dict:
    """
    Serialise a RecoveryResult into a flat dict for persistence/sqlite_store.py.

    All values are JSON-safe (strings, ints, floats, bools).

    Usage in sqlite_store.py:
        row = result_to_log_dict(question, result, query_id=str(uuid.uuid4()))
        store.insert_query_log(row)
    """
    last = result.last_attempt
    return {
        "query_id":         query_id or "",
        "question":         question,
        "sql":              result.sql or "",
        "success":          result.success,
        "is_security":      result.is_security,
        "attempt_count":    result.attempt_count,
        "rows_returned":    last.rows_returned if (last and last.success) else 0,
        "total_elapsed_ms": round(result.total_elapsed_ms, 1),
        "failure_reason":   result.failure_reason,
        "all_errors":       " ||| ".join(result.all_errors),
        # Attempt-level detail (stored as structured strings for SQLite)
        "attempt_1_sql":    result.attempts[0].display_sql if len(result.attempts) >= 1 else "",
        "attempt_1_error":  result.attempts[0].failure_summary if len(result.attempts) >= 1 else "",
        "attempt_2_sql":    result.attempts[1].display_sql if len(result.attempts) >= 2 else "",
        "attempt_2_error":  result.attempts[1].failure_summary if len(result.attempts) >= 2 else "",
        "attempt_3_sql":    result.attempts[2].display_sql if len(result.attempts) >= 3 else "",
        "attempt_3_error":  result.attempts[2].failure_summary if len(result.attempts) >= 3 else "",
    }


# ── __all__ ───────────────────────────────────────────────────────────────────

__all__ = [
    "run_with_recovery",
    "RecoveryResult",
    "AttemptRecord",
    "FailureKind",
    "result_to_log_dict",
    "MAX_ATTEMPTS",
    "QUERY_TIMEOUT_SECS",
    "ROW_CAP",
]