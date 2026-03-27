"""
persistence/sqlite_store.py

SQLite persistence layer for query history, feedback logs, and analytics.

Tables created automatically:
    query_log    — every query executed (question, SQL, success, timing)
    feedback_log — every feedback event (positive, negative, corrected)

Thread-safe: uses check_same_thread=False for Streamlit compatibility.
File: persistence/queries.db (auto-created)

Usage:
    from persistence.sqlite_store import SQLiteStore, get_sqlite_store

    store = get_sqlite_store()
    store.log_query(query_id="...", question="...", sql="...", success=True)
    store.log_feedback(query_id="...", question="...", sql="...", feedback="positive")
    df = store.get_query_log()
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict

import pandas as pd

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "persistence" / "queries.db"

# ── Schema ───────────────────────────────────────────────────────────────────
_CREATE_QUERY_LOG = """
CREATE TABLE IF NOT EXISTS query_log (
    id             TEXT PRIMARY KEY,
    timestamp      TEXT NOT NULL,
    question       TEXT NOT NULL,
    sql_generated  TEXT,
    success        INTEGER DEFAULT 0,
    error_message  TEXT,
    rows_returned  INTEGER DEFAULT 0,
    retries        INTEGER DEFAULT 0,
    execution_ms   INTEGER DEFAULT 0,
    chart_type     TEXT,
    notes          TEXT
);
"""

_CREATE_FEEDBACK_LOG = """
CREATE TABLE IF NOT EXISTS feedback_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp      TEXT NOT NULL,
    query_id       TEXT,
    question       TEXT,
    sql            TEXT,
    feedback       TEXT NOT NULL,
    trained        INTEGER DEFAULT 0,
    original_sql   TEXT,
    notes          TEXT
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_query_timestamp ON query_log(timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_query_success ON query_log(success);",
    "CREATE INDEX IF NOT EXISTS idx_feedback_timestamp ON feedback_log(timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_feedback_type ON feedback_log(feedback);",
    "CREATE INDEX IF NOT EXISTS idx_feedback_query_id ON feedback_log(query_id);",
]


class SQLiteStore:
    """
    Thread-safe SQLite store for query history and feedback.

    Auto-creates the database file and tables on first use.
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a connection (creates file if needed)."""
        conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=10,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        """Create tables and indexes if they don't exist."""
        try:
            conn = self._get_conn()
            conn.execute(_CREATE_QUERY_LOG)
            conn.execute(_CREATE_FEEDBACK_LOG)
            for idx_sql in _CREATE_INDEXES:
                conn.execute(idx_sql)
            conn.commit()
            conn.close()
            logger.info("SQLite store initialised at %s", self.db_path)
        except Exception as exc:
            logger.error("SQLite init failed: %s", exc)

    # ── Query logging ────────────────────────────────────────────────────

    def log_query(
        self,
        query_id: str,
        question: str,
        sql: str = "",
        success: bool = True,
        error_message: str = "",
        rows_returned: int = 0,
        retries: int = 0,
        execution_ms: int = 0,
        chart_type: str = "",
        notes: str = "",
    ) -> None:
        """Log a query execution to the query_log table."""
        timestamp = datetime.now().isoformat()
        with self._lock:
            try:
                conn = self._get_conn()
                conn.execute(
                    """
                    INSERT OR REPLACE INTO query_log
                        (id, timestamp, question, sql_generated, success,
                         error_message, rows_returned, retries,
                         execution_ms, chart_type, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        query_id, timestamp, question, sql,
                        1 if success else 0,
                        error_message, rows_returned, retries,
                        execution_ms, chart_type, notes,
                    ),
                )
                conn.commit()
                conn.close()
                logger.debug("Logged query: %s", query_id)
            except Exception as exc:
                logger.error("Failed to log query: %s", exc)

    # ── Feedback logging ─────────────────────────────────────────────────

    def log_feedback(
        self,
        query_id: str,
        question: str,
        sql: str,
        feedback: str,
        trained: bool = False,
        original_sql: str = "",
        notes: str = "",
    ) -> None:
        """Log a feedback event to the feedback_log table."""
        timestamp = datetime.now().isoformat()
        with self._lock:
            try:
                conn = self._get_conn()
                conn.execute(
                    """
                    INSERT INTO feedback_log
                        (timestamp, query_id, question, sql, feedback,
                         trained, original_sql, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        timestamp, query_id, question, sql, feedback,
                        1 if trained else 0, original_sql, notes,
                    ),
                )
                conn.commit()
                conn.close()
                logger.debug("Logged feedback: %s / %s", query_id, feedback)
            except Exception as exc:
                logger.error("Failed to log feedback: %s", exc)

    # ── Retrieval ────────────────────────────────────────────────────────

    def get_query_log(
        self,
        limit: int = 500,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Get query log as a DataFrame, most recent first."""
        try:
            conn = self._get_conn()
            df = pd.read_sql_query(
                f"""
                SELECT * FROM query_log
                ORDER BY timestamp DESC
                LIMIT {limit} OFFSET {offset}
                """,
                conn,
            )
            conn.close()
            return df
        except Exception as exc:
            logger.error("Failed to read query log: %s", exc)
            return pd.DataFrame()

    def get_feedback_log(
        self,
        limit: int = 500,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Get feedback log as a DataFrame, most recent first."""
        try:
            conn = self._get_conn()
            df = pd.read_sql_query(
                f"""
                SELECT * FROM feedback_log
                ORDER BY timestamp DESC
                LIMIT {limit} OFFSET {offset}
                """,
                conn,
            )
            conn.close()
            return df
        except Exception as exc:
            logger.error("Failed to read feedback log: %s", exc)
            return pd.DataFrame()

    def get_query_by_id(self, query_id: str) -> Optional[dict]:
        """Get a single query log entry by ID."""
        try:
            conn = self._get_conn()
            cursor = conn.execute(
                "SELECT * FROM query_log WHERE id = ?", (query_id,)
            )
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        except Exception as exc:
            logger.error("Failed to get query %s: %s", query_id, exc)
            return None

    def get_feedback_for_query(self, query_id: str) -> pd.DataFrame:
        """Get all feedback entries for a specific query."""
        try:
            conn = self._get_conn()
            df = pd.read_sql_query(
                "SELECT * FROM feedback_log WHERE query_id = ? ORDER BY timestamp",
                conn,
                params=(query_id,),
            )
            conn.close()
            return df
        except Exception as exc:
            logger.error("Failed to get feedback for %s: %s", query_id, exc)
            return pd.DataFrame()

    # ── Statistics ───────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics from both tables."""
        stats = {
            "total_queries": 0,
            "successful_queries": 0,
            "total_feedback": 0,
            "db_size_kb": 0,
        }
        try:
            conn = self._get_conn()

            cursor = conn.execute("SELECT COUNT(*) FROM query_log")
            stats["total_queries"] = cursor.fetchone()[0]

            cursor = conn.execute(
                "SELECT COUNT(*) FROM query_log WHERE success = 1"
            )
            stats["successful_queries"] = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM feedback_log")
            stats["total_feedback"] = cursor.fetchone()[0]

            conn.close()

            # File size
            if self.db_path.exists():
                stats["db_size_kb"] = round(
                    self.db_path.stat().st_size / 1024, 1
                )

        except Exception as exc:
            logger.error("Failed to get stats: %s", exc)

        return stats

    # ── Cleanup ──────────────────────────────────────────────────────────

    def cleanup_old_entries(self, days: int = 90) -> int:
        """Delete query log entries older than N days."""
        cutoff = (datetime.now() - __import__("datetime").timedelta(days=days)).isoformat()
        deleted = 0
        with self._lock:
            try:
                conn = self._get_conn()
                cursor = conn.execute(
                    "DELETE FROM query_log WHERE timestamp < ?", (cutoff,)
                )
                deleted = cursor.rowcount
                conn.commit()
                conn.close()
                logger.info("Cleaned up %d old query log entries.", deleted)
            except Exception as exc:
                logger.error("Cleanup failed: %s", exc)
        return deleted


# ── Singleton factory ────────────────────────────────────────────────────────
_store_instance: SQLiteStore | None = None


def get_sqlite_store(db_path: Path | str | None = None) -> SQLiteStore:
    """
    Return the module-level SQLiteStore singleton.

    Thread-safe for Streamlit's multi-thread model.
    """
    global _store_instance
    if _store_instance is None:
        _store_instance = SQLiteStore(db_path=db_path)
    return _store_instance


__all__ = ["SQLiteStore", "get_sqlite_store"]