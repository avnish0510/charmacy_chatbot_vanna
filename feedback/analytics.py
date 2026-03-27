"""
feedback/analytics.py

Query history and accuracy analytics.

Reads from SQLite (persistence/queries.db) to provide:
    - Query count by day / week / month
    - Success rate (queries that returned data vs errors)
    - Feedback breakdown (positive / negative / corrected)
    - Most common questions
    - Average retries per query
    - Training coverage assessment
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd

from persistence.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class FeedbackAnalytics:
    """
    Analytics engine for query history and feedback data.

    Usage:
        from feedback.analytics import FeedbackAnalytics
        analytics = FeedbackAnalytics(sqlite_store)
        summary = analytics.get_summary()
        daily = analytics.queries_by_day(days=30)
    """

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    # ── Summary statistics ───────────────────────────────────────────────

    def get_summary(self) -> Dict[str, Any]:
        """
        Get overall analytics summary.

        Returns:
            {
                "total_queries": int,
                "successful_queries": int,
                "failed_queries": int,
                "success_rate": float (0-100),
                "positive_feedback": int,
                "negative_feedback": int,
                "corrected_feedback": int,
                "feedback_rate": float (0-100),
                "unique_questions": int,
                "avg_retries": float,
                "first_query": str (datetime),
                "last_query": str (datetime),
            }
        """
        try:
            query_df = self.store.get_query_log()
            feedback_df = self.store.get_feedback_log()
        except Exception as exc:
            logger.error("Failed to load analytics data: %s", exc)
            return self._empty_summary()

        summary: Dict[str, Any] = {}

        # Query stats
        if query_df is not None and not query_df.empty:
            summary["total_queries"] = len(query_df)
            summary["successful_queries"] = int(
                query_df["success"].sum() if "success" in query_df.columns else 0
            )
            summary["failed_queries"] = summary["total_queries"] - summary["successful_queries"]
            summary["success_rate"] = round(
                (summary["successful_queries"] / summary["total_queries"]) * 100, 1
            ) if summary["total_queries"] > 0 else 0.0

            if "question" in query_df.columns:
                summary["unique_questions"] = query_df["question"].nunique()
            else:
                summary["unique_questions"] = 0

            if "retries" in query_df.columns:
                summary["avg_retries"] = round(query_df["retries"].mean(), 2)
            else:
                summary["avg_retries"] = 0.0

            if "timestamp" in query_df.columns:
                summary["first_query"] = str(query_df["timestamp"].min())
                summary["last_query"] = str(query_df["timestamp"].max())
            else:
                summary["first_query"] = ""
                summary["last_query"] = ""
        else:
            summary.update(self._empty_summary())

        # Feedback stats
        if feedback_df is not None and not feedback_df.empty:
            if "feedback" in feedback_df.columns:
                fb_counts = feedback_df["feedback"].value_counts()
                summary["positive_feedback"] = int(fb_counts.get("positive", 0))
                summary["negative_feedback"] = int(fb_counts.get("negative", 0))
                summary["corrected_feedback"] = int(fb_counts.get("corrected", 0))
            else:
                summary["positive_feedback"] = 0
                summary["negative_feedback"] = 0
                summary["corrected_feedback"] = 0

            total_fb = (
                summary["positive_feedback"] +
                summary["negative_feedback"] +
                summary["corrected_feedback"]
            )
            summary["feedback_rate"] = round(
                (total_fb / summary.get("total_queries", 1)) * 100, 1
            ) if summary.get("total_queries", 0) > 0 else 0.0
        else:
            summary["positive_feedback"] = 0
            summary["negative_feedback"] = 0
            summary["corrected_feedback"] = 0
            summary["feedback_rate"] = 0.0

        return summary

    def _empty_summary(self) -> Dict[str, Any]:
        return {
            "total_queries": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "success_rate": 0.0,
            "unique_questions": 0,
            "avg_retries": 0.0,
            "first_query": "",
            "last_query": "",
            "positive_feedback": 0,
            "negative_feedback": 0,
            "corrected_feedback": 0,
            "feedback_rate": 0.0,
        }

    # ── Time-series analytics ────────────────────────────────────────────

    def queries_by_day(self, days: int = 30) -> pd.DataFrame:
        """
        Get daily query counts for the last N days.

        Returns:
            DataFrame with columns: date, total, successful, failed
        """
        try:
            df = self.store.get_query_log()
        except Exception:
            return pd.DataFrame(columns=["date", "total", "successful", "failed"])

        if df is None or df.empty or "timestamp" not in df.columns:
            return pd.DataFrame(columns=["date", "total", "successful", "failed"])

        df["date"] = pd.to_datetime(df["timestamp"]).dt.date
        cutoff = (datetime.now() - timedelta(days=days)).date()
        df = df[df["date"] >= cutoff]

        if df.empty:
            return pd.DataFrame(columns=["date", "total", "successful", "failed"])

        daily = df.groupby("date").agg(
            total=("date", "count"),
            successful=("success", "sum"),
        ).reset_index()
        daily["failed"] = daily["total"] - daily["successful"]

        return daily

    def feedback_by_day(self, days: int = 30) -> pd.DataFrame:
        """Get daily feedback counts by type."""
        try:
            df = self.store.get_feedback_log()
        except Exception:
            return pd.DataFrame()

        if df is None or df.empty or "timestamp" not in df.columns:
            return pd.DataFrame()

        df["date"] = pd.to_datetime(df["timestamp"]).dt.date
        cutoff = (datetime.now() - timedelta(days=days)).date()
        df = df[df["date"] >= cutoff]

        if df.empty or "feedback" not in df.columns:
            return pd.DataFrame()

        pivot = df.groupby(["date", "feedback"]).size().unstack(fill_value=0).reset_index()
        return pivot

    # ── Top-N analytics ──────────────────────────────────────────────────

    def most_common_questions(self, top_n: int = 20) -> pd.DataFrame:
        """Get the most frequently asked questions."""
        try:
            df = self.store.get_query_log()
        except Exception:
            return pd.DataFrame(columns=["question", "count"])

        if df is None or df.empty or "question" not in df.columns:
            return pd.DataFrame(columns=["question", "count"])

        counts = (
            df["question"]
            .value_counts()
            .head(top_n)
            .reset_index()
        )
        counts.columns = ["question", "count"]
        return counts

    def most_failed_questions(self, top_n: int = 10) -> pd.DataFrame:
        """Get questions that failed most often."""
        try:
            df = self.store.get_query_log()
        except Exception:
            return pd.DataFrame(columns=["question", "failure_count"])

        if df is None or df.empty:
            return pd.DataFrame(columns=["question", "failure_count"])

        if "success" not in df.columns:
            return pd.DataFrame(columns=["question", "failure_count"])

        failed = df[df["success"] == 0]
        if failed.empty:
            return pd.DataFrame(columns=["question", "failure_count"])

        counts = (
            failed["question"]
            .value_counts()
            .head(top_n)
            .reset_index()
        )
        counts.columns = ["question", "failure_count"]
        return counts

    def negatively_rated_queries(self, top_n: int = 10) -> pd.DataFrame:
        """Get questions with the most negative feedback (training gaps)."""
        try:
            df = self.store.get_feedback_log()
        except Exception:
            return pd.DataFrame(columns=["question", "negative_count"])

        if df is None or df.empty or "feedback" not in df.columns:
            return pd.DataFrame(columns=["question", "negative_count"])

        negative = df[df["feedback"] == "negative"]
        if negative.empty or "question" not in negative.columns:
            return pd.DataFrame(columns=["question", "negative_count"])

        counts = (
            negative["question"]
            .value_counts()
            .head(top_n)
            .reset_index()
        )
        counts.columns = ["question", "negative_count"]
        return counts

    # ── Training gap analysis ────────────────────────────────────────────

    def training_gap_analysis(self, vn) -> Dict[str, Any]:
        """
        Identify questions that users ask but the model handles poorly.

        Combines:
            - Most failed questions
            - Negatively-rated questions
            - Questions NOT in training data

        Returns:
            {"gaps": list[str], "recommendation": str}
        """
        failed = self.most_failed_questions(20)
        negative = self.negatively_rated_queries(20)

        gap_questions = set()

        if not failed.empty:
            gap_questions.update(failed["question"].tolist())

        if not negative.empty:
            gap_questions.update(negative["question"].tolist())

        # Check which are already in training data
        try:
            training_df = vn.get_training_data()
            if training_df is not None and "question" in training_df.columns:
                trained_qs = set(
                    training_df["question"].dropna().str.strip().str.lower()
                )
                # Remove questions that are already trained
                gap_questions = {
                    q for q in gap_questions
                    if q.strip().lower() not in trained_qs
                }
        except Exception:
            pass

        gaps = sorted(gap_questions)[:20]

        if len(gaps) == 0:
            recommendation = "No training gaps detected. Coverage looks good!"
        elif len(gaps) < 5:
            recommendation = (
                f"Found {len(gaps)} questions that could benefit from training examples. "
                f"Add them via the Admin page."
            )
        else:
            recommendation = (
                f"Found {len(gaps)} training gaps. Adding Q→SQL examples for these "
                f"questions will significantly improve accuracy."
            )

        return {
            "gaps": gaps,
            "recommendation": recommendation,
        }


__all__ = ["FeedbackAnalytics"]