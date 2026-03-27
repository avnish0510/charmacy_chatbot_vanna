"""
feedback/example_manager.py

Manage Vanna training data: add, list, search, remove, export.

Wraps Vanna's native API:
    vn.train(question=..., sql=...)       → add Q→SQL example
    vn.train(ddl=...)                     → add DDL schema
    vn.train(documentation=...)           → add documentation
    vn.get_training_data()                → list all training entries
    vn.remove_training_data(id=...)       → delete specific entry

Adds:
    - Duplicate detection before adding
    - Batch operations (add_many, remove_many)
    - Export to JSON
    - Search / filter by type or content
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from core.vanna_instance import MyVanna

logger = logging.getLogger(__name__)


class ExampleManager:
    """
    High-level training data manager wrapping Vanna's native API.

    Usage:
        from feedback.example_manager import ExampleManager
        mgr = ExampleManager(vn)
        mgr.add_example("What is total revenue?", "SELECT SUM(MRP)...")
        mgr.list_examples()
        mgr.remove_example(id="abc123")
    """

    def __init__(self, vn: MyVanna) -> None:
        self.vn = vn

    # ── Add operations ───────────────────────────────────────────────────

    def add_example(
        self,
        question: str,
        sql: str,
        check_duplicate: bool = True,
    ) -> dict:
        """
        Add a Q→SQL training example.

        Args:
            question: Natural language question
            sql: Correct T-SQL query
            check_duplicate: If True, skip if an identical question exists

        Returns:
            {"success": bool, "message": str, "skipped": bool}
        """
        if not question.strip() or not sql.strip():
            return {"success": False, "message": "Question and SQL cannot be empty.", "skipped": False}

        if check_duplicate and self._is_duplicate_question(question):
            logger.info("Duplicate question skipped: '%s'", question[:60])
            return {"success": True, "message": "Duplicate — already exists.", "skipped": True}

        try:
            self.vn.train(question=question.strip(), sql=sql.strip())
            logger.info("Added Q→SQL example: '%s'", question[:60])
            return {"success": True, "message": "Example added.", "skipped": False}
        except Exception as exc:
            logger.error("Failed to add example: %s", exc)
            return {"success": False, "message": str(exc), "skipped": False}

    def add_ddl(self, ddl: str) -> dict:
        """Add a DDL schema training entry."""
        if not ddl.strip():
            return {"success": False, "message": "DDL cannot be empty."}
        try:
            self.vn.train(ddl=ddl.strip())
            return {"success": True, "message": "DDL trained."}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def add_documentation(self, doc: str) -> dict:
        """Add a documentation training entry."""
        if not doc.strip():
            return {"success": False, "message": "Documentation cannot be empty."}
        try:
            self.vn.train(documentation=doc.strip())
            return {"success": True, "message": "Documentation trained."}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def add_many_examples(
        self,
        examples: list[dict],
        check_duplicate: bool = True,
    ) -> dict:
        """
        Batch add Q→SQL examples.

        Args:
            examples: List of {"question": str, "sql": str} dicts
            check_duplicate: Skip duplicates

        Returns:
            {"added": int, "skipped": int, "failed": int}
        """
        added = skipped = failed = 0

        for item in examples:
            q = item.get("question", "").strip()
            s = item.get("sql", "").strip()
            if not q or not s:
                failed += 1
                continue

            result = self.add_example(q, s, check_duplicate=check_duplicate)
            if result.get("skipped"):
                skipped += 1
            elif result.get("success"):
                added += 1
            else:
                failed += 1

        logger.info(
            "Batch add: added=%d skipped=%d failed=%d", added, skipped, failed
        )
        return {"added": added, "skipped": skipped, "failed": failed}

    # ── List / search operations ─────────────────────────────────────────

    def get_all(self) -> pd.DataFrame:
        """Get all training data as a DataFrame."""
        try:
            return self.vn.get_training_data()
        except Exception as exc:
            logger.error("Failed to fetch training data: %s", exc)
            return pd.DataFrame()

    def list_examples(self) -> pd.DataFrame:
        """Get only Q→SQL examples."""
        df = self.get_all()
        if df.empty:
            return df
        return df[df["training_data_type"] == "sql"].reset_index(drop=True)

    def list_ddl(self) -> pd.DataFrame:
        """Get only DDL entries."""
        df = self.get_all()
        if df.empty:
            return df
        return df[df["training_data_type"] == "ddl"].reset_index(drop=True)

    def list_documentation(self) -> pd.DataFrame:
        """Get only documentation entries."""
        df = self.get_all()
        if df.empty:
            return df
        return df[df["training_data_type"] == "documentation"].reset_index(drop=True)

    def search(self, query: str, data_type: str | None = None) -> pd.DataFrame:
        """
        Search training data by text content.

        Args:
            query: Search string (case-insensitive)
            data_type: Optional filter — "sql", "ddl", "documentation"
        """
        df = self.get_all()
        if df.empty:
            return df

        if data_type:
            df = df[df["training_data_type"] == data_type]

        mask = pd.Series([False] * len(df), index=df.index)
        for col in df.columns:
            if df[col].dtype == "object":
                mask |= df[col].str.contains(query, case=False, na=False)

        return df[mask].reset_index(drop=True)

    def count_by_type(self) -> dict:
        """Return counts by training data type."""
        return self.vn.training_summary()

    # ── Remove operations ────────────────────────────────────────────────

    def remove_example(self, id: str) -> dict:
        """Remove a single training entry by ID."""
        try:
            self.vn.remove_training_data(id=id)
            logger.info("Removed training entry: %s", id)
            return {"success": True, "message": f"Deleted {id}"}
        except Exception as exc:
            logger.error("Failed to remove %s: %s", id, exc)
            return {"success": False, "message": str(exc)}

    def remove_many(self, ids: list[str]) -> dict:
        """Batch remove training entries by ID."""
        deleted = failed = 0
        for entry_id in ids:
            result = self.remove_example(entry_id)
            if result["success"]:
                deleted += 1
            else:
                failed += 1
        return {"deleted": deleted, "failed": failed}

    def remove_all_by_type(self, data_type: str) -> dict:
        """Remove ALL entries of a given type (use with caution)."""
        df = self.get_all()
        if df.empty:
            return {"deleted": 0, "failed": 0}

        type_entries = df[df["training_data_type"] == data_type]
        ids = type_entries["id"].tolist()
        return self.remove_many(ids)

    # ── Export ───────────────────────────────────────────────────────────

    def export_examples_json(self, output_path: Path | str) -> int:
        """
        Export all Q→SQL examples to a JSON file.

        Returns:
            Number of examples exported.
        """
        df = self.list_examples()
        if df.empty:
            logger.warning("No examples to export.")
            return 0

        examples = []
        for _, row in df.iterrows():
            entry = {"question": row.get("question", ""), "sql": row.get("content", "")}
            if entry["question"] and entry["sql"]:
                examples.append(entry)

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(examples, fh, indent=2, ensure_ascii=False)

        logger.info("Exported %d examples to %s", len(examples), output_path)
        return len(examples)

    # ── Internal helpers ─────────────────────────────────────────────────

    def _is_duplicate_question(self, question: str) -> bool:
        """Check if an identical question already exists in training data."""
        df = self.list_examples()
        if df.empty or "question" not in df.columns:
            return False

        q_clean = question.strip().lower()
        existing = df["question"].dropna().str.strip().str.lower()
        return q_clean in existing.values


__all__ = ["ExampleManager"]