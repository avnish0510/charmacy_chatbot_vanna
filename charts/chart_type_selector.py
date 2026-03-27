"""
charts/chart_type_selector.py

Select the appropriate chart type based on DataShape analysis and
rules from config/rules.yaml.

Public API:
    from charts.chart_type_selector import select_chart_type

    chart_type: str = select_chart_type(shape, chart_hint="")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from charts.data_shape_analyzer import DataShape

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
RULES_PATH = ROOT / "config" / "rules.yaml"

# ── Cached rules ─────────────────────────────────────────────────────────────
_rules_cache: Dict[str, Any] | None = None


def _load_rules() -> Dict[str, Any]:
    """Load and cache rules.yaml. Returns empty dict on missing/malformed file."""
    global _rules_cache
    if _rules_cache is not None:
        return _rules_cache
    if not RULES_PATH.exists():
        logger.warning("rules.yaml not found at %s — using empty rules.", RULES_PATH)
        _rules_cache = {}
        return _rules_cache
    with open(RULES_PATH, "r", encoding="utf-8") as fh:
        _rules_cache = yaml.safe_load(fh) or {}
    logger.debug("Loaded %d rules from %s", len(_rules_cache.get("rules", [])), RULES_PATH)
    return _rules_cache


def _get_hint_aliases() -> Dict[str, str]:
    """Return chart_hint_aliases mapping from rules.yaml."""
    rules = _load_rules()
    return rules.get("chart_hint_aliases", {})


def _resolve_chart_hint(raw_hint: str) -> str:
    """Normalize a user chart hint to a canonical chart type name."""
    if not raw_hint:
        return ""
    hint_lower = raw_hint.strip().lower()
    aliases = _get_hint_aliases()
    return aliases.get(hint_lower, hint_lower)


def _condition_matches(
    condition_key: str,
    condition_value: Any,
    shape: DataShape,
    resolved_hint: str,
) -> bool:
    """
    Check if a single condition from a rule matches the DataShape.

    Supported condition keys:
        n_rows, n_cols, n_numeric, n_temporal, n_categorical, n_boolean
            → exact match (int == int)
        n_numeric_min, n_temporal_min, n_categorical_min, n_categories_min
            → greater-than-or-equal (int >= int)
        n_categories_max
            → less-than-or-equal (int <= int)
        data_pattern
            → string match
        chart_hint
            → string match against resolved hint
    """
    # Min / max suffixed conditions
    if condition_key.endswith("_min"):
        base_key = condition_key[:-4]  # e.g. "n_numeric_min" → "n_numeric"
        actual = getattr(shape, base_key, None)
        if actual is None:
            return False
        return actual >= condition_value

    if condition_key.endswith("_max"):
        base_key = condition_key[:-4]
        actual = getattr(shape, base_key, None)
        if actual is None:
            return False
        return actual <= condition_value

    # chart_hint
    if condition_key == "chart_hint":
        return resolved_hint == condition_value

    # data_pattern
    if condition_key == "data_pattern":
        return shape.data_pattern == condition_value

    # Direct attribute match
    actual = getattr(shape, condition_key, None)
    if actual is None:
        logger.debug("Unknown condition key: %s", condition_key)
        return False
    return actual == condition_value


def _evaluate_rule(
    rule: Dict[str, Any],
    shape: DataShape,
    resolved_hint: str,
) -> bool:
    """Return True if ALL conditions in a rule are satisfied."""
    conditions = rule.get("conditions", {})
    if not conditions:
        # Empty conditions → fallback rule (always matches)
        return True
    return all(
        _condition_matches(k, v, shape, resolved_hint)
        for k, v in conditions.items()
    )


# ── Public API ───────────────────────────────────────────────────────────────

def select_chart_type(
    shape: DataShape,
    chart_hint: str = "",
) -> str:
    """
    Determine the best chart type for the given DataShape.

    Resolution order:
        1. If chart_hint is provided and maps to a valid chart type,
           use it (user intent overrides rules).
        2. Evaluate rules from config/rules.yaml top-to-bottom.
           First matching rule wins.
        3. Fallback to "table" if no rule matches.

    Args:
        shape: Result of analyze_data_shape(df, question).
        chart_hint: Raw chart-type hint extracted from user's question
                    (e.g. "show as bar chart" → "bar chart").

    Returns:
        A canonical chart type string, e.g. "bar_vertical", "line",
        "kpi_card", "table", etc.
    """
    # ── Handle edge-case flags before rules ──────────────────────────────
    if shape.is_empty or shape.all_null:
        logger.info("Data is empty or all-null → table (with warning).")
        return "table"

    if shape.all_identical and shape.n_rows > 1:
        logger.info("All values identical → table.")
        return "table"

    if shape.is_single_value:
        logger.info("Single value → kpi_card.")
        return "kpi_card"

    # ── Chart hint override ──────────────────────────────────────────────
    resolved_hint = _resolve_chart_hint(chart_hint)
    if resolved_hint:
        # Validate the hint is a known chart type
        known_types = {
            "bar_vertical", "bar_horizontal", "line", "multi_line",
            "area", "pie", "donut", "scatter", "bubble", "histogram",
            "heatmap", "grouped_bar", "kpi_card", "diverging_bar", "table",
        }
        if resolved_hint in known_types:
            logger.info(
                "Chart hint override: '%s' → '%s'", chart_hint, resolved_hint
            )
            return resolved_hint
        else:
            logger.warning(
                "Unrecognised chart hint '%s' (resolved: '%s') — "
                "falling through to rules.",
                chart_hint, resolved_hint,
            )

    # ── Rule evaluation ──────────────────────────────────────────────────
    rules_config = _load_rules()
    rules_list = rules_config.get("rules", [])

    for rule in rules_list:
        if _evaluate_rule(rule, shape, resolved_hint):
            chart_type = rule.get("chart_type", "table")
            logger.info(
                "Rule matched: '%s' → chart_type='%s'",
                rule.get("name", "unnamed"), chart_type,
            )
            return chart_type

    # ── Absolute fallback ────────────────────────────────────────────────
    logger.info("No rule matched — defaulting to 'table'.")
    return "table"


def get_thresholds() -> Dict[str, int]:
    """Return the thresholds dict from rules.yaml (for use by other modules)."""
    rules = _load_rules()
    return rules.get("thresholds", {})


__all__ = ["select_chart_type", "get_thresholds"]