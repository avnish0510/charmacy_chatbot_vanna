"""
charts/theme_engine.py

Load and apply the Vega-Lite theme from config/chart_theme.json
to any Vega-Lite specification dict.

Public API:
    from charts.theme_engine import apply_theme, get_theme_config
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
THEME_PATH = ROOT / "config" / "chart_theme.json"

# ── Cached theme ─────────────────────────────────────────────────────────────
_theme_cache: Dict[str, Any] | None = None


def get_theme_config() -> Dict[str, Any]:
    """
    Load and cache the Vega-Lite theme from chart_theme.json.

    Returns the 'config' block from the theme file, which is what
    Vega-Lite expects under the top-level "config" key of a spec.
    """
    global _theme_cache
    if _theme_cache is not None:
        return _theme_cache

    if not THEME_PATH.exists():
        logger.warning(
            "chart_theme.json not found at %s — using empty theme.", THEME_PATH
        )
        _theme_cache = {}
        return _theme_cache

    try:
        with open(THEME_PATH, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except json.JSONDecodeError as exc:
        logger.error("Malformed chart_theme.json: %s — using empty theme.", exc)
        _theme_cache = {}
        return _theme_cache

    # The file stores the theme under a "config" key (Vega-Lite convention).
    _theme_cache = raw.get("config", raw)
    logger.debug("Loaded chart theme with %d top-level keys.", len(_theme_cache))
    return _theme_cache


def get_color_palette() -> list[str]:
    """Return the categorical colour palette from the theme."""
    theme = get_theme_config()
    range_cfg = theme.get("range", {})
    return range_cfg.get("category", [
        "#6366f1", "#f59e0b", "#10b981", "#ef4444",
        "#8b5cf6", "#ec4899", "#06b6d4", "#f97316",
    ])


def apply_theme(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge the theme config into a Vega-Lite spec.

    The theme is applied as a deep merge into spec["config"], so
    spec-level overrides take precedence over theme defaults.

    Args:
        spec: A complete Vega-Lite specification dict.

    Returns:
        The same spec dict (mutated in-place AND returned for convenience).
    """
    theme_config = get_theme_config()
    if not theme_config:
        return spec

    existing_config = spec.get("config", {})

    # Deep merge: theme_config is the base, existing_config overrides
    merged = _deep_merge(theme_config, existing_config)
    spec["config"] = merged

    # Ensure schema is set
    if "$schema" not in spec:
        spec["$schema"] = "https://vega.github.io/schema/vega-lite/v5.json"

    return spec


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """
    Recursively merge override into base.
    override values take precedence over base values.
    """
    result = base.copy()
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


__all__ = ["apply_theme", "get_theme_config", "get_color_palette"]