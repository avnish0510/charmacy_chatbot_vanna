
"""
streamlit_app/components/chart_renderer.py

Render Vega-Lite chart specifications in Streamlit using vega-embed.

Features:
  - Responsive HTML container with custom styling
  - Export actions (PNG, SVG via vega-embed toolbar)
  - Graceful fallback to st.dataframe on render failure
  - Beautiful container with subtle shadow and rounded corners
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

import streamlit as st
import streamlit.components.v1 as components

logger = logging.getLogger(__name__)


# ── Vega-Embed HTML template ──────────────────────────────────────────────
# Styled with the Slate (#0F172A) + Rose (#E11D48) colour palette.
# Responsive container, subtle shadow, rounded corners, clean toolbar.
_VEGA_EMBED_TEMPLATE = """
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"
          rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
      * {{
        margin: 0;
        padding: 0;
        box-sizing: border-box;
      }}
      body {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont,
                     'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        background: transparent;
        display: flex;
        justify-content: center;
        padding: 8px 0;
      }}
      #vis-container {{
        background: #ede9f7;
        border-radius: 14px;
        padding: 20px 24px 16px 24px;
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06),
                    0 1px 2px rgba(15, 23, 42, 0.04);
        border: 1px solid #E2E8F0;
        max-width: 100%;
        overflow-x: auto;
      }}
      #vis {{
        width: 100%;
      }}
      /* Vega-embed action menu styling */
      .vega-actions a {{
        font-family: 'Inter', sans-serif !important;
        font-size: 11px !important;
        color: #64748B !important;
        padding: 5px 10px !important;
        border-radius: 4px !important;
        transition: all 0.15s ease !important;
      }}
      .vega-actions a:hover {{
        color: #E11D48 !important;
        background: #FFF1F2 !important;
      }}
      .vega-embed summary {{
        opacity: 0.3;
        transition: opacity 0.2s ease;
      }}
      .vega-embed summary:hover {{
        opacity: 0.8;
      }}
    </style>
  </head>
  <body>
    <div id="vis-container">
      <div id="vis"></div>
    </div>
    <script>
      var spec = {spec_json};
      var embedOpt = {{
        actions: {actions},
        renderer: "svg",
        theme: "none",
        config: {{
          autosize: {{ type: "fit", contains: "padding" }}
        }}
      }};
      vegaEmbed('#vis', spec, embedOpt).then(function(result) {{
        // Chart rendered successfully
      }}).catch(function(error) {{
        document.getElementById('vis').innerHTML =
          '<p style="color:#EF4444; padding:20px; font-family:Inter,sans-serif; '
          + 'font-size:13px;">Chart render error: ' + error.message + '</p>';
        console.error('Vega-Embed error:', error);
      }});
    </script>
  </body>
</html>
"""


def render_chart(
    spec: Dict[str, Any],
    chart_type: str = "",
    height: int | None = None,
    show_actions: bool = True,
) -> None:
    """
    Render a Vega-Lite spec inside a beautiful container.

    Args:
        spec:         Complete Vega-Lite JSON spec dict.
        chart_type:   The chart type string (for logging and fallback logic).
        height:       Override iframe height in pixels.
                      Auto-calculated from spec if not provided.
        show_actions: Whether to show the vega-embed export toolbar.
    """

    # ── Table fallback ─────────────────────────────────────────────
    if chart_type == "table" or spec.get("_chart_type") == "table":
        data_values = spec.get("data", {}).get("values", [])
        if data_values:
            import pandas as pd
            st.dataframe(pd.DataFrame(data_values), use_container_width=True)
        else:
            st.info("No data to display.")
        return

    # ── Calculate height ───────────────────────────────────────────
    if height is None:
        spec_height = spec.get("height", 380)
        # Account for container padding, title, legend
        if isinstance(spec_height, (int, float)):
            height = int(spec_height) + 120
        else:
            height = 500

    # ── Build HTML ─────────────────────────────────────────────────
    try:
        spec_json = json.dumps(spec, default=str, ensure_ascii=False)
        actions_str = "true" if show_actions else "false"

        html = _VEGA_EMBED_TEMPLATE.format(
            spec_json=spec_json,
            actions=actions_str,
        )

        components.html(html, height=height, scrolling=False)
        logger.debug("Rendered chart: type=%s height=%d", chart_type, height)

    except Exception as exc:
        logger.error("Chart rendering failed: %s", exc, exc_info=True)
        st.warning(f"⚠️ Chart rendering failed: {exc}")

        # Fallback: show raw data if available
        data_values = spec.get("data", {}).get("values", [])
        if data_values:
            import pandas as pd
            st.dataframe(pd.DataFrame(data_values), use_container_width=True)


def render_chart_with_title(
    spec: Dict[str, Any],
    title: str,
    chart_type: str = "",
    subtitle: str = "",
) -> None:
    """
    Render a chart with a Streamlit-native title above it.

    Useful when you want the title outside the Vega iframe
    (e.g. for consistent styling with the rest of the page).
    """
    if title:
        st.markdown(
            f"<h3 style='margin-bottom:4px; color:#0F172A; font-weight:700; "
            f"font-size:16px; letter-spacing:-0.01em;'>{title}</h3>",
            unsafe_allow_html=True,
        )
    if subtitle:
        st.caption(subtitle)

    render_chart(spec, chart_type=chart_type)


__all__ = ["render_chart", "render_chart_with_title"]