"""
streamlit_app/pages/03_admin.py

Admin page for managing Vanna training data.

Features:
    - View all training data (DDL, documentation, Q→SQL examples)
    - Add new Q→SQL examples manually
    - Add new documentation chunks
    - Delete individual training entries
    - Training coverage warnings
    - Bulk import from JSON file
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.vanna_instance import get_vanna, MyVanna

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Admin — Charmacy Milano AI",
    page_icon="⚙️",
    layout="wide",
)


@st.cache_resource
def _load_vanna() -> MyVanna:
    return get_vanna()


def main() -> None:
    # ── Load Vanna ───────────────────────────────────────────────────────
    try:
        vn = _load_vanna()
    except Exception as exc:
        st.error(f"❌ Failed to initialise Vanna: {exc}")
        st.stop()

    # ── Header ───────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="padding:8px 0 20px 0;">
            <h1 style="margin:0; color:#1a1a2e; font-weight:700; font-size:28px;">
                ⚙️ Training Data Admin
            </h1>
            <p style="margin:4px 0 0 0; color:#6b7280; font-size:14px;">
                Manage DDL, documentation, and Q→SQL training examples
                stored in ChromaDB.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Training summary ─────────────────────────────────────────────────
    _render_training_summary(vn)

    st.divider()

    # ── Tabs ─────────────────────────────────────────────────────────────
    tab_view, tab_add_example, tab_add_doc, tab_import, tab_delete = st.tabs([
        "📋 View Training Data",
        "➕ Add Q→SQL Example",
        "📝 Add Documentation",
        "📦 Bulk Import",
        "🗑️ Delete Entries",
    ])

    with tab_view:
        _render_view_training_data(vn)

    with tab_add_example:
        _render_add_example(vn)

    with tab_add_doc:
        _render_add_documentation(vn)

    with tab_import:
        _render_bulk_import(vn)

    with tab_delete:
        _render_delete_entries(vn)


# ── Training summary ────────────────────────────────────────────────────────

def _render_training_summary(vn: MyVanna) -> None:
    """Show training data counts and coverage warnings."""
    try:
        summary = vn.training_summary()
    except Exception as exc:
        st.warning(f"Could not load training summary: {exc}")
        return

    total = summary.get("total", 0)
    sql_count = summary.get("sql", 0)
    ddl_count = summary.get("ddl", 0)
    doc_count = summary.get("documentation", 0)

    cols = st.columns(4)
    cols[0].metric("📊 Q→SQL Examples", sql_count)
    cols[1].metric("🏗️ DDL Schemas", ddl_count)
    cols[2].metric("📄 Documentation", doc_count)
    cols[3].metric("📦 Total Entries", total)

    # Coverage warnings
    if sql_count < 20:
        st.warning(
            f"⚠️ **Low training coverage**: Only **{sql_count}** Q→SQL examples. "
            f"For reliable SQL generation, add at least **20** diverse examples. "
            f"Aim for **50+** for production quality."
        )
    elif sql_count < 50:
        st.info(
            f"ℹ️ **{sql_count}** Q→SQL examples. Good start! "
            f"Adding more diverse examples will improve accuracy."
        )
    else:
        st.success(
            f"✅ **{sql_count}** Q→SQL examples. Training coverage looks good!"
        )

    if ddl_count == 0:
        st.error(
            "❌ **No DDL trained**. The model doesn't know your table schemas. "
            "Run `python scripts/train_vanna.py` to train DDL."
        )

    if doc_count == 0:
        st.warning(
            "⚠️ **No documentation trained**. Add business rules, column definitions, "
            "and T-SQL rules for better accuracy."
        )


# ── View training data ──────────────────────────────────────────────────────

def _render_view_training_data(vn: MyVanna) -> None:
    """Display all training data in a searchable table."""
    st.markdown("### All Training Data")

    try:
        training_df = vn.get_training_data()
    except Exception as exc:
        st.error(f"Failed to load training data: {exc}")
        return

    if training_df is None or training_df.empty:
        st.info("No training data found. Use the tabs above to add some.")
        return

    # ── Filter by type ───────────────────────────────────────────────────
    col1, col2 = st.columns([2, 4])

    with col1:
        type_options = ["All"] + sorted(
            training_df["training_data_type"].dropna().unique().tolist()
        )
        type_filter = st.selectbox(
            "Filter by type:",
            options=type_options,
            key="admin_type_filter",
        )

    with col2:
        search = st.text_input(
            "Search:",
            placeholder="Search questions, SQL, documentation…",
            key="admin_search",
        )

    # ── Apply filters ────────────────────────────────────────────────────
    filtered = training_df.copy()

    if type_filter != "All":
        filtered = filtered[filtered["training_data_type"] == type_filter]

    if search:
        mask = pd.Series([False] * len(filtered), index=filtered.index)
        for col in filtered.columns:
            if filtered[col].dtype == "object":
                mask |= filtered[col].str.contains(search, case=False, na=False)
        filtered = filtered[mask]

    st.caption(f"Showing {len(filtered)} of {len(training_df)} entries")

    # ── Display ──────────────────────────────────────────────────────────
    st.dataframe(
        filtered,
        use_container_width=True,
        hide_index=True,
        column_config={
            "id": st.column_config.TextColumn("ID", width="small"),
            "training_data_type": st.column_config.TextColumn("Type", width="small"),
            "question": st.column_config.TextColumn("Question", width="medium"),
            "content": st.column_config.TextColumn("Content", width="large"),
        },
    )


# ── Add Q→SQL example ───────────────────────────────────────────────────────

def _render_add_example(vn: MyVanna) -> None:
    """Form to add a single Q→SQL training example."""
    st.markdown("### Add Q→SQL Training Example")
    st.caption(
        "Add a natural language question and its correct SQL query. "
        "This is the most impactful way to improve accuracy."
    )

    with st.form("add_example_form", clear_on_submit=True):
        question = st.text_input(
            "Question (natural language):",
            placeholder="e.g. What is the total revenue by platform?",
        )
        sql = st.text_area(
            "SQL (correct T-SQL query):",
            height=200,
            placeholder=(
                "SELECT platform, SUM(MRP) AS revenue\n"
                "FROM [dbo].[B2B_B2C]\n"
                "WHERE NOT (\n"
                "    (platform='Amazon' AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))\n"
                "    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))\n"
                "    OR (platform='Shopify' AND ISNULL(fulfilment_type,'') = 'unfulfilled')\n"
                ")\n"
                "GROUP BY platform\n"
                "ORDER BY revenue DESC"
            ),
        )

        submitted = st.form_submit_button("➕ Add Example", type="primary")

    if submitted:
        if not question or not question.strip():
            st.error("Question cannot be empty.")
        elif not sql or not sql.strip():
            st.error("SQL cannot be empty.")
        else:
            try:
                vn.train(question=question.strip(), sql=sql.strip())
                st.success(
                    f"✅ Training example added!\n\n"
                    f"**Q:** {question.strip()}\n\n"
                    f"**SQL:** `{sql.strip()[:100]}…`"
                )
                # Clear cache so training summary updates
                st.cache_resource.clear()
            except Exception as exc:
                st.error(f"❌ Failed to add example: {exc}")


# ── Add documentation ────────────────────────────────────────────────────────

def _render_add_documentation(vn: MyVanna) -> None:
    """Form to add a documentation chunk."""
    st.markdown("### Add Documentation / Business Rules")
    st.caption(
        "Add text that describes business rules, column meanings, "
        "data quirks, or SQL patterns. This helps the model understand "
        "your data better."
    )

    with st.form("add_doc_form", clear_on_submit=True):
        doc_text = st.text_area(
            "Documentation text:",
            height=250,
            placeholder=(
                "MRP is the total line-item amount (quantity × unit price), "
                "NOT the unit price. SUM(MRP) = Total Revenue. "
                "Per-unit price = MRP / NULLIF(quantity, 0)."
            ),
        )

        submitted = st.form_submit_button("📝 Add Documentation", type="primary")

    if submitted:
        if not doc_text or not doc_text.strip():
            st.error("Documentation text cannot be empty.")
        else:
            try:
                vn.train(documentation=doc_text.strip())
                st.success("✅ Documentation chunk added to training data!")
                st.cache_resource.clear()
            except Exception as exc:
                st.error(f"❌ Failed to add documentation: {exc}")


# ── Bulk import ──────────────────────────────────────────────────────────────

def _render_bulk_import(vn: MyVanna) -> None:
    """Import Q→SQL examples from a JSON file."""
    st.markdown("### Bulk Import Q→SQL Examples")
    st.caption(
        "Upload a JSON file with an array of `{\"question\": \"...\", \"sql\": \"...\"}` objects."
    )

    uploaded = st.file_uploader(
        "Upload JSON file:",
        type=["json"],
        key="bulk_import_file",
    )

    if uploaded:
        try:
            content = json.loads(uploaded.read())
        except json.JSONDecodeError as exc:
            st.error(f"Invalid JSON: {exc}")
            return

        if not isinstance(content, list):
            st.error("JSON must be an array of objects.")
            return

        st.info(f"Found **{len(content)}** examples in the file.")

        # Preview
        with st.expander("Preview (first 5)", expanded=True):
            for i, item in enumerate(content[:5]):
                st.markdown(f"**{i+1}. Q:** {item.get('question', '?')}")
                st.code(item.get("sql", "?"), language="sql")

        if st.button(
            f"🚀 Import all {len(content)} examples",
            type="primary",
        ):
            success_count = 0
            fail_count = 0
            progress = st.progress(0)

            for i, item in enumerate(content):
                q = item.get("question", "").strip()
                s = item.get("sql", "").strip()

                if not q or not s:
                    fail_count += 1
                    continue

                try:
                    vn.train(question=q, sql=s)
                    success_count += 1
                except Exception:
                    fail_count += 1

                progress.progress((i + 1) / len(content))

            progress.empty()
            st.success(
                f"✅ Imported **{success_count}** examples "
                f"({fail_count} failed/skipped)."
            )
            st.cache_resource.clear()


# ── Delete entries ───────────────────────────────────────────────────────────

def _render_delete_entries(vn: MyVanna) -> None:
    """Delete individual training entries by ID."""
    st.markdown("### Delete Training Entries")
    st.caption(
        "⚠️ **Caution:** Deleted entries cannot be recovered. "
        "Use this to remove incorrect Q→SQL examples or outdated DDL."
    )

    try:
        training_df = vn.get_training_data()
    except Exception as exc:
        st.error(f"Failed to load training data: {exc}")
        return

    if training_df is None or training_df.empty:
        st.info("No training data to delete.")
        return

    # ── Select entries to delete ─────────────────────────────────────────
    st.markdown("**Select entries to delete:**")

    # Show condensed view
    display_df = training_df.copy()
    if "content" in display_df.columns:
        display_df["content_preview"] = display_df["content"].astype(str).str[:100] + "…"

    cols_to_show = [c for c in ["id", "training_data_type", "question", "content_preview"]
                    if c in display_df.columns]

    st.dataframe(
        display_df[cols_to_show],
        use_container_width=True,
        hide_index=True,
    )

    # ── Delete by ID ─────────────────────────────────────────────────────
    st.markdown("---")
    delete_id = st.text_input(
        "Enter the ID of the entry to delete:",
        placeholder="Paste the ID from the table above",
        key="delete_id_input",
    )

    col1, col2 = st.columns([2, 6])
    with col1:
        confirm_delete = st.button(
            "🗑️ Delete Entry",
            type="primary",
            key="confirm_delete_btn",
        )

    if confirm_delete:
        if not delete_id or not delete_id.strip():
            st.error("Please enter an ID.")
        else:
            # Verify ID exists
            if delete_id.strip() not in training_df["id"].values:
                st.error(f"ID `{delete_id.strip()}` not found in training data.")
            else:
                try:
                    vn.remove_training_data(id=delete_id.strip())
                    st.success(f"✅ Deleted entry `{delete_id.strip()}`.")
                    st.cache_resource.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"❌ Delete failed: {exc}")

    # ── Bulk delete by type ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**⚠️ Bulk delete (advanced)**")

    bulk_type = st.selectbox(
        "Delete ALL entries of type:",
        options=["(select)", "sql", "ddl", "documentation"],
        key="bulk_delete_type",
    )

    if bulk_type != "(select)":
        type_entries = training_df[
            training_df["training_data_type"] == bulk_type
        ]
        st.warning(
            f"This will delete **{len(type_entries)}** `{bulk_type}` entries. "
            f"This cannot be undone."
        )

        confirm_text = st.text_input(
            f'Type "{bulk_type}" to confirm:',
            key="bulk_delete_confirm",
        )

        if st.button("🗑️ Bulk Delete", type="primary", key="bulk_delete_btn"):
            if confirm_text.strip().lower() == bulk_type.lower():
                deleted = 0
                progress = st.progress(0)
                for i, row in enumerate(type_entries.itertuples()):
                    try:
                        vn.remove_training_data(id=row.id)
                        deleted += 1
                    except Exception:
                        pass
                    progress.progress((i + 1) / len(type_entries))
                progress.empty()
                st.success(f"✅ Deleted {deleted} / {len(type_entries)} entries.")
                st.cache_resource.clear()
                st.rerun()
            else:
                st.error("Confirmation text doesn't match. Aborting.")


if __name__ == "__main__":
    main()
else:
    main()