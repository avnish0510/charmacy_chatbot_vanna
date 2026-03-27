# 💄 Charmacy Milano — AI-Powered Sales Analytics Chatbot

A **text-to-SQL chatbot** that lets non-technical users query Charmacy Milano's multi-platform e-commerce data using plain English. Powered by **Vanna AI**, **Ollama** (local LLM), **ChromaDB** (vector store), and **Streamlit** (frontend).

---

## 🏗️ Architecture

```
User Question (English)
        │
        ▼
┌─────────────────────────────────┐
│  Preprocess                     │
│  • Resolve dates ("last month") │
│  • Extract chart hint           │
│  • Clean question for SQL gen   │
└────────────┬────────────────────┘
             ▼
┌─────────────────────────────────┐
│  Vanna (RAG Pipeline)           │
│  1. Embed question              │
│  2. Retrieve DDL + docs + Q→SQL │
│  3. Assemble prompt             │
│  4. Ollama qwen3:9b → SQL       │
└────────────┬────────────────────┘
             ▼
┌─────────────────────────────────┐
│  SQL Validator (2-phase)        │
│  Phase 1: Security (hard block) │
│  Phase 2: Sanity (auto-fix/retry│
└────────────┬────────────────────┘
             ▼
┌─────────────────────────────────┐
│  Execute on SQL Server (SSMS)   │
│  → pandas DataFrame             │
│  Error? → Auto-retry (max 2)    │
└────────────┬────────────────────┘
             ▼
┌─────────────────────────────────┐
│  Chart Pipeline                 │
│  1. Edge case handling          │
│  2. Data shape analysis         │
│  3. Chart type selection        │
│  4. Vega-Lite spec generation   │
│  5. Theme application           │
│  6. Insight annotations         │
│  7. Render in Streamlit         │
└────────────┬────────────────────┘
             ▼
┌─────────────────────────────────┐
│  Feedback (👍 👎 ✏️)            │
│  👍 → train Vanna + log        │
│  👎 → log only (no training)   │
│  ✏️ → edit SQL → validate →    │
│       execute → train + log     │
└─────────────────────────────────┘
```

---

## 📁 Project Structure

```
project/
├── config/
│   ├── database.yaml          # SQL Server connection config
│   ├── vanna_config.yaml      # Ollama model + ChromaDB path
│   ├── chart_theme.json       # Vega-Lite theme (beautiful charts)
│   └── rules.yaml             # Chart type selection rules
│
├── core/
│   ├── vanna_instance.py      # MyVanna class + singleton + T-SQL rules
│   ├── sql_validator.py       # 2-phase SQL validation (security + sanity)
│   └── error_recovery.py      # Auto-retry with error context
│
├── charts/
│   ├── data_shape_analyzer.py # Classify columns, detect data patterns
│   ├── chart_type_selector.py # Rule-based chart type selection
│   ├── chart_spec_generator.py# Build Vega-Lite specs (15 chart types)
│   ├── theme_engine.py        # Load + apply chart_theme.json
│   ├── insight_annotator.py   # Max/min callouts, trend detection
│   ├── edge_case_handler.py   # Empty data, KPI, pre-aggregation
│   └── templates/             # Reference Vega-Lite templates (16 types)
│
├── feedback/
│   ├── feedback_collector.py  # 👍 👎 ✏️ handlers
│   ├── example_manager.py     # Vanna training data CRUD
│   └── analytics.py           # Query history + accuracy tracking
│
├── persistence/
│   ├── sqlite_store.py        # SQLite query/feedback logging
│   └── queries.db             # Auto-created
│
├── training/
│   ├── ddl/*.sql              # Table/view CREATE statements
│   ├── documentation/*.md     # Business rules, column definitions
│   └── examples/
│       └── seed_examples.json # Q→SQL training pairs
│
├── scripts/
│   ├── train_vanna.py         # One-time training pipeline
│   ├── introspect_ssms.py     # Auto-extract DDL from SQL Server
│   └── test_connection.py     # Verify SSMS + Ollama + Vanna
│
├── streamlit_app/
│   ├── app.py                 # Main entry point
│   ├── components/
│   │   ├── chat.py            # Chat orchestrator + sidebar
│   │   ├── chart_renderer.py  # Vega-Lite rendering via vega-embed
│   │   ├── sql_viewer.py      # Collapsible SQL display
│   │   ├── data_table.py      # Formatted DataFrame display
│   │   ├── feedback_bar.py    # 👍 👎 ✏️ UI controls
│   │   └── kpi_card.py        # KPI card rendering (₹, Cr, L)
│   └── pages/
│       ├── 01_chat.py         # Main Q&A page
│       ├── 02_history.py      # Past queries
│       └── 03_admin.py        # Training data management
│
├── vectordb/                  # ChromaDB persistence (gitignore)
├── logs/                      # queries.log, errors.log, security.log
├── requirements.txt
├── .env                       # Credentials (NEVER commit)
└── README.md
```

---

## 🚀 Quick Start

### 1. Prerequisites

- **Python 3.10+**
- **SQL Server** with ODBC Driver 17
- **Ollama** running locally (`ollama serve`)
- **qwen3:9b model** pulled (`ollama pull qwen3:9b`)

### 2. Install

```bash
git clone <repo-url>
cd charmacy_chatbot_vanna
pip install -r requirements.txt
```

### 3. Configure

```bash
# Copy and edit .env with your SQL Server credentials
cp .env.example .env
# Edit .env: set DB_SERVER, DB_USER, DB_PASSWORD
```

### 4. Verify Connections

```bash
python scripts/test_connection.py
```

This checks:
- ✅ SQL Server connectivity + [dbo].[B2B_B2C] view
- ✅ Ollama reachable + qwen3:9b model loaded
- ✅ Vanna initialisation + ChromaDB + RAG pipeline

### 5. Train Vanna (one-time)

```bash
# Full training: DDL + documentation + Q→SQL examples
python scripts/train_vanna.py

# Or step by step:
python scripts/train_vanna.py --ddl-only
python scripts/train_vanna.py --docs-only
python scripts/train_vanna.py --examples-only

# Preview without training:
python scripts/train_vanna.py --dry-run
```

### 6. Launch

```bash
streamlit run streamlit_app/app.py
```

Open http://localhost:8501 in your browser.

---

## 📊 Database Reference

| Field | Detail |
|---|---|
| **Database** | `Charmacy_f_automate` |
| **View** | `[dbo].[B2B_B2C]` |
| **Columns** | 42 |
| **Rows** | ~10,000–15,000 (growing daily) |
| **Grain** | One row = one line-item (one product in one order) |
| **Currency** | INR (₹) |
| **Platforms** | Amazon, Flipkart, Myntra, Shopify, Nykaa, Zepto |

### Critical Data Rules

1. **MRP = total line-item amount** (not unit price). `SUM(MRP)` = revenue.
2. **Always apply the net sales filter** (exclude cancels/returns/unfulfilled).
3. **Order counting differs by platform** — see `TSQL_RULES` in `vanna_instance.py`.
4. **Shopify has NO `order_date`** — date filters silently exclude Shopify.
5. **`ship_to_state`** has mixed formats — always use `IN()` with both spellings.
6. **Never mix Primary + Secondary** `salestype` in revenue sums.

---

## 🛡️ Security

- SQL validation blocks all DML/DDL, linked servers, stacked statements
- Database login uses **`db_datareader`** role (read-only)
- Credentials in `.env` (never committed — see `.gitignore`)
- Security violations logged to `logs/security.log`

---

## 📈 Chart Types Supported

| Chart | When Used |
|---|---|
| KPI Card | Single value or single-row results |
| Vertical Bar | 1 category (≤12 values) + 1 numeric |
| Horizontal Bar | 1 category (>12 values) + 1 numeric |
| Line | 1 temporal + 1 numeric (time series) |
| Multi-Line | 1 temporal + category + numeric |
| Area | Time series with area hint |
| Donut | Part-of-whole with ≤8 slices |
| Scatter | 2 numeric columns |
| Bubble | 3 numeric columns (third as size) |
| Histogram | Single numeric distribution |
| Heatmap | 2 categorical + 1 numeric (matrix) |
| Grouped Bar | 2 categorical + 1 numeric |
| Diverging Bar | Comparison with positive/negative values |
| Table | Fallback for complex or wide results |

All charts feature: rounded corners, no axis lines, soft grid, Inter font, and the Charmacy Milano color palette.

---

## 🔧 Development

```bash
# Auto-extract DDL from SQL Server
python scripts/introspect_ssms.py --train

# Reset all training data and retrain
python scripts/train_vanna.py --reset

# Export trained Q→SQL examples to JSON
# (Use ExampleManager in Python)
```

---

## 📝 License

Internal project — Charmacy Milano.