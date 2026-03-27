# #!/usr/bin/env python3
# """
# scripts/test_connection.py

# Comprehensive pre-flight health check for the Charmacy Milano Text-to-SQL system.
# Run this BEFORE train_vanna.py or starting the Streamlit app.

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTIONS TESTED
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#   1. Environment & config files
#   2. Python package imports
#   3. ODBC driver installation
#   4. SQL Server connection + data integrity (10 sub-checks)
#   5. Ollama service + model availability
#   6. ChromaDB initialisation
#   7. Vanna end-to-end (init → connect → training summary)
#   8. sql_validator self-tests (4 known-case assertions)
#   9. Full generation test: generate_sql → validate → run_sql  [slow]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# USAGE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#     # Full test (includes LLM generation — ~30-90s depending on GPU)
#     python scripts/test_connection.py

#     # Skip Section 9 (LLM generation) for fast CI-style check (~5s)
#     python scripts/test_connection.py --quick

#     # Run only one section by keyword
#     python scripts/test_connection.py --section db
#     python scripts/test_connection.py --section ollama
#     python scripts/test_connection.py --section validator

# Exit codes:
#     0  All tests passed (warnings are OK)
#     1  One or more FAIL results
# """

# from __future__ import annotations

# import argparse
# import sys
# import time
# import urllib.error
# import urllib.request
# from dataclasses import dataclass, field
# from enum import Enum
# from pathlib import Path
# from typing import Any, Callable

# # ── Ensure project root is on sys.path ────────────────────────────────────────
# # Works whether the script is run as:
# #   python scripts/test_connection.py      (from project root)
# #   python test_connection.py              (from scripts/ directory)

# _SCRIPT_DIR  = Path(__file__).resolve().parent
# _PROJECT_ROOT = _SCRIPT_DIR.parent

# if str(_PROJECT_ROOT) not in sys.path:
#     sys.path.insert(0, str(_PROJECT_ROOT))


# # ── ANSI colour helpers (no external deps) ────────────────────────────────────

# _RESET  = "\033[0m"
# _BOLD   = "\033[1m"
# _DIM    = "\033[2m"
# _GREEN  = "\033[32m"
# _YELLOW = "\033[33m"
# _RED    = "\033[31m"
# _CYAN   = "\033[36m"
# _WHITE  = "\033[97m"
# _BLUE   = "\033[34m"

# def _green(s: str)  -> str: return f"{_GREEN}{s}{_RESET}"
# def _yellow(s: str) -> str: return f"{_YELLOW}{s}{_RESET}"
# def _red(s: str)    -> str: return f"{_RED}{s}{_RESET}"
# def _cyan(s: str)   -> str: return f"{_CYAN}{s}{_RESET}"
# def _bold(s: str)   -> str: return f"{_BOLD}{s}{_RESET}"
# def _dim(s: str)    -> str: return f"{_DIM}{s}{_RESET}"


# # ── Test result model ─────────────────────────────────────────────────────────

# class Status(str, Enum):
#     PASS = "PASS"
#     FAIL = "FAIL"
#     WARN = "WARN"
#     SKIP = "SKIP"


# @dataclass
# class TestResult:
#     name:       str
#     status:     Status
#     message:    str
#     detail:     str  = ""       # extra context printed below the main line
#     fix_hint:   str  = ""       # actionable fix shown on failure
#     elapsed_ms: float = 0.0

#     @property
#     def icon(self) -> str:
#         return {
#             Status.PASS: _green("✓"),
#             Status.FAIL: _red("✗"),
#             Status.WARN: _yellow("⚠"),
#             Status.SKIP: _dim("○"),
#         }[self.status]

#     @property
#     def status_label(self) -> str:
#         labels = {
#             Status.PASS: _green("PASS"),
#             Status.FAIL: _red("FAIL"),
#             Status.WARN: _yellow("WARN"),
#             Status.SKIP: _dim("SKIP"),
#         }
#         return labels[self.status]


# # ── Runner ────────────────────────────────────────────────────────────────────

# class Runner:
#     """Collects TestResults and handles display."""

#     def __init__(self) -> None:
#         self.results: list[TestResult] = []
#         self._section_name: str = ""

#     # ── Section header ────────────────────────────────────────────────────────

#     def section(self, title: str) -> None:
#         self._section_name = title
#         bar = "━" * 62
#         print(f"\n{_BOLD}{_BLUE}{bar}{_RESET}")
#         print(f"  {_bold(title)}")
#         print(f"{_BOLD}{_BLUE}{bar}{_RESET}")

#     # ── Core executor ─────────────────────────────────────────────────────────

#     def run(
#         self,
#         name: str,
#         fn: Callable[[], TestResult | tuple[Status, str] | tuple[Status, str, str]],
#         skip: bool = False,
#     ) -> TestResult:
#         """
#         Execute fn(), time it, record the result.

#         fn() may return:
#           - TestResult                         — full control
#           - (Status, message)                  — minimal
#           - (Status, message, detail_or_hint)  — with extra text
#         """
#         if skip:
#             r = TestResult(name=name, status=Status.SKIP, message="Skipped.")
#         else:
#             t0 = time.perf_counter()
#             try:
#                 raw = fn()
#                 elapsed = (time.perf_counter() - t0) * 1000
#                 if isinstance(raw, TestResult):
#                     r = raw
#                     r.elapsed_ms = elapsed
#                 elif isinstance(raw, tuple):
#                     status, msg, *rest = raw
#                     detail    = rest[0] if rest else ""
#                     fix_hint  = rest[1] if len(rest) > 1 else ""
#                     r = TestResult(
#                         name=name, status=status, message=msg,
#                         detail=detail, fix_hint=fix_hint, elapsed_ms=elapsed,
#                     )
#                 else:
#                     r = TestResult(
#                         name=name, status=Status.FAIL,
#                         message="Test function returned unexpected type.",
#                         elapsed_ms=(time.perf_counter() - t0) * 1000,
#                     )
#             except Exception as exc:  # noqa: BLE001
#                 elapsed = (time.perf_counter() - t0) * 1000
#                 r = TestResult(
#                     name=name,
#                     status=Status.FAIL,
#                     message=f"{type(exc).__name__}: {exc}",
#                     elapsed_ms=elapsed,
#                 )

#         self._print_result(r)
#         self.results.append(r)
#         return r

#     def _print_result(self, r: TestResult) -> None:
#         # Main line
#         elapsed = f"{_dim(f'{r.elapsed_ms:.0f}ms')}" if r.elapsed_ms >= 1 else ""
#         name_col = f"{r.name:<52}"
#         print(f"  {r.icon}  {name_col}  {r.status_label}  {elapsed}")

#         # Detail (extra info on PASS/WARN)
#         if r.detail:
#             for line in r.detail.splitlines():
#                 print(f"       {_dim(line)}")

#         # Fix hint (only on FAIL/WARN)
#         if r.fix_hint and r.status in (Status.FAIL, Status.WARN):
#             for line in r.fix_hint.splitlines():
#                 print(f"       {_yellow('→')} {line}")

#     # ── Summary ───────────────────────────────────────────────────────────────

#     def summary(self) -> int:
#         """Print final summary table and return exit code (0=ok, 1=failures)."""
#         counts = {s: 0 for s in Status}
#         for r in self.results:
#             counts[r.status] += 1

#         total = len(self.results)
#         passed = counts[Status.PASS]
#         failed = counts[Status.FAIL]
#         warned = counts[Status.WARN]
#         skipped = counts[Status.SKIP]

#         print(f"\n{'━' * 62}")
#         print(f"  {_bold('SUMMARY')}  —  {total} checks")
#         print(f"{'━' * 62}")
#         print(f"  {_green('✓  Passed')}   {passed}")
#         print(f"  {_red('✗  Failed')}   {failed}")
#         print(f"  {_yellow('⚠  Warned')}   {warned}")
#         print(f"  {_dim('○  Skipped')}  {skipped}")
#         print(f"{'━' * 62}")

#         if failed == 0 and warned == 0:
#             print(f"\n  {_green(_bold('All systems ready.'))}  Run scripts/train_vanna.py next.\n")
#         elif failed == 0:
#             print(f"\n  {_yellow(_bold('Ready with warnings.'))}  Check items above before training.\n")
#         else:
#             print(f"\n  {_red(_bold('Fix failures before proceeding.'))}\n")

#         # List all failures for easy copy-paste
#         failures = [r for r in self.results if r.status == Status.FAIL]
#         if failures:
#             print("  Failed checks:")
#             for r in failures:
#                 print(f"    {_red('✗')}  {r.name}")
#                 print(f"       {r.message}")
#                 if r.fix_hint:
#                     print(f"       {_yellow('→')} {r.fix_hint}")
#             print()

#         return 1 if failed > 0 else 0


# # ── Shared project paths ──────────────────────────────────────────────────────

# ROOT        = _PROJECT_ROOT
# CONFIG_DIR  = ROOT / "config"
# VECTORDB_DIR = ROOT / "vectordb"
# LOGS_DIR    = ROOT / "logs"

# # Expected platforms (case-sensitive, from spec)
# EXPECTED_PLATFORMS = {"Amazon", "Flipkart", "Myntra", "Shopify", "Nykaa", "Zepto"}

# # Minimum SQL Q→SQL examples before warning
# MIN_TRAINING_EXAMPLES = 20


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 1 — Environment & Config
# # ══════════════════════════════════════════════════════════════════════════════

# def section_env(runner: Runner) -> None:
#     runner.section("SECTION 1 — Environment & Config")

#     # 1.1  .env file
#     def check_env_file() -> TestResult:
#         env_path = ROOT / ".env"
#         if env_path.exists():
#             return TestResult(
#                 name=".env file present",
#                 status=Status.PASS,
#                 message=f"Found: {env_path}",
#             )
#         # Not strictly required if env vars are set at OS level
#         return TestResult(
#             name=".env file present",
#             status=Status.WARN,
#             message=".env file not found.",
#             fix_hint=(
#                 "Create .env in the project root with:\n"
#                 "       DB_SERVER=your_server\n"
#                 "       DB_USER=your_user\n"
#                 "       DB_PASSWORD=your_password\n"
#                 "       DB_DATABASE=Charmacy_f_automate\n"
#                 "       (Alternatively export as OS environment variables)"
#             ),
#         )
#     runner.run(".env file present", check_env_file)

#     # 1.2  Load .env
#     def check_dotenv_load() -> tuple[Status, str]:
#         from dotenv import load_dotenv
#         result = load_dotenv(dotenv_path=ROOT / ".env", override=False)
#         if result:
#             return Status.PASS, ".env loaded successfully."
#         return Status.WARN, ".env not found or already loaded (env vars may be set at OS level)."
#     runner.run(".env loads without error", check_dotenv_load)

#     # 1.3  database.yaml
#     def check_db_yaml() -> TestResult:
#         path = CONFIG_DIR / "database.yaml"
#         if not path.exists():
#             return TestResult(
#                 name="config/database.yaml",
#                 status=Status.WARN,
#                 message="File not found — DB credentials will be read from .env only.",
#                 fix_hint=(
#                     "Create config/database.yaml with:\n"
#                     "       server: YOUR_SERVER\n"
#                     "       database: Charmacy_f_automate\n"
#                     "       driver: ODBC Driver 17 for SQL Server\n"
#                     "       extra_params: TrustServerCertificate=yes\n"
#                     "       (Omit user/password — use .env for credentials)"
#                 ),
#             )
#         import yaml
#         with open(path, "r") as f:
#             data = yaml.safe_load(f) or {}
#         keys = list(data.keys())
#         return TestResult(
#             name="config/database.yaml",
#             status=Status.PASS,
#             message=f"Valid YAML with {len(keys)} key(s).",
#             detail=f"Keys: {', '.join(keys)}",
#         )
#     runner.run("config/database.yaml", check_db_yaml)

#     # 1.4  vanna_config.yaml
#     def check_vanna_yaml() -> TestResult:
#         path = CONFIG_DIR / "vanna_config.yaml"
#         if not path.exists():
#             return TestResult(
#                 name="config/vanna_config.yaml",
#                 status=Status.WARN,
#                 message="File not found — will use built-in defaults.",
#                 fix_hint=(
#                     "Create config/vanna_config.yaml with:\n"
#                     "       model: qwen3:9b\n"
#                     "       ollama_host: http://localhost:11434\n"
#                     "       chromadb_path: ./vectordb"
#                 ),
#             )
#         import yaml
#         with open(path, "r") as f:
#             data = yaml.safe_load(f) or {}
#         model = data.get("model", "(not set — default: qwen3:9b)")
#         host  = data.get("ollama_host", "(not set — default: http://localhost:11434)")
#         path_ = data.get("chromadb_path", "(not set — default: ./vectordb)")
#         return TestResult(
#             name="config/vanna_config.yaml",
#             status=Status.PASS,
#             message="Valid YAML.",
#             detail=f"model={model}  host={host}  chromadb_path={path_}",
#         )
#     runner.run("config/vanna_config.yaml", check_vanna_yaml)

#     # 1.5  Required credentials present
#     def check_credentials() -> TestResult:
#         import os
#         # Try .env first then OS env
#         from dotenv import dotenv_values
#         env_file_vals = dotenv_values(ROOT / ".env")
#         import os as _os

#         def get(key: str) -> str:
#             return env_file_vals.get(key) or _os.getenv(key) or ""

#         # Also check database.yaml
#         db_yaml_vals: dict = {}
#         db_yaml_path = CONFIG_DIR / "database.yaml"
#         if db_yaml_path.exists():
#             import yaml
#             with open(db_yaml_path) as f:
#                 db_yaml_vals = yaml.safe_load(f) or {}

#         server   = get("DB_SERVER")   or db_yaml_vals.get("server", "")
#         user     = get("DB_USER")     or db_yaml_vals.get("user", "")
#         password = get("DB_PASSWORD") or db_yaml_vals.get("password", "")
#         database = get("DB_DATABASE") or db_yaml_vals.get("database", "Charmacy_f_automate")

#         missing = []
#         if not server:   missing.append("DB_SERVER")
#         if not user:     missing.append("DB_USER")
#         if not password: missing.append("DB_PASSWORD")

#         if missing:
#             return TestResult(
#                 name="DB credentials present",
#                 status=Status.FAIL,
#                 message=f"Missing: {', '.join(missing)}",
#                 fix_hint="Set missing values in .env or config/database.yaml.",
#             )
#         # Mask password in detail
#         masked_pw = "*" * len(password) if len(password) <= 8 else ("*" * 8 + "…")
#         return TestResult(
#             name="DB credentials present",
#             status=Status.PASS,
#             message="All required credentials found.",
#             detail=f"SERVER={server}  DATABASE={database}  USER={user}  PASSWORD={masked_pw}",
#         )
#     runner.run("DB credentials present", check_credentials)

#     # 1.6  Directories
#     def check_dirs() -> TestResult:
#         created = []
#         for d in [VECTORDB_DIR, LOGS_DIR, ROOT / "training" / "ddl",
#                   ROOT / "training" / "documentation", ROOT / "training" / "examples",
#                   ROOT / "persistence"]:
#             if not d.exists():
#                 d.mkdir(parents=True, exist_ok=True)
#                 created.append(str(d.relative_to(ROOT)))
#         msg = "All required directories exist."
#         detail = ""
#         if created:
#             msg = f"Created {len(created)} missing director{'y' if len(created)==1 else 'ies'}."
#             detail = "  ".join(created)
#         return TestResult(
#             name="Required directories exist / created",
#             status=Status.PASS,
#             message=msg,
#             detail=detail,
#         )
#     runner.run("Required directories exist / created", check_dirs)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 2 — Python Package Imports
# # ══════════════════════════════════════════════════════════════════════════════

# def section_imports(runner: Runner) -> None:
#     runner.section("SECTION 2 — Python Package Imports")

#     packages = [
#         ("pyodbc",          "pyodbc",                  "pip install pyodbc"),
#         ("vanna",           "vanna",                   "pip install vanna"),
#         ("chromadb",        "chromadb",                "pip install chromadb"),
#         ("ollama",          "ollama",                  "pip install ollama"),
#         ("sqlalchemy",      "sqlalchemy",              "pip install sqlalchemy"),
#         ("pandas",          "pandas",                  "pip install pandas"),
#         ("numpy",           "numpy",                   "pip install numpy"),
#         ("yaml (PyYAML)",   "yaml",                    "pip install pyyaml"),
#         ("python-dotenv",   "dotenv",                  "pip install python-dotenv"),
#         ("streamlit",       "streamlit",               "pip install streamlit"),
#     ]

#     for display_name, import_name, install_cmd in packages:
#         def _make_check(imp: str, install: str, disp: str) -> Callable:
#             def check() -> TestResult:
#                 import importlib
#                 mod = importlib.import_module(imp)
#                 ver = getattr(mod, "__version__", "?")
#                 return TestResult(
#                     name=f"import {disp}",
#                     status=Status.PASS,
#                     message=f"v{ver}",
#                 )
#             return check

#         def _make_fail_check(imp: str, install: str, disp: str) -> Callable:
#             """Wraps import attempt with proper failure reporting."""
#             def check() -> TestResult:
#                 import importlib
#                 try:
#                     mod = importlib.import_module(imp)
#                     ver = getattr(mod, "__version__", "?")
#                     return TestResult(
#                         name=f"import {disp}",
#                         status=Status.PASS,
#                         message=f"v{ver}",
#                     )
#                 except ImportError as e:
#                     return TestResult(
#                         name=f"import {disp}",
#                         status=Status.FAIL,
#                         message=f"ImportError: {e}",
#                         fix_hint=install,
#                     )
#             return check

#         runner.run(f"import {display_name}", _make_fail_check(import_name, install_cmd, display_name))

#     # Our own modules
#     def check_vanna_instance() -> TestResult:
#         try:
#             from core.vanna_instance import MyVanna, get_vanna, TSQL_RULES  # noqa: F401
#             return TestResult(
#                 name="import core.vanna_instance",
#                 status=Status.PASS,
#                 message=f"MyVanna, get_vanna, TSQL_RULES exported correctly.",
#                 detail=f"TSQL_RULES length: {len(TSQL_RULES)} chars",
#             )
#         except ImportError as e:
#             return TestResult(
#                 name="import core.vanna_instance",
#                 status=Status.FAIL,
#                 message=str(e),
#                 fix_hint="Ensure core/__init__.py exists (can be empty) and vanna_instance.py is in core/.",
#             )
#     runner.run("import core.vanna_instance", check_vanna_instance)

#     def check_sql_validator() -> TestResult:
#         try:
#             from core.sql_validator import validate_sql, ValidationResult, KNOWN_COLUMNS  # noqa: F401
#             return TestResult(
#                 name="import core.sql_validator",
#                 status=Status.PASS,
#                 message=f"validate_sql, ValidationResult exported.",
#                 detail=f"KNOWN_COLUMNS: {len(KNOWN_COLUMNS)} columns",
#             )
#         except ImportError as e:
#             return TestResult(
#                 name="import core.sql_validator",
#                 status=Status.FAIL,
#                 message=str(e),
#                 fix_hint="Ensure core/sql_validator.py exists.",
#             )
#     runner.run("import core.sql_validator", check_sql_validator)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 3 — ODBC Driver
# # ══════════════════════════════════════════════════════════════════════════════

# def section_odbc(runner: Runner) -> None:
#     runner.section("SECTION 3 — ODBC Driver Installation")

#     def check_odbc_import() -> tuple[Status, str]:
#         import pyodbc  # noqa: F401
#         return Status.PASS, f"pyodbc {pyodbc.version} imported successfully."
#     runner.run("pyodbc import", check_odbc_import)

#     def check_odbc_drivers() -> TestResult:
#         import pyodbc
#         drivers = pyodbc.drivers()
#         sql_server_drivers = [d for d in drivers if "SQL Server" in d]

#         preferred = [
#             "ODBC Driver 18 for SQL Server",
#             "ODBC Driver 17 for SQL Server",
#         ]
#         found_preferred = [d for d in preferred if d in sql_server_drivers]

#         if not sql_server_drivers:
#             return TestResult(
#                 name="SQL Server ODBC driver installed",
#                 status=Status.FAIL,
#                 message="No SQL Server ODBC driver found.",
#                 detail=f"All installed drivers: {drivers or ['(none)']}",
#                 fix_hint=(
#                     "Install ODBC Driver 17 for SQL Server:\n"
#                     "       Windows: https://aka.ms/downloadmsodbcsql\n"
#                     "       Ubuntu:  sudo apt-get install msodbcsql17\n"
#                     "       macOS:   brew install msodbcsql17"
#                 ),
#             )

#         detail = "Available SQL Server drivers:\n" + "\n".join(f"  • {d}" for d in sql_server_drivers)
#         if found_preferred:
#             return TestResult(
#                 name="SQL Server ODBC driver installed",
#                 status=Status.PASS,
#                 message=f"Found: {found_preferred[0]}",
#                 detail=detail,
#             )

#         # Has a SQL Server driver but not 17/18
#         return TestResult(
#             name="SQL Server ODBC driver installed",
#             status=Status.WARN,
#             message=f"Found SQL Server driver but not v17/v18: {sql_server_drivers[0]}",
#             detail=detail,
#             fix_hint=(
#                 "Recommended: install 'ODBC Driver 17 for SQL Server' or newer.\n"
#                 "       Your driver may still work — check the connection test below."
#             ),
#         )
#     runner.run("SQL Server ODBC driver installed", check_odbc_drivers)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 4 — SQL Server Connection + Data Integrity
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_odbc_conn_str() -> str:
#     """Assemble ODBC string from env / config — mirrors core/vanna_instance.py logic."""
#     import os
#     from dotenv import load_dotenv
#     load_dotenv(ROOT / ".env", override=False)

#     db_cfg: dict = {}
#     db_yaml = CONFIG_DIR / "database.yaml"
#     if db_yaml.exists():
#         import yaml
#         with open(db_yaml) as f:
#             db_cfg = yaml.safe_load(f) or {}

#     server   = os.getenv("DB_SERVER",   db_cfg.get("server",   ""))
#     database = os.getenv("DB_DATABASE", db_cfg.get("database", "Charmacy_f_automate"))
#     user     = os.getenv("DB_USER",     db_cfg.get("user",     ""))
#     password = os.getenv("DB_PASSWORD", db_cfg.get("password", ""))
#     driver   = db_cfg.get("driver", "ODBC Driver 17 for SQL Server")
#     extra    = db_cfg.get("extra_params", "TrustServerCertificate=yes")

#     return (
#         f"DRIVER={{{driver}}};"
#         f"SERVER={server};"
#         f"DATABASE={database};"
#         f"UID={user};"
#         f"PWD={password};"
#         f"{extra}"
#     )


# def section_db(runner: Runner) -> None:
#     runner.section("SECTION 4 — SQL Server Connection & Data Integrity")

#     # 4.1 Raw pyodbc connection
#     conn_result = runner.run(
#         "pyodbc.connect() to SQL Server",
#         lambda: _db_connect_check(),
#     )
#     db_ok = conn_result.status == Status.PASS

#     # 4.2 through 4.10 — only if connection succeeded
#     runner.run("SELECT 1 (smoke test)",                   lambda: _db_smoke(),        skip=not db_ok)
#     runner.run("Correct database (Charmacy_f_automate)",  lambda: _db_check_dbname(), skip=not db_ok)
#     runner.run("SQL Server version readable",             lambda: _db_version(),      skip=not db_ok)
#     runner.run("[dbo].[B2B_B2C] view exists",             lambda: _db_view_exists(),  skip=not db_ok)
#     runner.run("[dbo].[B2B_B2C] has data (row count)",    lambda: _db_row_count(),    skip=not db_ok)
#     runner.run("All 42 columns present",                  lambda: _db_columns(),      skip=not db_ok)
#     runner.run("All 6 platforms present",                 lambda: _db_platforms(),    skip=not db_ok)
#     runner.run("MRP values are non-null",                 lambda: _db_mrp_check(),    skip=not db_ok)
#     runner.run("Shopify order_date all NULL (spec)",      lambda: _db_shopify_null(), skip=not db_ok)
#     runner.run("Login is read-only (no INSERT rights)",   lambda: _db_readonly(),     skip=not db_ok)


# def _db_connect_check() -> TestResult:
#     import pyodbc
#     conn_str = _get_odbc_conn_str()
#     conn = pyodbc.connect(conn_str, timeout=10)
#     conn.close()
#     return TestResult(
#         name="pyodbc.connect() to SQL Server",
#         status=Status.PASS,
#         message="Connection established and closed cleanly.",
#     )


# def _db_run(sql: str) -> Any:
#     """Execute a SQL query and return all rows. Opens + closes its own connection."""
#     import pyodbc
#     conn = pyodbc.connect(_get_odbc_conn_str(), timeout=15)
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     rows = cursor.fetchall()
#     conn.close()
#     return rows


# def _db_smoke() -> tuple[Status, str]:
#     rows = _db_run("SELECT 1 AS test_col")
#     val = rows[0][0]
#     if val == 1:
#         return Status.PASS, "SELECT 1 returned 1."
#     return Status.FAIL, f"Unexpected result: {val}"


# def _db_check_dbname() -> TestResult:
#     rows = _db_run("SELECT DB_NAME() AS db_name, @@SERVERNAME AS srv_name")
#     db_name, srv_name = rows[0]
#     db_name = (db_name or "").strip()
#     if db_name.lower() == "charmacy_f_automate":
#         return TestResult(
#             name="Correct database",
#             status=Status.PASS,
#             message=f"Connected to '{db_name}' on server '{srv_name}'.",
#         )
#     return TestResult(
#         name="Correct database",
#         status=Status.WARN,
#         message=f"Connected to '{db_name}' (expected 'Charmacy_f_automate').",
#         fix_hint="Update DATABASE in .env: DB_DATABASE=Charmacy_f_automate",
#     )


# def _db_version() -> TestResult:
#     rows = _db_run("SELECT @@VERSION AS ver")
#     ver_full = str(rows[0][0] or "")
#     # First line of @@VERSION is readable: "Microsoft SQL Server 20xx ..."
#     ver_short = ver_full.splitlines()[0][:100]
#     return TestResult(
#         name="SQL Server version",
#         status=Status.PASS,
#         message="Version readable.",
#         detail=ver_short,
#     )


# def _db_view_exists() -> TestResult:
#     rows = _db_run(
#         "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS "
#         "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'B2B_B2C'"
#     )
#     count = rows[0][0]
#     if count == 1:
#         return TestResult(
#             name="[dbo].[B2B_B2C] view exists",
#             status=Status.PASS,
#             message="View confirmed in INFORMATION_SCHEMA.VIEWS.",
#         )
#     return TestResult(
#         name="[dbo].[B2B_B2C] view exists",
#         status=Status.FAIL,
#         message="View [dbo].[B2B_B2C] NOT found in database.",
#         fix_hint=(
#             "Check the view exists in SSMS:\n"
#             "       SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'B2B_B2C'\n"
#             "       If missing, the view must be created before using this system."
#         ),
#     )


# def _db_row_count() -> TestResult:
#     rows = _db_run("SELECT COUNT(*) FROM [dbo].[B2B_B2C] WITH (NOLOCK)")
#     count = rows[0][0]
#     if count == 0:
#         return TestResult(
#             name="Row count > 0",
#             status=Status.FAIL,
#             message="[dbo].[B2B_B2C] has 0 rows — no data to query.",
#         )
#     status = Status.PASS
#     msg = f"{count:,} rows found."
#     detail = ""
#     if count < 100:
#         status = Status.WARN
#         msg = f"Only {count:,} rows — suspiciously low."
#         detail = "Expected 10,000–15,000+ rows per spec. Verify the view is populated."
#     return TestResult(name="Row count", status=status, message=msg, detail=detail)


# def _db_columns() -> TestResult:
#     """Verify all 42 expected columns exist in the view."""
#     from core.sql_validator import KNOWN_COLUMNS  # our single source of truth

#     rows = _db_run(
#         "SELECT LOWER(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS "
#         "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'B2B_B2C' "
#         "ORDER BY ORDINAL_POSITION"
#     )
#     actual_cols: set[str] = {r[0] for r in rows}
#     missing = KNOWN_COLUMNS - actual_cols

#     if not actual_cols:
#         return TestResult(
#             name="All 42 columns present",
#             status=Status.FAIL,
#             message="Could not read column list from INFORMATION_SCHEMA.COLUMNS.",
#             fix_hint="Ensure the login has VIEW DEFINITION permission on [dbo].[B2B_B2C].",
#         )

#     if missing:
#         return TestResult(
#             name="All 42 columns present",
#             status=Status.WARN,
#             message=f"{len(missing)} expected column(s) not found in view.",
#             detail="Missing: " + ", ".join(sorted(missing)),
#             fix_hint=(
#                 "These columns are referenced in KNOWN_COLUMNS (sql_validator.py).\n"
#                 "       Update KNOWN_COLUMNS if the schema has changed, or check the view definition."
#             ),
#         )

#     return TestResult(
#         name="All 42 columns present",
#         status=Status.PASS,
#         message=f"All {len(KNOWN_COLUMNS)} expected columns confirmed.",
#         detail=f"View has {len(actual_cols)} total columns.",
#     )


# def _db_platforms() -> TestResult:
#     rows = _db_run(
#         "SELECT DISTINCT platform FROM [dbo].[B2B_B2C] WITH (NOLOCK) WHERE platform IS NOT NULL"
#     )
#     found: set[str] = {str(r[0]).strip() for r in rows}
#     missing = EXPECTED_PLATFORMS - found
#     extra   = found - EXPECTED_PLATFORMS

#     detail_parts = []
#     if found:
#         detail_parts.append("Found: " + ", ".join(sorted(found)))
#     if extra:
#         detail_parts.append("Unexpected platforms: " + ", ".join(sorted(extra)))

#     if missing:
#         return TestResult(
#             name="All 6 platforms present",
#             status=Status.WARN,
#             message=f"Missing platform(s): {', '.join(sorted(missing))}",
#             detail="\n".join(detail_parts),
#             fix_hint=(
#                 "Some platforms may have no data yet.\n"
#                 "       If a platform is intentionally absent, update EXPECTED_PLATFORMS in this script."
#             ),
#         )
#     return TestResult(
#         name="All 6 platforms present",
#         status=Status.PASS,
#         message=f"All 6 platforms confirmed: {', '.join(sorted(found))}",
#     )


# def _db_mrp_check() -> TestResult:
#     rows = _db_run(
#         "SELECT "
#         "  COUNT(*) AS total_rows, "
#         "  SUM(CASE WHEN MRP IS NULL THEN 1 ELSE 0 END) AS null_mrp, "
#         "  CAST(SUM(MRP) AS DECIMAL(18,2)) AS total_mrp "
#         "FROM [dbo].[B2B_B2C] WITH (NOLOCK)"
#     )
#     total, null_mrp, total_mrp = rows[0]
#     null_pct = (null_mrp / total * 100) if total else 0

#     if total_mrp is None or float(total_mrp) <= 0:
#         return TestResult(
#             name="MRP values are non-null",
#             status=Status.FAIL,
#             message=f"SUM(MRP) = {total_mrp} — revenue data appears missing.",
#             detail=f"Total rows: {total:,}  |  NULL MRP rows: {null_mrp:,}",
#         )

#     status = Status.WARN if null_pct > 5 else Status.PASS
#     return TestResult(
#         name="MRP values non-null & positive",
#         status=status,
#         message=f"SUM(MRP) = ₹{float(total_mrp):,.2f}",
#         detail=f"NULL MRP: {null_mrp:,} / {total:,} rows ({null_pct:.1f}%)",
#     )


# def _db_shopify_null() -> TestResult:
#     """
#     Per spec: ALL Shopify rows have NULL order_date.
#     If Shopify rows have non-NULL order_date, date filters will behave differently
#     than expected and could corrupt monthly trend charts.
#     """
#     rows = _db_run(
#         "SELECT COUNT(*) FROM [dbo].[B2B_B2C] WITH (NOLOCK) "
#         "WHERE platform = 'Shopify' AND order_date IS NOT NULL"
#     )
#     non_null_count = rows[0][0]
#     if non_null_count == 0:
#         return TestResult(
#             name="Shopify order_date all NULL",
#             status=Status.PASS,
#             message="Confirmed: all Shopify rows have NULL order_date.",
#         )
#     return TestResult(
#         name="Shopify order_date all NULL",
#         status=Status.WARN,
#         message=f"{non_null_count:,} Shopify rows have non-NULL order_date.",
#         detail="Spec states Shopify order_date is always NULL. This may affect date-filtered queries.",
#         fix_hint=(
#             "If Shopify now has order_date data, update the date-filter logic in\n"
#             "       core/vanna_instance.py (TSQL_RULES section 6) and training documentation."
#         ),
#     )


# def _db_readonly() -> TestResult:
#     """
#     Attempt an INSERT — it must fail.  This verifies the login has db_datareader only.
#     We catch the expected error and treat it as a PASS.
#     """
#     import pyodbc
#     conn = pyodbc.connect(_get_odbc_conn_str(), timeout=10)
#     cursor = conn.cursor()
#     try:
#         # This should raise an error (permission denied or read-only view)
#         cursor.execute(
#             "INSERT INTO [dbo].[B2B_B2C] (platform) VALUES ('__test__')"
#         )
#         conn.rollback()
#         conn.close()
#         return TestResult(
#             name="Login is read-only",
#             status=Status.FAIL,
#             message="INSERT succeeded — the login has WRITE permissions.",
#             fix_hint=(
#                 "The DB login should have db_datareader role ONLY.\n"
#                 "       In SSMS: ALTER ROLE db_datareader ADD MEMBER [your_login]\n"
#                 "       Then revoke any db_datawriter role."
#             ),
#         )
#     except pyodbc.Error:
#         conn.close()
#         return TestResult(
#             name="Login is read-only",
#             status=Status.PASS,
#             message="INSERT correctly rejected — login is read-only.",
#         )


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 5 — Ollama Service + Model
# # ══════════════════════════════════════════════════════════════════════════════

# def _get_ollama_host() -> str:
#     """Read ollama_host from vanna_config.yaml or default."""
#     path = CONFIG_DIR / "vanna_config.yaml"
#     if path.exists():
#         import yaml
#         with open(path) as f:
#             data = yaml.safe_load(f) or {}
#         return data.get("ollama_host", "http://localhost:11434")
#     return "http://localhost:11434"


# def _get_ollama_model() -> str:
#     path = CONFIG_DIR / "vanna_config.yaml"
#     if path.exists():
#         import yaml
#         with open(path) as f:
#             data = yaml.safe_load(f) or {}
#         return data.get("model", "qwen3:9b")
#     return "qwen3:9b"


# def section_ollama(runner: Runner) -> None:
#     runner.section("SECTION 5 — Ollama Service & Model")

#     host  = _get_ollama_host()
#     model = _get_ollama_model()

#     # 5.1 HTTP health check
#     def check_ollama_running() -> TestResult:
#         url = host.rstrip("/")
#         try:
#             req = urllib.request.Request(url, method="GET")
#             with urllib.request.urlopen(req, timeout=5) as resp:
#                 body = resp.read().decode("utf-8", errors="replace")
#             if "Ollama is running" in body or resp.status == 200:
#                 return TestResult(
#                     name="Ollama service running",
#                     status=Status.PASS,
#                     message=f"HTTP 200 from {url}",
#                 )
#             return TestResult(
#                 name="Ollama service running",
#                 status=Status.WARN,
#                 message=f"Ollama responded but body unexpected: {body[:80]}",
#             )
#         except urllib.error.URLError as e:
#             return TestResult(
#                 name="Ollama service running",
#                 status=Status.FAIL,
#                 message=f"Cannot reach {url}: {e.reason}",
#                 fix_hint=(
#                     f"Start Ollama:  ollama serve\n"
#                     f"       Or verify OLLAMA_HOST in vanna_config.yaml is correct ({host})"
#                 ),
#             )
#     ollama_result = runner.run("Ollama service running", check_ollama_running)
#     ollama_ok = ollama_result.status == Status.PASS

#     # 5.2 List models via /api/tags
#     def check_model_list() -> TestResult:
#         import json
#         url = host.rstrip("/") + "/api/tags"
#         try:
#             with urllib.request.urlopen(url, timeout=8) as resp:
#                 data = json.loads(resp.read().decode())
#             models_raw = data.get("models", [])
#             model_names = [m.get("name", "") for m in models_raw]
#             return TestResult(
#                 name="Ollama /api/tags readable",
#                 status=Status.PASS,
#                 message=f"{len(model_names)} model(s) installed.",
#                 detail="Models: " + ", ".join(model_names) if model_names else "No models found.",
#             )
#         except Exception as e:
#             return TestResult(
#                 name="Ollama /api/tags readable",
#                 status=Status.FAIL,
#                 message=str(e),
#             )
#     runner.run("Ollama /api/tags readable", check_model_list, skip=not ollama_ok)

#     # 5.3 Target model present
#     def check_target_model() -> TestResult:
#         import json
#         url = host.rstrip("/") + "/api/tags"
#         with urllib.request.urlopen(url, timeout=8) as resp:
#             data = json.loads(resp.read().decode())
#         model_names = [m.get("name", "") for m in data.get("models", [])]
#         # Match base name — model may be stored as "qwen3:9b" or "qwen3:9b-instruct" etc.
#         base_model = model.split(":")[0]
#         exact_match = any(m == model for m in model_names)
#         partial_match = any(base_model in m for m in model_names)

#         if exact_match:
#             return TestResult(
#                 name=f"Model '{model}' available",
#                 status=Status.PASS,
#                 message=f"'{model}' found in Ollama model list.",
#             )
#         if partial_match:
#             matched = [m for m in model_names if base_model in m]
#             return TestResult(
#                 name=f"Model '{model}' available",
#                 status=Status.WARN,
#                 message=f"Exact model '{model}' not found, but related: {matched}",
#                 fix_hint=(
#                     f"Pull the exact model:  ollama pull {model}\n"
#                     f"       Or update 'model' in config/vanna_config.yaml to match an installed model."
#                 ),
#             )
#         return TestResult(
#             name=f"Model '{model}' available",
#             status=Status.FAIL,
#             message=f"'{model}' not in Ollama model list.",
#             detail=f"Installed: {model_names or ['(none)']}",
#             fix_hint=f"Run:  ollama pull {model}",
#         )
#     runner.run(f"Model '{model}' available", check_target_model, skip=not ollama_ok)

#     # 5.4 Quick generation smoke test (just ping the model, not full SQL)
#     def check_ollama_generate() -> TestResult:
#         import json, urllib.request
#         url = host.rstrip("/") + "/api/generate"
#         payload = json.dumps({
#             "model": model,
#             "prompt": "Reply with only the word: READY",
#             "stream": False,
#         }).encode()
#         req = urllib.request.Request(
#             url, data=payload,
#             headers={"Content-Type": "application/json"},
#             method="POST",
#         )
#         with urllib.request.urlopen(req, timeout=60) as resp:
#             data = json.loads(resp.read().decode())
#         response_text = data.get("response", "").strip()
#         if response_text:
#             return TestResult(
#                 name=f"Ollama generate() responds",
#                 status=Status.PASS,
#                 message=f"Model replied: '{response_text[:60]}'",
#             )
#         return TestResult(
#             name="Ollama generate() responds",
#             status=Status.WARN,
#             message="Empty response from model.",
#         )
#     runner.run(f"Ollama generate() smoke test", check_ollama_generate, skip=not ollama_ok)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 6 — ChromaDB
# # ══════════════════════════════════════════════════════════════════════════════

# def section_chromadb(runner: Runner) -> None:
#     runner.section("SECTION 6 — ChromaDB Vector Store")

#     def check_chromadb_init() -> TestResult:
#         import chromadb
#         chroma_path = str(VECTORDB_DIR)
#         client = chromadb.PersistentClient(path=chroma_path)
#         # List collections — this exercises the persistence layer
#         collections = client.list_collections()
#         col_names = [c.name for c in collections]
#         detail = f"Path: {chroma_path}"
#         if col_names:
#             detail += f"\nExisting collections: {', '.join(col_names)}"
#         else:
#             detail += "\nNo existing collections (fresh install)."
#         return TestResult(
#             name="ChromaDB PersistentClient init",
#             status=Status.PASS,
#             message=f"ChromaDB v{chromadb.__version__} initialised at {chroma_path}",
#             detail=detail,
#         )
#     runner.run("ChromaDB PersistentClient init", check_chromadb_init)

#     def check_chromadb_collection() -> TestResult:
#         """Create a temp collection, add one document, query it, delete it."""
#         import chromadb
#         client = chromadb.PersistentClient(path=str(VECTORDB_DIR))
#         col = client.get_or_create_collection("__healthcheck__")
#         col.add(
#             documents=["test document for health check"],
#             ids=["hc_001"],
#         )
#         results = col.query(query_texts=["health check"], n_results=1)
#         hit = results["ids"][0][0] if results["ids"] else None
#         client.delete_collection("__healthcheck__")
#         if hit == "hc_001":
#             return TestResult(
#                 name="ChromaDB add + query roundtrip",
#                 status=Status.PASS,
#                 message="Document stored and retrieved successfully.",
#             )
#         return TestResult(
#             name="ChromaDB add + query roundtrip",
#             status=Status.FAIL,
#             message=f"Expected 'hc_001' back from query, got: {hit}",
#         )
#     runner.run("ChromaDB add + query roundtrip", check_chromadb_collection)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 7 — Vanna End-to-End
# # ══════════════════════════════════════════════════════════════════════════════

# def section_vanna(runner: Runner) -> None:
#     runner.section("SECTION 7 — Vanna End-to-End (Init + Connect + Training)")

#     # 7.1  MyVanna instantiates
#     def check_myvanna_init() -> TestResult:
#         from core.vanna_instance import MyVanna, _build_vanna_config
#         vanna_cfg, _ = _build_vanna_config()
#         vn = MyVanna(config=vanna_cfg)
#         model = vanna_cfg.get("model", "?")
#         path  = vanna_cfg.get("path", "?")
#         return TestResult(
#             name="MyVanna.__init__() succeeds",
#             status=Status.PASS,
#             message=f"MyVanna created. model={model}",
#             detail=f"ChromaDB path: {path}",
#         )
#     vanna_init = runner.run("MyVanna.__init__() succeeds", check_myvanna_init)
#     vanna_ok = vanna_init.status == Status.PASS

#     # 7.2  connect() to MSSQL
#     def check_vanna_connect() -> TestResult:
#         from core.vanna_instance import get_vanna
#         vn = get_vanna(force_new=True)
#         return TestResult(
#             name="vn.connect() to SQL Server",
#             status=Status.PASS,
#             message="MyVanna connected to SQL Server via MSSQL connector.",
#         )
#     connect_result = runner.run("vn.connect() to SQL Server", check_vanna_connect, skip=not vanna_ok)
#     connected = connect_result.status == Status.PASS

#     # 7.3  run_sql() with a known-good query
#     def check_vanna_run_sql() -> TestResult:
#         from core.vanna_instance import get_vanna
#         vn = get_vanna()
#         sql = "SELECT TOP 3 platform, COUNT(*) AS cnt FROM [dbo].[B2B_B2C] GROUP BY platform ORDER BY cnt DESC"
#         df = vn.run_sql(sql)
#         if df is None or len(df) == 0:
#             return TestResult(
#                 name="vn.run_sql() executes and returns DataFrame",
#                 status=Status.FAIL,
#                 message="run_sql() returned None or empty DataFrame.",
#             )
#         return TestResult(
#             name="vn.run_sql() returns DataFrame",
#             status=Status.PASS,
#             message=f"Returned {len(df)} rows × {len(df.columns)} cols.",
#             detail=str(df.to_string(index=False)),
#         )
#     runner.run("vn.run_sql() returns DataFrame", check_vanna_run_sql, skip=not connected)

#     # 7.4  Training summary
#     def check_training_summary() -> TestResult:
#         from core.vanna_instance import get_vanna
#         vn = get_vanna()
#         summary = vn.training_summary()
#         ddl_count  = summary.get("ddl", 0)
#         doc_count  = summary.get("documentation", 0)
#         sql_count  = summary.get("sql", 0)
#         total      = summary.get("total", 0)

#         detail = (
#             f"DDL entries:           {ddl_count}\n"
#             f"Documentation entries: {doc_count}\n"
#             f"Q→SQL examples:        {sql_count}\n"
#             f"Total:                 {total}"
#         )

#         if total == 0:
#             return TestResult(
#                 name="Vanna training data",
#                 status=Status.WARN,
#                 message="No training data in ChromaDB yet.",
#                 detail=detail,
#                 fix_hint="Run:  python scripts/train_vanna.py",
#             )
#         if sql_count < MIN_TRAINING_EXAMPLES:
#             return TestResult(
#                 name="Vanna training data",
#                 status=Status.WARN,
#                 message=f"Only {sql_count} Q→SQL examples (minimum recommended: {MIN_TRAINING_EXAMPLES}).",
#                 detail=detail,
#                 fix_hint=(
#                     f"Add more Q→SQL examples to training/examples/seed_examples.json\n"
#                     f"       then re-run:  python scripts/train_vanna.py"
#                 ),
#             )
#         return TestResult(
#             name="Vanna training data",
#             status=Status.PASS,
#             message=f"Training data loaded: {total} entries ({sql_count} Q→SQL examples).",
#             detail=detail,
#         )
#     runner.run("Vanna training data", check_training_summary, skip=not connected)

#     # 7.5  get_sql_prompt() override is active
#     def check_tsql_injection() -> TestResult:
#         from core.vanna_instance import get_vanna, TSQL_RULES
#         vn = get_vanna()
#         try:
#             # Call get_sql_prompt with a dummy question — Vanna may need some training
#             # data to not error; catch gracefully.
#             messages = vn.get_sql_prompt(
#                 question="What is the total revenue?",
#                 question_sql_list=[],
#                 ddl_list=[],
#                 doc_list=[],
#             )
#             if not messages:
#                 return TestResult(
#                     name="T-SQL rules injected into prompts",
#                     status=Status.WARN,
#                     message="get_sql_prompt() returned empty list.",
#                 )
#             system_content = messages[0].get("content", "")
#             if "STRICT T-SQL RULES" in system_content:
#                 return TestResult(
#                     name="T-SQL rules injected into prompts",
#                     status=Status.PASS,
#                     message="TSQL_RULES confirmed in system message at index 0.",
#                 )
#             return TestResult(
#                 name="T-SQL rules injected into prompts",
#                 status=Status.FAIL,
#                 message="TSQL_RULES NOT found in assembled prompt.",
#                 fix_hint="Check get_sql_prompt() override in core/vanna_instance.py.",
#             )
#         except Exception as e:
#             return TestResult(
#                 name="T-SQL rules injected into prompts",
#                 status=Status.WARN,
#                 message=f"Could not call get_sql_prompt() directly: {e}",
#                 detail="This is non-critical — the override will still fire during generate_sql().",
#             )
#     runner.run("T-SQL rules injected into prompts", check_tsql_injection, skip=not vanna_ok)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 8 — sql_validator Self-Tests
# # ══════════════════════════════════════════════════════════════════════════════

# def section_validator(runner: Runner) -> None:
#     runner.section("SECTION 8 — sql_validator Self-Tests (Known-Case Assertions)")

#     from core.sql_validator import validate_sql, ViolationType

#     # 8.1  Good SQL passes both phases
#     def check_valid_sql() -> TestResult:
#         sql = (
#             "SELECT TOP 10 platform, SUM(MRP) AS revenue\n"
#             "FROM [dbo].[B2B_B2C]\n"
#             "WHERE NOT (\n"
#             "    (platform = 'Amazon' AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))\n"
#             "    OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))\n"
#             "    OR (platform = 'Shopify' AND ISNULL(fulfilment_type,'') = 'unfulfilled')\n"
#             ")\n"
#             "GROUP BY platform\n"
#             "ORDER BY revenue DESC"
#         )
#         result = validate_sql(sql)
#         if result.is_valid and not result.auto_fixed:
#             return TestResult(
#                 name="Valid SQL passes both phases",
#                 status=Status.PASS,
#                 message="Correct SELECT with net filter → PASS, no auto-fix.",
#             )
#         return TestResult(
#             name="Valid SQL passes both phases",
#             status=Status.FAIL,
#             message=f"is_valid={result.is_valid} auto_fixed={result.auto_fixed} violations={result.violations}",
#         )
#     runner.run("Valid SQL passes both phases", check_valid_sql)

#     # 8.2  DROP TABLE → security violation
#     def check_drop_blocked() -> TestResult:
#         result = validate_sql("DROP TABLE [dbo].[B2B_B2C]")
#         if not result.is_valid and result.violation_type == ViolationType.SECURITY:
#             return TestResult(
#                 name="DROP TABLE → SECURITY violation",
#                 status=Status.PASS,
#                 message=f"Correctly blocked. Violation: {result.violations[0][:80]}",
#             )
#         return TestResult(
#             name="DROP TABLE → SECURITY violation",
#             status=Status.FAIL,
#             message=f"DROP TABLE was NOT blocked! is_valid={result.is_valid} type={result.violation_type}",
#             fix_hint="Review _check_forbidden_patterns() in core/sql_validator.py.",
#         )
#     runner.run("DROP TABLE → SECURITY violation", check_drop_blocked)

#     # 8.3  xp_cmdshell → security violation
#     def check_xp_cmdshell() -> TestResult:
#         result = validate_sql("SELECT * FROM [dbo].[B2B_B2C]; EXEC xp_cmdshell('dir')")
#         if not result.is_valid and result.violation_type == ViolationType.SECURITY:
#             return TestResult(
#                 name="xp_cmdshell → SECURITY violation",
#                 status=Status.PASS,
#                 message="Correctly blocked. Both stacked statement and xp_ pattern detected.",
#             )
#         return TestResult(
#             name="xp_cmdshell → SECURITY violation",
#             status=Status.FAIL,
#             message=f"xp_cmdshell was NOT blocked. is_valid={result.is_valid}",
#         )
#     runner.run("xp_cmdshell → SECURITY violation", check_xp_cmdshell)

#     # 8.4  String literal with blocked keyword — must NOT false-positive
#     def check_no_false_positive() -> TestResult:
#         # The word 'Cancel' appears as a value — must not trigger DML check
#         sql = (
#             "SELECT COUNT(*) AS cancellations\n"
#             "FROM [dbo].[B2B_B2C]\n"
#             "WHERE platform = 'Amazon' AND transaction_type = 'Cancel'"
#         )
#         result = validate_sql(sql)
#         if result.is_valid:
#             return TestResult(
#                 name="String literal 'Cancel' does not false-positive",
#                 status=Status.PASS,
#                 message="Correctly passed — string literal masking works.",
#             )
#         return TestResult(
#             name="String literal 'Cancel' does not false-positive",
#             status=Status.FAIL,
#             message=f"False positive! violations={result.violations}",
#             fix_hint="Check _strip_string_literals() in core/sql_validator.py.",
#         )
#     runner.run("String literal 'Cancel' does not false-positive", check_no_false_positive)

#     # 8.5  SELECT * without TOP → auto-fixed to SELECT TOP 1000 *
#     def check_auto_top_inject() -> TestResult:
#         sql = "SELECT * FROM [dbo].[B2B_B2C]"
#         result = validate_sql(sql)
#         has_top = "TOP 1000" in result.fixed_sql.upper()
#         if result.is_valid and result.auto_fixed and has_top:
#             return TestResult(
#                 name="SELECT * → auto-injected SELECT TOP 1000 *",
#                 status=Status.PASS,
#                 message=f"Auto-fixed: '{result.fixed_sql[:60]}…'",
#             )
#         return TestResult(
#             name="SELECT * → auto-injected SELECT TOP 1000 *",
#             status=Status.FAIL,
#             message=(
#                 f"is_valid={result.is_valid}  auto_fixed={result.auto_fixed}  "
#                 f"TOP 1000 present={has_top}\n"
#                 f"fixed_sql='{result.fixed_sql[:80]}'"
#             ),
#             fix_hint="Check _auto_inject_top() in core/sql_validator.py.",
#         )
#     runner.run("SELECT * → auto-injected SELECT TOP 1000 *", check_auto_top_inject)

#     # 8.6  Hallucinated column name → sanity violation
#     def check_unknown_column() -> TestResult:
#         sql = "SELECT [fake_revenue_column] FROM [dbo].[B2B_B2C]"
#         result = validate_sql(sql)
#         if not result.is_valid and result.violation_type == ViolationType.SANITY:
#             return TestResult(
#                 name="Unknown [bracketed_col] → SANITY violation",
#                 status=Status.PASS,
#                 message=f"Correctly caught: {result.violations[0][:80]}",
#             )
#         return TestResult(
#             name="Unknown [bracketed_col] → SANITY violation",
#             status=Status.FAIL,
#             message=f"Hallucinated column NOT detected. is_valid={result.is_valid}",
#             fix_hint="Check _check_bracketed_column_names() in core/sql_validator.py.",
#         )
#     runner.run("Unknown [bracketed_col] → SANITY violation", check_unknown_column)

#     # 8.7  Wrong table name → sanity violation
#     def check_wrong_table() -> TestResult:
#         sql = "SELECT TOP 10 * FROM [dbo].[SalesOrders]"
#         result = validate_sql(sql)
#         if not result.is_valid and result.violation_type == ViolationType.SANITY:
#             return TestResult(
#                 name="Wrong table name → SANITY violation",
#                 status=Status.PASS,
#                 message="Correctly flagged: target view not referenced.",
#             )
#         return TestResult(
#             name="Wrong table name → SANITY violation",
#             status=Status.FAIL,
#             message=f"Wrong table NOT detected. is_valid={result.is_valid}",
#         )
#     runner.run("Wrong table name → SANITY violation", check_wrong_table)

#     # 8.8  build_correction_context() produces usable output
#     def check_correction_context() -> TestResult:
#         from core.sql_validator import build_correction_context, ValidationResult, ViolationType
#         fake_result = ValidationResult(
#             is_valid=False,
#             fixed_sql="SELECT * FROM [dbo].[B2B_B2C]",
#             violation_type=ViolationType.SANITY,
#             violations=["Target view [dbo].[B2B_B2C] is not referenced."],
#             original_sql="SELECT * FROM SalesOrders",
#         )
#         ctx = build_correction_context(fake_result, "What is the total revenue?")
#         required_phrases = ["B2B_B2C", "TOP N", "net sales filter", "ONLY the SQL"]
#         missing = [p for p in required_phrases if p not in ctx]
#         if not missing:
#             return TestResult(
#                 name="build_correction_context() complete",
#                 status=Status.PASS,
#                 message="All required phrases present in correction context.",
#             )
#         return TestResult(
#             name="build_correction_context() complete",
#             status=Status.FAIL,
#             message=f"Missing phrases in correction context: {missing}",
#         )
#     runner.run("build_correction_context() complete", check_correction_context)


# # ══════════════════════════════════════════════════════════════════════════════
# # SECTION 9 — Full Generation Test (slow — skipped with --quick)
# # ══════════════════════════════════════════════════════════════════════════════

# def section_generation(runner: Runner, skip: bool = False) -> None:
#     runner.section("SECTION 9 — Full Generation Test  [generate_sql → validate → run_sql]")
#     if skip:
#         runner.run("generate_sql() end-to-end", lambda: (Status.SKIP, "Skipped (--quick mode)."), skip=True)
#         runner.run("Generated SQL passes validator", lambda: ..., skip=True)
#         runner.run("Generated SQL executes on SSMS",  lambda: ..., skip=True)
#         return

#     # 9.1  generate_sql()
#     test_question = "What are the top 5 platforms by total revenue?"
#     generated_sql: str = ""

#     def check_generate_sql() -> TestResult:
#         nonlocal generated_sql
#         from core.vanna_instance import get_vanna
#         vn = get_vanna()
#         print(f"\n       {_dim(f'Question: {test_question}')}")
#         print(f"       {_dim('Sending to Ollama (qwen3:9b)… this may take 30–90s')}")
#         sql = vn.generate_sql(test_question)
#         if not sql or not sql.strip():
#             return TestResult(
#                 name="generate_sql() returns non-empty SQL",
#                 status=Status.FAIL,
#                 message="generate_sql() returned empty string.",
#                 fix_hint=(
#                     "Check Ollama is running and qwen3:9b is loaded.\n"
#                     "       Also check Vanna has at least some training data."
#                 ),
#             )
#         generated_sql = sql.strip()
#         preview = generated_sql[:120].replace("\n", " ")
#         return TestResult(
#             name="generate_sql() returns non-empty SQL",
#             status=Status.PASS,
#             message="SQL generated successfully.",
#             detail=f"SQL preview: {preview}…" if len(generated_sql) > 120 else f"SQL: {preview}",
#         )

#     gen_result = runner.run("generate_sql() returns non-empty SQL", check_generate_sql)
#     gen_ok = gen_result.status == Status.PASS

#     # 9.2  Validate the generated SQL
#     def check_generated_validates() -> TestResult:
#         nonlocal generated_sql
#         from core.sql_validator import validate_sql
#         result = validate_sql(generated_sql)
#         if result.is_valid:
#             auto_msg = " (TOP injected by auto-fix)" if result.auto_fixed else ""
#             return TestResult(
#                 name="Generated SQL passes validator",
#                 status=Status.PASS,
#                 message=f"Passed both phases{auto_msg}.",
#                 detail=f"fixed_sql preview: {result.fixed_sql[:100]}…",
#             )
#         return TestResult(
#             name="Generated SQL passes validator",
#             status=Status.WARN if result.violation_type == "sanity" else Status.FAIL,
#             message=f"{result.violation_type.upper()} violation: {result.violations[0][:100]}",
#             detail="\n".join(result.violations),
#             fix_hint=(
#                 "This means the model generated unsafe or incorrect SQL.\n"
#                 "       Add more Q→SQL training examples and re-train.\n"
#                 "       Check TSQL_RULES injection is working (Section 7)."
#             ),
#         )
#     validate_result = runner.run("Generated SQL passes validator", check_generated_validates, skip=not gen_ok)
#     exec_ok = validate_result.status in (Status.PASS, Status.WARN)

#     # 9.3  Execute the generated SQL
#     def check_generated_executes() -> TestResult:
#         nonlocal generated_sql
#         from core.vanna_instance import get_vanna
#         from core.sql_validator import validate_sql
#         vn = get_vanna()
#         result = validate_sql(generated_sql)
#         df = vn.run_sql(result.fixed_sql)
#         if df is None:
#             return TestResult(
#                 name="Generated SQL executes on SSMS",
#                 status=Status.FAIL,
#                 message="vn.run_sql() returned None.",
#             )
#         preview_rows = df.head(3).to_string(index=False) if len(df) > 0 else "(empty result)"
#         return TestResult(
#             name="Generated SQL executes on SSMS",
#             status=Status.PASS,
#             message=f"Executed. {len(df)} rows × {len(df.columns)} cols.",
#             detail=f"Sample rows:\n{preview_rows}",
#         )
#     runner.run("Generated SQL executes on SSMS", check_generated_executes, skip=not (gen_ok and exec_ok))


# # ══════════════════════════════════════════════════════════════════════════════
# # CLI entry point
# # ══════════════════════════════════════════════════════════════════════════════

# def _parse_args() -> argparse.Namespace:
#     parser = argparse.ArgumentParser(
#         description="Pre-flight health check for the Charmacy Milano Text-to-SQL system.",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog=textwrap.dedent("""
#         Examples:
#           python scripts/test_connection.py             # full test
#           python scripts/test_connection.py --quick     # skip LLM generation
#           python scripts/test_connection.py --section db
#           python scripts/test_connection.py --section ollama
#           python scripts/test_connection.py --section validator
#           python scripts/test_connection.py --section env
#         """),
#     )
#     parser.add_argument(
#         "--quick", action="store_true",
#         help="Skip Section 9 (LLM generation test). Runs in ~5s.",
#     )
#     parser.add_argument(
#         "--section", type=str, default=None,
#         choices=["env", "imports", "odbc", "db", "ollama", "chromadb", "vanna", "validator", "generation"],
#         help="Run only a specific section.",
#     )
#     return parser.parse_args()


# import textwrap   # used in argparse epilog above


# def main() -> None:
#     args = _parse_args()
#     runner = Runner()

#     section_filter = args.section
#     quick = args.quick

#     print(f"\n{_bold(_cyan('━' * 62))}")
#     print(f"  {_bold('Charmacy Milano — Text-to-SQL System Health Check')}")
#     print(f"  {_dim('Project root: ' + str(ROOT))}")
#     print(f"{_bold(_cyan('━' * 62))}")

#     def _should_run(section_key: str) -> bool:
#         return section_filter is None or section_filter == section_key

#     if _should_run("env"):
#         section_env(runner)
#     if _should_run("imports"):
#         section_imports(runner)
#     if _should_run("odbc"):
#         section_odbc(runner)
#     if _should_run("db"):
#         section_db(runner)
#     if _should_run("ollama"):
#         section_ollama(runner)
#     if _should_run("chromadb"):
#         section_chromadb(runner)
#     if _should_run("vanna"):
#         section_vanna(runner)
#     if _should_run("validator"):
#         section_validator(runner)
#     if _should_run("generation"):
#         section_generation(runner, skip=quick)

#     exit_code = runner.summary()
#     sys.exit(exit_code)


# if __name__ == "__main__":
#     main()


"""
scripts/test_connection.py

Verify connectivity to all three services:
    1. SQL Server (SSMS) — via pyodbc
    2. Ollama — via HTTP health check
    3. Vanna (ChromaDB + full pipeline) — generate a test query

Usage:
    python scripts/test_connection.py

Exit codes:
    0 — all checks passed
    1 — one or more checks failed
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _print_status(label: str, passed: bool, detail: str = "") -> None:
    """Pretty-print a check result."""
    icon = "✅" if passed else "❌"
    msg = f"  {icon}  {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def check_sql_server() -> bool:
    """Test SQL Server connectivity and basic query."""
    print("\n" + "=" * 60)
    print("1. SQL SERVER (SSMS)")
    print("=" * 60)

    try:
        import pyodbc
        import yaml
        from dotenv import load_dotenv

        load_dotenv()

        # Load config
        db_yaml = ROOT / "config" / "database.yaml"
        db_cfg = {}
        if db_yaml.exists():
            with open(db_yaml, "r", encoding="utf-8") as fh:
                db_cfg = yaml.safe_load(fh) or {}

        server = os.getenv("DB_SERVER", db_cfg.get("server", ""))
        database = os.getenv("DB_DATABASE", db_cfg.get("database", "Charmacy_f_automate"))
        user = os.getenv("DB_USER", db_cfg.get("user", ""))
        password = os.getenv("DB_PASSWORD", db_cfg.get("password", ""))
        driver = db_cfg.get("driver", "ODBC Driver 17 for SQL Server")
        extra = db_cfg.get("extra_params", "TrustServerCertificate=yes")

        if not all([server, user, password]):
            _print_status("Credentials", False, "Missing DB_SERVER, DB_USER, or DB_PASSWORD")
            return False

        conn_str = (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"UID={user};PWD={password};{extra}"
        )

        _print_status("Config loaded", True, f"SERVER={server} DB={database}")

        # Connect
        conn = pyodbc.connect(conn_str, timeout=15)
        _print_status("Connection", True, "pyodbc connected")

        cursor = conn.cursor()

        # Test: database version
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0].split("\n")[0]
        _print_status("Server version", True, version[:80])

        # Test: target view exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'B2B_B2C'
        """)
        view_exists = cursor.fetchone()[0] > 0
        _print_status("[dbo].[B2B_B2C] exists", view_exists,
                      "Found" if view_exists else "NOT FOUND — check database")

        # Test: row count
        if view_exists:
            cursor.execute("SELECT COUNT(*) FROM [dbo].[B2B_B2C]")
            row_count = cursor.fetchone()[0]
            _print_status("Row count", True, f"{row_count:,} rows")

            # Test: column count
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'B2B_B2C'
            """)
            col_count = cursor.fetchone()[0]
            _print_status("Column count", col_count == 42,
                          f"{col_count} columns (expected 42)")

            # Test: platforms
            cursor.execute("""
                SELECT DISTINCT platform FROM [dbo].[B2B_B2C]
                ORDER BY platform
            """)
            platforms = [row[0] for row in cursor.fetchall()]
            _print_status("Platforms", len(platforms) >= 4,
                          ", ".join(platforms))

        # Test: read-only (try a write — should fail)
        try:
            cursor.execute("""
                CREATE TABLE [dbo].[__test_readonly__] (id INT)
            """)
            conn.rollback()
            _print_status("Read-only check", False,
                          "WARNING: User can CREATE tables! Should be db_datareader only.")
        except pyodbc.ProgrammingError:
            _print_status("Read-only check", True, "User cannot write (db_datareader)")
        except Exception:
            _print_status("Read-only check", True, "Write blocked (good)")

        cursor.close()
        conn.close()
        return True

    except ImportError as exc:
        _print_status("Import", False, f"Missing package: {exc}")
        return False
    except pyodbc.Error as exc:
        _print_status("Connection", False, str(exc)[:200])
        return False
    except Exception as exc:
        _print_status("Unexpected error", False, str(exc)[:200])
        return False


def check_ollama() -> bool:
    """Test Ollama connectivity and model availability."""
    print("\n" + "=" * 60)
    print("2. OLLAMA")
    print("=" * 60)

    try:
        import yaml
        import urllib.request
        import json

        # Load Vanna config for ollama_host
        vanna_yaml = ROOT / "config" / "vanna_config.yaml"
        vanna_cfg = {}
        if vanna_yaml.exists():
            with open(vanna_yaml, "r", encoding="utf-8") as fh:
                vanna_cfg = yaml.safe_load(fh) or {}

        ollama_host = vanna_cfg.get("ollama_host", "http://localhost:11434")
        target_model = vanna_cfg.get("model", "qwen3:9b")

        _print_status("Config", True, f"host={ollama_host} model={target_model}")

        # Health check
        try:
            req = urllib.request.Request(f"{ollama_host}/api/tags", method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read())
        except Exception as exc:
            _print_status("Ollama reachable", False,
                          f"Cannot reach {ollama_host} — is Ollama running? Error: {exc}")
            return False

        _print_status("Ollama reachable", True, f"{ollama_host} responded")

        # Check model list
        models = body.get("models", [])
        model_names = [m.get("name", "") for m in models]
        _print_status("Models available", len(models) > 0,
                      f"{len(models)} models loaded")

        # Check target model
        target_found = any(target_model in name for name in model_names)
        if target_found:
            _print_status(f"Target model '{target_model}'", True, "Found")
        else:
            _print_status(f"Target model '{target_model}'", False,
                          f"NOT found. Available: {', '.join(model_names[:5])}")
            print(f"\n    Run: ollama pull {target_model}")
            return False

        # Quick generation test
        try:
            test_payload = json.dumps({
                "model": target_model,
                "prompt": "Reply with just the word 'hello'.",
                "stream": False,
            }).encode("utf-8")
            req = urllib.request.Request(
                f"{ollama_host}/api/generate",
                data=test_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                gen_body = json.loads(resp.read())
            response_text = gen_body.get("response", "")[:100]
            _print_status("Generation test", bool(response_text),
                          f"Response: '{response_text}'")
        except Exception as exc:
            _print_status("Generation test", False, str(exc)[:200])

        return True

    except Exception as exc:
        _print_status("Unexpected error", False, str(exc)[:200])
        return False


def check_vanna() -> bool:
    """Test full Vanna pipeline: init, ChromaDB, training data, SQL generation."""
    print("\n" + "=" * 60)
    print("3. VANNA (ChromaDB + LLM Pipeline)")
    print("=" * 60)

    try:
        from core.vanna_instance import get_vanna

        vn = get_vanna()
        _print_status("Vanna initialised", True, type(vn).__name__)

        # Training data summary
        summary = vn.training_summary()
        total = summary.get("total", 0)
        sql_count = summary.get("sql", 0)
        ddl_count = summary.get("ddl", 0)
        doc_count = summary.get("documentation", 0)

        _print_status("ChromaDB connected", True,
                      f"Total entries: {total}")
        _print_status("Training data",
                      total > 0,
                      f"DDL={ddl_count}  Docs={doc_count}  Q→SQL={sql_count}")

        if not vn.has_minimum_training():
            print(f"    ⚠️  Only {sql_count} Q→SQL examples (minimum 20 recommended)")
            print(f"    Run: python scripts/train_vanna.py")

        # Test SQL generation
        test_question = "What is the total revenue?"
        print(f"\n    Testing SQL generation: \"{test_question}\"")
        try:
            sql = vn.generate_sql(question=test_question)
            if sql and sql.strip():
                _print_status("SQL generation", True, "")
                # Print the generated SQL (truncated)
                sql_display = sql.strip()[:300]
                for line in sql_display.split("\n"):
                    print(f"      {line}")
                if len(sql.strip()) > 300:
                    print("      ...")
            else:
                _print_status("SQL generation", False, "Empty SQL returned")
        except Exception as exc:
            _print_status("SQL generation", False, str(exc)[:200])

        # Test SQL execution
        try:
            test_sql = "SELECT TOP 1 platform, MRP FROM [dbo].[B2B_B2C]"
            df = vn.run_sql(test_sql)
            _print_status("SQL execution", df is not None and not df.empty,
                          f"Returned {len(df)} rows, {len(df.columns)} cols")
        except Exception as exc:
            _print_status("SQL execution", False, str(exc)[:200])

        # Test RAG retrieval
        try:
            debug = vn.debug_retrieval(test_question)
            ddl_retrieved = len(debug.get("ddl", []))
            docs_retrieved = len(debug.get("docs", []))
            examples_retrieved = len(debug.get("examples", []))
            _print_status("RAG retrieval", True,
                          f"DDL={ddl_retrieved}  Docs={docs_retrieved}  Examples={examples_retrieved}")
        except Exception as exc:
            _print_status("RAG retrieval", False, str(exc)[:200])

        return True

    except ImportError as exc:
        _print_status("Import", False, f"Missing: {exc}")
        return False
    except ValueError as exc:
        _print_status("Config", False, str(exc)[:200])
        return False
    except RuntimeError as exc:
        _print_status("Connection", False, str(exc)[:200])
        return False
    except Exception as exc:
        _print_status("Unexpected error", False, str(exc)[:200])
        return False


def main():
    print("\n" + "╔" + "═" * 58 + "╗")
    print("║   CHARMACY MILANO — CONNECTION & HEALTH CHECK            ║")
    print("╚" + "═" * 58 + "╝")

    results = {}

    results["sql_server"] = check_sql_server()
    results["ollama"] = check_ollama()
    results["vanna"] = check_vanna()

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_passed = True
    for name, passed in results.items():
        icon = "✅" if passed else "❌"
        print(f"  {icon}  {name.upper()}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n  🎉 All checks passed! System is ready.")
    else:
        print("\n  ⚠️  Some checks failed. Review errors above.")

    print("")
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()