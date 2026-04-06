

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