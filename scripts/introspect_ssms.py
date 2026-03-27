"""
scripts/introspect_ssms.py

Auto-extract DDL from SQL Server (SSMS) and optionally train Vanna.

What it does:
    1. Connect to SQL Server using credentials from .env / config/database.yaml
    2. Query INFORMATION_SCHEMA for all tables/views in dbo schema
    3. For each table/view:
       - Extract column names, data types, nullability
       - Extract primary keys, foreign keys, indexes
       - Fetch TOP 5 sample rows
       - Fetch row count
    4. Build rich DDL strings combining schema + sample data
    5. Optionally call vn.train(ddl=...) for each

Usage:
    python scripts/introspect_ssms.py              # Print DDL to stdout
    python scripts/introspect_ssms.py --train      # Print + train Vanna
    python scripts/introspect_ssms.py --output ddl # Save to training/ddl/
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from textwrap import dedent

# ── Project root on sys.path ─────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyodbc
import yaml
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
CONFIG_DIR = ROOT / "config"
DDL_OUTPUT_DIR = ROOT / "training" / "ddl"


def _load_db_config() -> dict:
    """Load database config from YAML + env vars."""
    db_yaml_path = CONFIG_DIR / "database.yaml"
    db_cfg = {}
    if db_yaml_path.exists():
        with open(db_yaml_path, "r", encoding="utf-8") as fh:
            db_cfg = yaml.safe_load(fh) or {}

    server = os.getenv("DB_SERVER", db_cfg.get("server", ""))
    database = os.getenv("DB_DATABASE", db_cfg.get("database", "Charmacy_f_automate"))
    user = os.getenv("DB_USER", db_cfg.get("user", ""))
    password = os.getenv("DB_PASSWORD", db_cfg.get("password", ""))
    driver = db_cfg.get("driver", "ODBC Driver 17 for SQL Server")
    extra = db_cfg.get("extra_params", "TrustServerCertificate=yes")

    missing = [n for n, v in [("DB_SERVER", server), ("DB_USER", user), ("DB_PASSWORD", password)] if not v]
    if missing:
        raise ValueError(f"Missing credentials: {', '.join(missing)}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"{extra}"
    )
    return {"conn_str": conn_str, "database": database}


def _connect(conn_str: str) -> pyodbc.Connection:
    """Establish pyodbc connection."""
    logger.info("Connecting to SQL Server...")
    conn = pyodbc.connect(conn_str, timeout=30)
    logger.info("Connected successfully.")
    return conn


# ── Schema extraction queries ────────────────────────────────────────────────

SQL_TABLES = """
SELECT TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_TYPE, TABLE_NAME
"""

SQL_COLUMNS = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"""

SQL_PRIMARY_KEYS = """
SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
WHERE tc.TABLE_SCHEMA = 'dbo'
    AND tc.TABLE_NAME = ?
    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
ORDER BY kcu.ORDINAL_POSITION
"""

SQL_FOREIGN_KEYS = """
SELECT
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
    OBJECT_NAME(fkc.referenced_object_id) AS referenced_table,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
WHERE OBJECT_NAME(fk.parent_object_id) = ?
    AND SCHEMA_NAME(fk.schema_id) = 'dbo'
"""

SQL_INDEXES = """
SELECT
    i.name AS index_name,
    i.type_desc AS index_type,
    i.is_unique,
    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE OBJECT_NAME(i.object_id) = ?
    AND SCHEMA_NAME(OBJECTPROPERTY(i.object_id, 'SchemaId')) = 'dbo'
    AND i.name IS NOT NULL
GROUP BY i.name, i.type_desc, i.is_unique
"""

SQL_ROW_COUNT = "SELECT COUNT(*) AS row_count FROM [dbo].[{table}]"

SQL_SAMPLE_ROWS = "SELECT TOP 5 * FROM [dbo].[{table}]"


# ── DDL builder ──────────────────────────────────────────────────────────────

def _build_column_def(row) -> str:
    """Build a single column definition line."""
    col_name = row.COLUMN_NAME
    data_type = row.DATA_TYPE.upper()

    # Add length/precision
    if row.CHARACTER_MAXIMUM_LENGTH:
        if row.CHARACTER_MAXIMUM_LENGTH == -1:
            data_type += "(MAX)"
        else:
            data_type += f"({row.CHARACTER_MAXIMUM_LENGTH})"
    elif row.NUMERIC_PRECISION and data_type in ("DECIMAL", "NUMERIC"):
        scale = row.NUMERIC_SCALE or 0
        data_type += f"({row.NUMERIC_PRECISION},{scale})"

    nullable = "NULL" if row.IS_NULLABLE == "YES" else "NOT NULL"
    default = f" DEFAULT {row.COLUMN_DEFAULT}" if row.COLUMN_DEFAULT else ""

    return f"    [{col_name}] {data_type} {nullable}{default}"


def introspect_table(
    cursor: pyodbc.Cursor,
    table_name: str,
    table_type: str,
) -> str:
    """
    Build a rich DDL string for a single table or view.

    Includes:
        - CREATE TABLE/VIEW statement with all columns
        - Primary keys
        - Foreign keys
        - Indexes
        - Row count
        - TOP 5 sample rows as comments
    """
    lines = []
    obj_keyword = "VIEW" if "VIEW" in table_type.upper() else "TABLE"

    # ── Column definitions ───────────────────────────────────────────────
    cursor.execute(SQL_COLUMNS, (table_name,))
    columns = cursor.fetchall()
    if not columns:
        logger.warning("No columns found for %s — skipping.", table_name)
        return ""

    col_defs = [_build_column_def(row) for row in columns]

    lines.append(f"-- {obj_keyword}: [dbo].[{table_name}]")
    lines.append(f"CREATE {obj_keyword} [dbo].[{table_name}] (")
    lines.append(",\n".join(col_defs))

    # ── Primary keys ─────────────────────────────────────────────────────
    if obj_keyword == "TABLE":
        cursor.execute(SQL_PRIMARY_KEYS, (table_name,))
        pk_rows = cursor.fetchall()
        if pk_rows:
            pk_cols = ", ".join(f"[{r.COLUMN_NAME}]" for r in pk_rows)
            lines.append(f"    ,CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_cols})")

    lines.append(");")
    lines.append("")

    # ── Foreign keys ─────────────────────────────────────────────────────
    if obj_keyword == "TABLE":
        try:
            cursor.execute(SQL_FOREIGN_KEYS, (table_name,))
            fk_rows = cursor.fetchall()
            for fk in fk_rows:
                lines.append(
                    f"-- FK: [{table_name}].[{fk.column_name}] "
                    f"→ [{fk.referenced_table}].[{fk.referenced_column}]"
                )
        except Exception as exc:
            logger.debug("FK query failed for %s: %s", table_name, exc)

    # ── Indexes ──────────────────────────────────────────────────────────
    try:
        cursor.execute(SQL_INDEXES, (table_name,))
        idx_rows = cursor.fetchall()
        for idx in idx_rows:
            unique = "UNIQUE " if idx.is_unique else ""
            lines.append(
                f"-- INDEX: {unique}{idx.index_type} [{idx.index_name}] "
                f"ON ({idx.columns})"
            )
    except Exception as exc:
        logger.debug("Index query failed for %s: %s", table_name, exc)

    # ── Row count ────────────────────────────────────────────────────────
    try:
        cursor.execute(SQL_ROW_COUNT.format(table=table_name))
        count_row = cursor.fetchone()
        row_count = count_row.row_count if count_row else "?"
        lines.append(f"-- Row count: {row_count:,}" if isinstance(row_count, int) else f"-- Row count: {row_count}")
    except Exception as exc:
        logger.debug("Row count failed for %s: %s", table_name, exc)
        lines.append("-- Row count: (query failed)")

    # ── Sample rows ──────────────────────────────────────────────────────
    try:
        cursor.execute(SQL_SAMPLE_ROWS.format(table=table_name))
        sample_rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        if sample_rows:
            lines.append(f"-- Sample data (TOP 5):")
            lines.append(f"-- Columns: {', '.join(col_names)}")
            for i, row in enumerate(sample_rows, 1):
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, str):
                        truncated = val[:60] + "..." if len(val) > 60 else val
                        values.append(f"'{truncated}'")
                    else:
                        values.append(str(val))
                lines.append(f"-- Row {i}: {', '.join(values)}")
    except Exception as exc:
        logger.debug("Sample query failed for %s: %s", table_name, exc)
        lines.append("-- Sample data: (query failed)")

    lines.append("")
    return "\n".join(lines)


def introspect_all(
    conn_str: str,
    train: bool = False,
    output_dir: Path | None = None,
) -> list[dict]:
    """
    Introspect all tables and views in the dbo schema.

    Args:
        conn_str: ODBC connection string.
        train: If True, call vn.train(ddl=...) for each object.
        output_dir: If set, save each DDL to a .sql file.

    Returns:
        List of {"name": str, "type": str, "ddl": str} dicts.
    """
    conn = _connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(SQL_TABLES)
    tables = cursor.fetchall()
    logger.info("Found %d objects in dbo schema.", len(tables))

    results = []
    vn = None

    if train:
        from core.vanna_instance import get_vanna
        vn = get_vanna()

    for tbl in tables:
        table_name = tbl.TABLE_NAME
        table_type = tbl.TABLE_TYPE
        logger.info("Introspecting [dbo].[%s] (%s)...", table_name, table_type)

        ddl = introspect_table(cursor, table_name, table_type)
        if not ddl:
            continue

        results.append({
            "name": table_name,
            "type": table_type,
            "ddl": ddl,
        })

        # Print to stdout
        print("=" * 80)
        print(ddl)

        # Train Vanna
        if vn and train:
            try:
                vn.train(ddl=ddl)
                logger.info("✅ Trained DDL for [dbo].[%s]", table_name)
            except Exception as exc:
                logger.error("❌ Training failed for %s: %s", table_name, exc)

        # Save to file
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            safe_name = table_name.replace(" ", "_")
            file_path = output_dir / f"{safe_name}.sql"
            with open(file_path, "w", encoding="utf-8") as fh:
                fh.write(ddl)
            logger.info("📄 Saved DDL to %s", file_path)

    cursor.close()
    conn.close()
    logger.info("Introspection complete. %d objects processed.", len(results))
    return results


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Auto-extract DDL from SQL Server and optionally train Vanna."
    )
    parser.add_argument(
        "--train",
        action="store_true",
        help="Train Vanna with extracted DDL (calls vn.train(ddl=...)).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory for .sql files (default: training/ddl/).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output) if args.output else DDL_OUTPUT_DIR

    db_config = _load_db_config()
    introspect_all(
        conn_str=db_config["conn_str"],
        train=args.train,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()