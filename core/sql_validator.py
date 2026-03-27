"""
core/sql_validator.py

Two-phase SQL validation for Vanna-generated T-SQL.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 1 — SECURITY  (hard reject, never retry)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Must start with SELECT or WITH (CTE)
  • Blocks all DML / DDL / system commands
  • Blocks stacked statements (semicolon attack)
  • Blocks linked-server / four-part-name access
  • Failures → logged to logs/security.log → user sees rejection message

PHASE 2 — SANITY  (soft reject → error_recovery.py retries)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Target view [dbo].[B2B_B2C] is referenced
  • No unknown bracketed column names
  • SELECT * without TOP → auto-fixed to SELECT TOP 1000 *
  • Unbounded SELECT without any TOP / WHERE / aggregation → warn
  • Failures that cannot be auto-fixed → returned for retry with context

PUBLIC API:
    from core.sql_validator import validate_sql, ValidationResult

    result = validate_sql(raw_sql)
    if not result.is_valid:
        if result.violation_type == "security":
            show_error_to_user(result.violations)       # hard stop
        else:
            send_to_error_recovery(result.violations)   # soft retry
    else:
        df = vn.run_sql(result.fixed_sql)               # use fixed_sql always
"""

from __future__ import annotations

import logging
import logging.handlers
import re
import textwrap
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

ROOT         = Path(__file__).resolve().parent.parent
LOGS_DIR     = ROOT / "logs"
SECURITY_LOG = LOGS_DIR / "security.log"

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ────────────────────────────────────────────────────────────────────
# Two loggers:
#   "sql_validator"          — normal debug / info to the root log stream
#   "sql_validator.security" — security violations only → logs/security.log

_root_logger = logging.getLogger("sql_validator")

_sec_logger = logging.getLogger("sql_validator.security")
_sec_logger.setLevel(logging.WARNING)
_sec_logger.propagate = False   # don't double-log to root

if not _sec_logger.handlers:
    _fh = logging.FileHandler(SECURITY_LOG, encoding="utf-8")
    _fh.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s  SECURITY  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    _sec_logger.addHandler(_fh)


# ── Domain knowledge ───────────────────────────────────────────────────────────
#
# All 42 columns of [dbo].[B2B_B2C].  Used to catch hallucinated column names.
# Keep lower-case — comparisons are case-insensitive.
#
KNOWN_COLUMNS: frozenset[str] = frozenset({
    "order_id", "fulfilment_type", "ordertype", "salestype",
    "transaction_type", "order_date", "month_year", "quantity",
    "product_description", "product_code", "invoice_number",
    "invoice_date", "bill_from_city", "bill_from_state",
    "bill_from_country", "bill_from_postal_code", "ship_to_city",
    "ship_to_state", "ship_to_postal_code", "payment_method",
    "event_sub_type", "ean", "article_type", "sku_code",
    "product_name", "mrp", "warehouse_id", "platform",
    "seller_code", "display_name", "company_name", "seller_type",
    "brand", "sku_name", "category_l1", "category_l2",
    "category_l3", "display_price", "selling_price", "total_qty",
    "total_orders", "total_customers",
})

# Allowed view / object names (so [b2b_b2c] bracket-check doesn't false-positive)
KNOWN_OBJECTS: frozenset[str] = frozenset({
    "b2b_b2c", "dbo", "charmacy_f_automate",
})

# SQL built-in functions and common aliases that are NOT column references.
# Used to suppress false positives in the column-existence check.
SQL_BUILTINS: frozenset[str] = frozenset({
    "sum", "count", "avg", "min", "max", "isnull", "nullif", "coalesce",
    "cast", "convert", "getdate", "dateadd", "datediff", "datename",
    "datepart", "year", "month", "day", "upper", "lower", "ltrim", "rtrim",
    "trim", "len", "left", "right", "substring", "charindex", "replace",
    "round", "abs", "ceiling", "floor", "power", "sqrt", "log",
    "row_number", "rank", "dense_rank", "ntile", "lag", "lead",
    "first_value", "last_value", "over", "partition",
    "case", "when", "then", "else", "end",
    "select", "from", "where", "group", "by", "order", "having",
    "join", "inner", "left", "right", "outer", "cross", "on",
    "union", "all", "intersect", "except", "distinct", "top", "with",
    "as", "and", "or", "not", "in", "between", "like", "is", "null",
    "exists", "any", "some", "asc", "desc", "into",
    # common CTE / alias names that Vanna tends to generate
    "cte", "sales", "revenue", "orders", "units", "data", "results",
    "ranked", "agg", "aggregated", "filtered", "base", "summary",
    "platform_sales", "monthly", "weekly", "daily",
    # metric aliases Vanna generates
    "total_revenue", "total_units", "total_orders", "net_revenue",
    "net_units", "order_count", "unit_count", "revenue", "units",
    "asp", "aov", "cancel_rate", "discount_pct", "pct", "rank",
    "row_num", "rn",
})

# Row cap for auto-TOP injection  (spec: "auto-add TOP 1000 if SELECT * without TOP")
AUTO_TOP_N = 1000


# ── Data model ─────────────────────────────────────────────────────────────────

class ViolationType(str, Enum):
    SECURITY = "security"   # hard reject — never retry
    SANITY   = "sanity"     # soft reject — pass to error_recovery.py


@dataclass
class ValidationResult:
    """
    Returned by validate_sql() for every call.

    Attributes:
        is_valid        True if the SQL passed both phases (possibly after auto-fix).
        fixed_sql       Always use this for execution — may differ from original_sql
                        (e.g. TOP injected).  Equals original_sql when no fix was applied.
        violation_type  "security" | "sanity" | None
        violations      Human-readable list of what went wrong.
                        Passed verbatim to error_recovery.py as retry context.
        original_sql    The raw string Vanna produced, unmodified.
        auto_fixed      True if a TOP was silently injected.
    """
    is_valid:       bool
    fixed_sql:      str
    violation_type: str | None
    violations:     list[str] = field(default_factory=list)
    original_sql:   str       = ""
    auto_fixed:     bool      = False

    # Convenience
    @property
    def is_security_violation(self) -> bool:
        return self.violation_type == ViolationType.SECURITY

    @property
    def is_sanity_violation(self) -> bool:
        return self.violation_type == ViolationType.SANITY

    def violation_summary(self) -> str:
        """Single-string summary for logging / UI display."""
        if not self.violations:
            return "No violations."
        return " | ".join(self.violations)


# ── SQL normalisation helpers ──────────────────────────────────────────────────

# Matches  -- single-line comments
_RE_LINE_COMMENT  = re.compile(r"--[^\n]*", re.MULTILINE)
# Matches  /* block comments */ — non-greedy
_RE_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
# Matches single-quoted string literals 'value' (handles escaped '' inside)
_RE_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'", re.DOTALL)
# Matches N'unicode string'
_RE_UNICODE_LITERAL = re.compile(r"N'(?:[^']|'')*'", re.DOTALL)


def _strip_comments(sql: str) -> str:
    """Remove -- and /* */ comments."""
    sql = _RE_BLOCK_COMMENT.sub(" ", sql)
    sql = _RE_LINE_COMMENT.sub(" ", sql)
    return sql


def _strip_string_literals(sql: str) -> str:
    """
    Replace all string literals with a safe placeholder.
    This prevents false-positives where blocked keywords appear inside a
    quoted value, e.g.  WHERE transaction_type = 'Cancel'  would otherwise
    trip a naive "Cancel" check.

    N'unicode' strings are handled first so N isn't left dangling.
    """
    sql = _RE_UNICODE_LITERAL.sub("'__STR__'", sql)
    sql = _RE_STRING_LITERAL.sub("'__STR__'", sql)
    return sql


def _normalize(sql: str) -> str:
    """
    Return a cleaned, upper-cased version of the SQL with:
      - Comments removed
      - String literals masked
      - Collapsed whitespace
    Used exclusively for pattern matching — never for execution.
    """
    s = _strip_comments(sql)
    s = _strip_string_literals(s)
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def _first_keyword(normalized_sql: str) -> str:
    """Return the first non-whitespace token of the normalized SQL."""
    tokens = normalized_sql.split()
    return tokens[0] if tokens else ""


# ── Security check helpers ─────────────────────────────────────────────────────

# All of these operate on normalized SQL (upper-case, no strings, no comments).

# DML / DDL statement starters that must never appear as the first keyword
# or anywhere as a standalone statement (stacked-injection concern handled separately)
_FORBIDDEN_STARTERS: tuple[str, ...] = (
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXECUTE", "EXEC", "MERGE",
    "BULK", "RESTORE", "BACKUP", "DBCC", "SHUTDOWN", "RECONFIGURE",
    "DENY", "ENABLE", "DISABLE",
)

# Dangerous sub-strings / tokens that must NEVER appear anywhere in the SQL,
# regardless of position.  Checked as whole-word boundaries where appropriate.
_FORBIDDEN_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # OS-level execution
    ("xp_cmdshell",        re.compile(r"\bXP_CMDSHELL\b")),
    ("xp_ procedure",      re.compile(r"\bXP_\w+")),
    ("sp_executesql",      re.compile(r"\bSP_EXECUTESQL\b")),
    ("sp_ procedure",      re.compile(r"\bSP_\w+")),
    # Bulk / external data access
    ("BULK INSERT",        re.compile(r"\bBULK\s+INSERT\b")),
    ("OPENROWSET",         re.compile(r"\bOPENROWSET\b")),
    ("OPENQUERY",          re.compile(r"\bOPENQUERY\b")),
    ("OPENDATASOURCE",     re.compile(r"\bOPENDATASOURCE\b")),
    # Dynamic SQL
    ("EXEC/EXECUTE",       re.compile(r"\bEXE?C(UTE)?\b")),
    # DML keywords anywhere (catches subquery injection like 'WHERE 1=1; DELETE')
    ("INSERT statement",   re.compile(r"\bINSERT\b")),
    ("UPDATE statement",   re.compile(r"\bUPDATE\b")),
    ("DELETE statement",   re.compile(r"\bDELETE\b")),
    ("DROP statement",     re.compile(r"\bDROP\b")),
    ("ALTER statement",    re.compile(r"\bALTER\b")),
    ("TRUNCATE statement", re.compile(r"\bTRUNCATE\b")),
    ("CREATE statement",   re.compile(r"\bCREATE\b")),
    ("GRANT statement",    re.compile(r"\bGRANT\b")),
    ("REVOKE statement",   re.compile(r"\bREVOKE\b")),
    ("MERGE statement",    re.compile(r"\bMERGE\b")),
    ("SHUTDOWN",           re.compile(r"\bSHUTDOWN\b")),
    ("RECONFIGURE",        re.compile(r"\bRECONFIGURE\b")),
    # INTO (SELECT INTO creates tables)
    ("SELECT INTO",        re.compile(r"\bSELECT\b.+\bINTO\b.+\bFROM\b")),
    # Linked server: four-part name  [server].[db].[schema].[table]
    # or   server.db.schema.table  (4 dot-separated identifiers)
    ("linked server (bracket notation)",
                           re.compile(r"\[[^\]]+\]\s*\.\s*\[[^\]]+\]\s*\.\s*\[[^\]]+\]\s*\.\s*\[[^\]]+\]")),
    ("linked server (dot notation)",
                           re.compile(r"\b\w+\.\w+\.\w+\.\w+\b")),
]

# Stacked-statement detection: a semicolon that is NOT the very last character
# (SQL Server allows a trailing semicolon on a single statement)
_RE_STACKED = re.compile(r";(?!\s*$)")


def _check_allowed_starter(norm: str) -> str | None:
    """
    Return a violation message if the SQL does not start with SELECT or WITH.
    None means OK.
    """
    first = _first_keyword(norm)
    if first not in ("SELECT", "WITH"):
        return (
            f"SQL must begin with SELECT or WITH (CTE).  "
            f"Got: '{first or '(empty)'}'. "
            f"Only read-only queries are permitted."
        )
    return None


def _check_forbidden_patterns(norm: str) -> list[str]:
    """
    Return a list of violation messages for every forbidden pattern found.
    Empty list means clean.
    """
    violations = []
    for label, pattern in _FORBIDDEN_PATTERNS:
        if pattern.search(norm):
            violations.append(f"Forbidden keyword/pattern detected: {label}.")
    return violations


def _check_stacked_statements(norm: str) -> str | None:
    """
    Detect semicolon-separated stacked statements (SQL injection vector).
    A trailing semicolon on a single statement is allowed.
    """
    if _RE_STACKED.search(norm):
        return (
            "Stacked statements detected (semicolon within query). "
            "Only a single SELECT statement is allowed."
        )
    return None


def _check_linked_server(norm: str) -> str | None:
    """
    Four-part names like [LinkedServer].[db].[schema].[table] allow
    cross-server queries and must never be allowed.
    Already covered by _FORBIDDEN_PATTERNS but kept explicit for log clarity.
    """
    # Re-use the patterns rather than duplicating logic
    for label, pattern in _FORBIDDEN_PATTERNS:
        if "linked server" in label and pattern.search(norm):
            return (
                "Linked-server / four-part-name access detected. "
                "Only [dbo].[B2B_B2C] within the local database is permitted."
            )
    return None


# ── Sanity check helpers ───────────────────────────────────────────────────────

# Matches   [dbo].[B2B_B2C]   or   [Charmacy_f_automate].[dbo].[B2B_B2C]
# or the unbracketed equivalents, case-insensitive.
_RE_TARGET_VIEW = re.compile(
    r"(\[?charmacy_f_automate\]?\s*\.\s*)?"      # optional DB qualifier
    r"\[?dbo\]?\s*\.\s*\[?B2B_B2C\]?",           # [dbo].[B2B_B2C]
    re.IGNORECASE,
)

# Matches  [identifier]  — explicitly bracket-quoted identifiers in the SQL.
# These are the only column refs we attempt to validate (unquoted identifiers
# are ambiguous with aliases, CTEs, and function names).
_RE_BRACKETED_IDENT = re.compile(r"\[([^\]]+)\]")

# Matches SELECT * without a preceding TOP N
# Handles:  SELECT *       SELECT  DISTINCT *      but not SELECT TOP 10 *
_RE_SELECT_STAR_NO_TOP = re.compile(
    r"\bSELECT\b(?!\s+TOP\b)(?:\s+DISTINCT\b)?\s+\*",
    re.IGNORECASE,
)

# Matches an existing TOP clause:  TOP 5   TOP (5)   TOP 1000
_RE_EXISTING_TOP = re.compile(r"\bTOP\s*\(?\s*\d+\s*\)?", re.IGNORECASE)


def _check_target_view_referenced(sql_original: str) -> str | None:
    """
    Warn if the SQL does not reference [dbo].[B2B_B2C].
    This catches cases where Vanna hallucinates a different table name.
    """
    if not _RE_TARGET_VIEW.search(sql_original):
        return (
            "Target view [dbo].[B2B_B2C] is not referenced in the query.  "
            "Vanna may have used an incorrect or hallucinated table name.  "
            "All queries must use [dbo].[B2B_B2C] (or its fully-qualified form)."
        )
    return None


def _check_bracketed_column_names(sql_original: str) -> list[str]:
    """
    Extract all explicitly bracket-quoted identifiers from the SQL and check
    them against KNOWN_COLUMNS + KNOWN_OBJECTS.

    Only bracket-quoted identifiers are checked — unquoted identifiers are too
    ambiguous (aliases, CTE names, function names, window partition names, etc.)
    to validate reliably with regex.

    Returns a list of violation messages for each unknown bracket-quoted name.
    """
    violations = []
    found = _RE_BRACKETED_IDENT.findall(sql_original)
    for ident in found:
        ident_lower = ident.lower().strip()
        if (
            ident_lower not in KNOWN_COLUMNS
            and ident_lower not in KNOWN_OBJECTS
            and ident_lower not in SQL_BUILTINS
        ):
            violations.append(
                f"Unknown bracket-quoted identifier [{ident}] — not a recognised "
                f"column name in [dbo].[B2B_B2C].  "
                f"Check spelling or remove the brackets."
            )
    return violations


def _auto_inject_top(sql: str) -> tuple[str, bool]:
    """
    If the outermost SELECT uses  SELECT *  without  TOP N, rewrite it as
    SELECT TOP 1000 *.

    Rules:
    - Only fires if SELECT * is present AND no TOP clause exists anywhere
      in the outermost SELECT (conservative: if any TOP exists, leave alone).
    - Returns (possibly_modified_sql, was_modified).
    - Does NOT touch subqueries that happen to use SELECT *.

    Why TOP 1000 in the validator (not just runtime cap in the executor)?
    Sending 50,000 rows from SQL Server to pandas is slow even if we truncate
    afterwards.  Capping at source is always faster.
    """
    # If any TOP already present → leave untouched
    if _RE_EXISTING_TOP.search(sql):
        return sql, False

    # Check for SELECT * pattern
    if not _RE_SELECT_STAR_NO_TOP.search(sql):
        return sql, False

    # Inject TOP 1000 after the first SELECT (handles DISTINCT too)
    def _inject(m: re.Match[str]) -> str:
        matched = m.group(0)
        # Preserve DISTINCT if present
        if re.search(r"\bDISTINCT\b", matched, re.IGNORECASE):
            return re.sub(r"(\bSELECT\b\s+\bDISTINCT\b)", rf"\1 TOP {AUTO_TOP_N}", matched, flags=re.IGNORECASE)
        return re.sub(r"(\bSELECT\b)", rf"\1 TOP {AUTO_TOP_N}", matched, flags=re.IGNORECASE, count=1)

    fixed = _RE_SELECT_STAR_NO_TOP.sub(_inject, sql, count=1)
    _root_logger.debug("AUTO-FIX: Injected TOP %d into SELECT *.", AUTO_TOP_N)
    return fixed, True


def _check_empty_sql(sql: str) -> str | None:
    if not sql or not sql.strip():
        return "Vanna returned an empty SQL string."
    return None


def _check_sql_too_short(sql: str) -> str | None:
    """Catch degenerate outputs like 'SELECT' with nothing after it."""
    norm = sql.strip().upper()
    if norm in ("SELECT", "WITH", "SELECT;", "WITH;"):
        return "SQL is incomplete — only the keyword was returned."
    return None


# ── Main public function ───────────────────────────────────────────────────────

def validate_sql(sql: str) -> ValidationResult:
    """
    Run both validation phases against a Vanna-generated SQL string.

    Returns a ValidationResult.  ALWAYS use result.fixed_sql for execution
    (it may have an auto-injected TOP clause).

    Phase 1 — Security:
        Any failure → is_valid=False, violation_type="security",
        logged to logs/security.log, NOT retried.

    Phase 2 — Sanity:
        Auto-fixable issues are silently corrected (auto_fixed=True).
        Remaining issues → is_valid=False, violation_type="sanity",
        violations list is forwarded to error_recovery.py as context.

    Args:
        sql: The raw SQL string from vn.generate_sql().

    Returns:
        ValidationResult with is_valid, fixed_sql, violation_type, violations.
    """
    original_sql = sql or ""

    # ── Pre-flight ────────────────────────────────────────────────────────────
    empty_err = _check_empty_sql(original_sql)
    if empty_err:
        _log_security(original_sql, [empty_err])
        return ValidationResult(
            is_valid=False,
            fixed_sql=original_sql,
            violation_type=ViolationType.SECURITY,
            violations=[empty_err],
            original_sql=original_sql,
        )

    short_err = _check_sql_too_short(original_sql)
    if short_err:
        return ValidationResult(
            is_valid=False,
            fixed_sql=original_sql,
            violation_type=ViolationType.SANITY,
            violations=[short_err],
            original_sql=original_sql,
        )

    # Normalized copy for pattern matching only — never executed
    norm = _normalize(original_sql)

    # ═════════════════════════════════════════════════════════════════════════
    # PHASE 1 — SECURITY
    # ═════════════════════════════════════════════════════════════════════════
    security_violations: list[str] = []

    starter_err = _check_allowed_starter(norm)
    if starter_err:
        security_violations.append(starter_err)

    security_violations.extend(_check_forbidden_patterns(norm))

    stacked_err = _check_stacked_statements(norm)
    if stacked_err:
        security_violations.append(stacked_err)

    # Linked-server check is already covered inside _check_forbidden_patterns,
    # but we call it separately for a more descriptive message in the log.
    # De-duplicate afterwards.
    linked_err = _check_linked_server(norm)
    if linked_err and linked_err not in security_violations:
        security_violations.append(linked_err)

    if security_violations:
        # De-duplicate while preserving order (Python 3.7+ dict trick)
        unique_violations = list(dict.fromkeys(security_violations))
        _log_security(original_sql, unique_violations)
        return ValidationResult(
            is_valid=False,
            fixed_sql=original_sql,
            violation_type=ViolationType.SECURITY,
            violations=unique_violations,
            original_sql=original_sql,
        )

    # ═════════════════════════════════════════════════════════════════════════
    # PHASE 2 — SANITY  (work on original_sql, not normalized)
    # ═════════════════════════════════════════════════════════════════════════
    sanity_violations: list[str] = []
    working_sql = original_sql
    auto_fixed  = False

    # 2a. Auto-fix: inject TOP if SELECT * without TOP
    working_sql, auto_fixed = _auto_inject_top(working_sql)
    if auto_fixed:
        _root_logger.info(
            "SQL auto-fixed: SELECT * → SELECT TOP %d *  (unbounded result set capped).",
            AUTO_TOP_N,
        )

    # 2b. Target view referenced?
    view_err = _check_target_view_referenced(working_sql)
    if view_err:
        sanity_violations.append(view_err)

    # 2c. Unknown bracket-quoted column names?
    col_violations = _check_bracketed_column_names(working_sql)
    sanity_violations.extend(col_violations)

    if sanity_violations:
        _root_logger.warning(
            "SANITY violations (%d): %s",
            len(sanity_violations),
            " | ".join(sanity_violations),
        )
        return ValidationResult(
            is_valid=False,
            fixed_sql=working_sql,
            violation_type=ViolationType.SANITY,
            violations=sanity_violations,
            original_sql=original_sql,
            auto_fixed=auto_fixed,
        )

    # ── All clear ─────────────────────────────────────────────────────────────
    _root_logger.debug("SQL passed both validation phases. auto_fixed=%s", auto_fixed)
    return ValidationResult(
        is_valid=True,
        fixed_sql=working_sql,
        violation_type=None,
        violations=[],
        original_sql=original_sql,
        auto_fixed=auto_fixed,
    )


# ── Internal logging helper ───────────────────────────────────────────────────

def _log_security(sql: str, violations: list[str]) -> None:
    """
    Write a security rejection event to logs/security.log.

    Format:
        SECURITY  <violation 1> | <violation 2>
                  SQL: SELECT ...
    """
    summary = " | ".join(violations)
    # Truncate SQL in log to 500 chars to keep log files manageable
    sql_excerpt = textwrap.shorten(sql.strip(), width=500, placeholder="…")
    _sec_logger.warning("%s  |||  SQL: %s", summary, sql_excerpt)
    _root_logger.warning("SECURITY violation — SQL rejected. %s", summary)


# ── Convenience wrapper used by error_recovery.py ─────────────────────────────

def build_correction_context(
    result: ValidationResult,
    question: str,
) -> str:
    """
    Build a correction_question string to pass back to vn.generate_sql()
    during retry.  Called by error_recovery.py.

    Format (plain English so the LLM can act on it directly):

        The following SQL was generated for the question:
        "<question>"

        SQL:
        <failed_sql>

        Validation errors that must be fixed:
        1. <violation 1>
        2. <violation 2>

        Please generate a corrected SQL query that addresses all of the above
        errors.  Use [dbo].[B2B_B2C], T-SQL syntax (TOP N not LIMIT N), and
        apply the net sales filter.
    """
    violation_lines = "\n".join(
        f"{i + 1}. {v}" for i, v in enumerate(result.violations)
    )
    return (
        f'The following SQL was generated for the question:\n'
        f'"{question}"\n\n'
        f"SQL:\n{result.original_sql}\n\n"
        f"Validation errors that must be fixed:\n{violation_lines}\n\n"
        f"Please generate a corrected SQL query that:\n"
        f"- Uses [dbo].[B2B_B2C] (the only available view)\n"
        f"- Uses T-SQL syntax (TOP N not LIMIT, ISNULL not COALESCE, GETDATE() not NOW())\n"
        f"- Applies the net sales filter (exclude Amazon Cancel/Refund, Flipkart Return/RTO, Shopify unfulfilled)\n"
        f"- Starts with SELECT or WITH (read-only only)\n"
        f"- Uses ORDER BY MIN(order_date) for chronological sort (not ORDER BY month_year)\n"
        f"- Returns ONLY the SQL query — no explanations, no markdown."
    )


# ── __all__ ───────────────────────────────────────────────────────────────────

__all__ = [
    "validate_sql",
    "ValidationResult",
    "ViolationType",
    "build_correction_context",
    "KNOWN_COLUMNS",
    "AUTO_TOP_N",
]