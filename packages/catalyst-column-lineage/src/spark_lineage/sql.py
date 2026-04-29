"""SQL → Spark Catalyst LogicalPlan JSON via PySpark.

This module makes the rest of `spark_lineage` work directly on SQL text:
just pass your query string into `build_lineage(...)` / `to_svg(...)` /
`to_html(...)` / etc. and a local `SparkSession` will analyse it for you,
returning the same pre-order JSON list of plan-node dicts that the
Catalyst `LogicalPlan.toJSON()` machinery emits.

PySpark is an **optional** dependency. Install it explicitly or via the
package extra:

    pip install catalyst-column-lineage[sql]

Public API
----------

* :func:`sql_to_plan` — convert a SQL query into a list of plan-node
  dicts (the same format as the project's ``test_json`` files).
* :func:`default_session` — build a default local-mode ``SparkSession``
  with sensible defaults (Hive support, configurable warehouse and
  metastore directories).
* :func:`looks_like_json` — small helper used by the auto-dispatch in
  :mod:`spark_lineage.api`.

Notes on multi-statement scripts
--------------------------------

Each call to :func:`sql_to_plan` accepts either a single SQL statement
or a script with ``;`` separators. When several statements are present,
all but the last are executed against the session for their **side
effects** (think ``USE db``, ``CREATE TABLE``, ``INSERT``); the analyser
then captures the analyzed plan for the **last** statement and returns
it. Statements inside string literals or block / line comments are not
split — the splitter is comment- and quote-aware.
"""

from __future__ import annotations

import json as _json
import re
from typing import Any, Iterable, List, Optional

DEFAULT_APP_NAME = "spark_lineage"


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def looks_like_json(text: str) -> bool:
    """Return ``True`` when *text* most likely is a JSON document.

    Uses a cheap first-non-whitespace-char heuristic so we avoid
    parsing potentially-large strings twice. Plan dumps always start
    with ``[`` (a list of nodes) or ``{`` (a wrapper dict / a single
    node).
    """
    s = text.lstrip()
    return bool(s) and s[0] in "[{"


def sql_to_plan(
    sql: str,
    *,
    spark: Optional[Any] = None,
    setup: Optional[Iterable[str]] = None,
    warehouse: Optional[str] = None,
    metastore: Optional[str] = None,
    enable_hive: bool = True,
    app_name: str = DEFAULT_APP_NAME,
    config: Optional[dict] = None,
) -> List[dict]:
    """Resolve and serialise *sql* through Spark, returning the analyzed plan.

    Parameters
    ----------
    sql
        SQL text. May contain several ``;``-separated statements; only
        the **last** one's analysed plan is returned, the rest are
        executed as setup.
    spark
        Optional ``pyspark.sql.SparkSession`` to use. When omitted, a
        local-mode session is created via :func:`default_session`.
    setup
        Optional iterable of additional SQL statements to execute
        before the main query (e.g. ``USE database``,
        ``CREATE TEMP VIEW`` …). Useful when analysing a query in
        isolation.
    warehouse, metastore
        Forwarded to :func:`default_session` when *spark* is not
        provided.
    enable_hive
        Forwarded to :func:`default_session` when *spark* is not
        provided. Defaults to ``True`` (matches the way Spark normally
        captures plans against the default Hive catalog).
    app_name
        Spark application name when a new session is created.
    config
        Extra ``{"spark.foo": "bar"}`` configuration entries used when
        a new session is created.

    Returns
    -------
    list[dict]
        The pre-order list of plan-node dicts (the same shape the rest
        of the package ingests).
    """

    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("sql must be a non-empty string")

    statements = split_statements(sql)
    if not statements:
        raise ValueError("No statements found in SQL input")

    if spark is None:
        spark = default_session(
            warehouse=warehouse,
            metastore=metastore,
            enable_hive=enable_hive,
            app_name=app_name,
            config=config,
        )

    if setup:
        for stmt in setup:
            stmt = stmt.strip().rstrip(";").strip()
            if stmt:
                spark.sql(stmt)

    *prelude, main_stmt = statements
    for stmt in prelude:
        spark.sql(stmt)

    df = spark.sql(main_stmt)
    plan = df._jdf.queryExecution().analyzed()
    json_str: str = plan.toJSON()
    return _json.loads(json_str)


def default_session(
    *,
    warehouse: Optional[str] = None,
    metastore: Optional[str] = None,
    enable_hive: bool = True,
    app_name: str = DEFAULT_APP_NAME,
    config: Optional[dict] = None,
):
    """Return a local-mode ``SparkSession`` configured for plan capture.

    All arguments are optional. Reuses the existing session of the
    process when one is already alive (Spark allows only one driver
    per JVM).
    """

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - depends on env
        raise ImportError(
            "PySpark is required to convert SQL to a Spark logical plan. "
            "Install it via `pip install catalyst-column-lineage[sql]` "
            "or `pip install pyspark`."
        ) from exc

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", str(warehouse))
    if metastore:
        builder = builder.config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
    if config:
        for k, v in config.items():
            builder = builder.config(k, v)
    if enable_hive:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Comment- and quote-aware ``;`` splitter
# ---------------------------------------------------------------------------


_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


def split_statements(sql: str) -> List[str]:
    """Split a SQL script on ``;`` while respecting strings and comments.

    Empty trailing statements are dropped; whitespace is trimmed.
    """

    out: List[str] = []
    buf: List[str] = []

    i, n = 0, len(sql)
    quote: Optional[str] = None  # "'" / '"' / '`'
    in_line_comment = False
    in_block_comment = False

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue
        if quote is not None:
            buf.append(ch)
            if ch == "\\" and nxt:
                buf.append(nxt)
                i += 2
                continue
            if ch == quote:
                # Doubled quote escape: ''  ""  ``
                if nxt == quote:
                    buf.append(nxt)
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        # Not inside a string / comment.
        if ch == "-" and nxt == "-":
            in_line_comment = True
            buf.append(ch)
            i += 1
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            buf.append(ch)
            buf.append(nxt)
            i += 2
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out
