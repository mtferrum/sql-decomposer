"""Column-level data lineage from Spark Catalyst LogicalPlan JSON or SQL text.

Quick start
-----------

>>> from spark_lineage import build_lineage, to_svg
>>> result = build_lineage(open("test_json/030_04_cte_and_aggregations_02.json").read())
>>> result.source_tables
['spark_catalog.default.employees']
>>> to_svg("SELECT a + b AS c FROM t", path="lineage.svg")  # SQL via PySpark

All entry points accept any of:
    * `str` / `bytes` — JSON plan dump (auto-detected by leading ``[`` / ``{``)
    * `str`           — SQL query (anything else; resolved via PySpark)
    * `list[dict]`    — Spark's native pre-order plan list
    * `dict`          — single plan node, or a wrapper such as
                        `{"plan": [...]}` / `{"logicalPlan": [...]}` / `{"nodes": [...]}`
    * `LineageResult` — an already-analysed result (returned unchanged)

Pass ``language="json"`` / ``language="sql"`` to bypass the auto-detection
and ``sql_kwargs={...}`` to control the SQL session (custom ``spark``
instance, ``warehouse=``, ``metastore=``, ``setup=`` …).
"""

from .models import (
    ColInfo,
    LineageResult,
    PlanKind,
    SourceColumn,
    TargetTable,
    TransformationType,
)
from .parser import parse_plan
from .analyzer import analyze
from .api import (
    build_lineage,
    plan_from_json,
    plan_from_sql,
    save,
    to_dot,
    to_html,
    to_json,
    to_svg,
    to_text,
)
from .sql import default_session, looks_like_json, sql_to_plan, split_statements
from .visualize import render_dot, render_html, render_svg, render_text

__all__ = [
    # Data classes
    "ColInfo",
    "LineageResult",
    "PlanKind",
    "SourceColumn",
    "TargetTable",
    "TransformationType",
    # Core
    "analyze",
    "build_lineage",
    "parse_plan",
    "plan_from_json",
    "plan_from_sql",
    # High-level helpers
    "to_json",
    "to_text",
    "to_svg",
    "to_dot",
    "to_html",
    "save",
    # SQL helpers (require pyspark)
    "default_session",
    "looks_like_json",
    "sql_to_plan",
    "split_statements",
    # Low-level renderers (operate on a LineageResult)
    "render_dot",
    "render_html",
    "render_svg",
    "render_text",
]
