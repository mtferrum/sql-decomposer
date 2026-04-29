"""High-level Python API for `spark_lineage`.

Every entry point accepts a plan in any of the supported forms and
returns the requested artefact:

    plan input                                  → artefact
    ───────────────────────────────────────────────────────
    str | bytes                JSON text         build_lineage(...) → LineageResult
    str                        SQL query         to_json(...)       → dict
    list[dict]                 native pre-order  to_text(...)       → str
    dict (single node /        wrapper           to_svg(...)        → str
        {"plan": [...]} /                        to_dot(...)        → str
        {"logicalPlan": [...]})                  to_html(...)       → str
                                                 save(plan, path)   → Path

When the input is a `str` / `bytes`, the first non-whitespace character
decides the dispatch: ``[`` / ``{`` → JSON, anything else → SQL (the
query is resolved and serialised through PySpark via
:func:`spark_lineage.sql.sql_to_plan`). Pass ``language="json"`` /
``language="sql"`` to bypass the auto-detection, and ``sql_kwargs={...}``
to control the SQL session (custom ``spark`` instance, ``warehouse=``,
``metastore=``, ``setup=`` …).

Optionally each renderer may write directly to a file via the `path`
argument; the directory is created on demand.
"""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Any, Optional, Union

from .analyzer import analyze
from .models import LineageResult, PlanNode
from .parser import PlanInput, parse_plan
from .visualize import (
    render_dot,
    render_html,
    render_svg,
    render_text,
)


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------


PlanLike = Union[PlanInput, LineageResult]
PathLike = Union[str, Path]


# ---------------------------------------------------------------------------
# SQL / JSON dispatch
# ---------------------------------------------------------------------------


def _coerce_sql(text: str, sql_kwargs: Optional[dict]) -> list:
    from .sql import sql_to_plan

    return sql_to_plan(text, **(sql_kwargs or {}))


def _maybe_route_sql(
    plan: PlanLike,
    *,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> PlanLike:
    """Route bare SQL strings through PySpark when *language* permits it.

    Returns *plan* unchanged when it is anything other than a string-ish
    object that was classified as SQL.
    """
    from .sql import looks_like_json

    lang = (language or "auto").lower()
    if lang not in {"auto", "json", "sql"}:
        raise ValueError(f"language must be 'auto', 'json' or 'sql' (got {language!r})")

    if isinstance(plan, (str, bytes, bytearray)):
        text = plan.decode() if isinstance(plan, (bytes, bytearray)) else plan
        if lang == "sql":
            return _coerce_sql(text, sql_kwargs)
        if lang == "auto" and not looks_like_json(text):
            return _coerce_sql(text, sql_kwargs)
        return plan
    return plan


# ---------------------------------------------------------------------------
# Coercion helpers
# ---------------------------------------------------------------------------


def plan_from_json(plan: PlanInput) -> PlanNode:
    """Parse any supported plan representation into a `PlanNode` tree."""
    return parse_plan(plan)


def plan_from_sql(sql: str, **sql_kwargs: Any) -> PlanNode:
    """Resolve *sql* through PySpark and return a `PlanNode` tree."""
    from .sql import sql_to_plan

    return parse_plan(sql_to_plan(sql, **sql_kwargs))


def build_lineage(
    plan: PlanLike,
    *,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> LineageResult:
    """Parse + analyse, returning a `LineageResult`.

    Accepts:
        * JSON text (`str` / `bytes`) — Spark Catalyst plan dump,
        * SQL text (`str`) — resolved through PySpark (optional
          dependency, install with the ``[sql]`` extra),
        * Spark's native pre-order list of plan dicts,
        * a single plan-node dict,
        * a wrapper dict like ``{"plan": [...]}`` / ``{"logicalPlan": [...]}`` /
          ``{"nodes": [...]}``,
        * an already-built ``LineageResult`` (returned unchanged for
          chaining).

    Use ``language="json"`` / ``language="sql"`` to skip auto-detection,
    and ``sql_kwargs={"spark": <session>, "warehouse": ..., ...}`` to
    customise SQL resolution; see :func:`spark_lineage.sql.sql_to_plan`.
    """
    if isinstance(plan, LineageResult):
        return plan
    plan = _maybe_route_sql(plan, language=language, sql_kwargs=sql_kwargs)
    return analyze(parse_plan(plan))


def _ensure_result(
    plan: PlanLike,
    *,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> LineageResult:
    if isinstance(plan, LineageResult):
        return plan
    return build_lineage(plan, language=language, sql_kwargs=sql_kwargs)


def _maybe_save(payload: str, path: Optional[PathLike]) -> str:
    if path is None:
        return payload
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        p.write_text(payload, encoding="utf-8")
    else:  # bytes
        p.write_bytes(payload)
    return payload


# ---------------------------------------------------------------------------
# Format-specific entry points
# ---------------------------------------------------------------------------


def to_json(
    plan: PlanLike,
    *,
    path: Optional[PathLike] = None,
    indent: Optional[int] = 2,
    ensure_ascii: bool = False,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> dict:
    """Return the lineage as a plain `dict` (and optionally save as JSON)."""
    result = _ensure_result(plan, language=language, sql_kwargs=sql_kwargs)
    payload = result.to_dict()
    if path is not None:
        text = _json.dumps(payload, ensure_ascii=ensure_ascii, indent=indent)
        _maybe_save(text, path)
    return payload


def to_text(
    plan: PlanLike,
    *,
    path: Optional[PathLike] = None,
    file_name: str = "",
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> str:
    """Return the human-readable text summary."""
    result = _ensure_result(plan, language=language, sql_kwargs=sql_kwargs)
    return _maybe_save(render_text(result, file_name=file_name), path)


def to_svg(
    plan: PlanLike,
    *,
    path: Optional[PathLike] = None,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> str:
    """Return the standalone SVG document."""
    result = _ensure_result(plan, language=language, sql_kwargs=sql_kwargs)
    return _maybe_save(render_svg(result), path)


def to_dot(
    plan: PlanLike,
    *,
    path: Optional[PathLike] = None,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> str:
    """Return the Graphviz DOT description."""
    result = _ensure_result(plan, language=language, sql_kwargs=sql_kwargs)
    return _maybe_save(render_dot(result), path)


def to_html(
    plan: PlanLike,
    *,
    path: Optional[PathLike] = None,
    title: str = "Column lineage",
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> str:
    """Return the SVG-embedded HTML page."""
    result = _ensure_result(plan, language=language, sql_kwargs=sql_kwargs)
    return _maybe_save(render_html(result, title=title), path)


# ---------------------------------------------------------------------------
# Generic save (auto-detects format from extension)
# ---------------------------------------------------------------------------


_EXTS = {
    ".json": "json",
    ".txt":  "text",
    ".text": "text",
    ".svg":  "svg",
    ".dot":  "dot",
    ".gv":   "dot",
    ".html": "html",
    ".htm":  "html",
}


def save(
    plan: PlanLike,
    path: PathLike,
    *,
    format: Optional[str] = None,
    language: str = "auto",
    sql_kwargs: Optional[dict] = None,
) -> Path:
    """Write the lineage to `path`, picking the format from the file extension.

    Use `format=` to override (`json` / `text` / `svg` / `dot` / `html`).
    Returns the resolved `Path`."""
    p = Path(path)
    fmt = (format or _EXTS.get(p.suffix.lower()) or "").lower()
    if not fmt:
        raise ValueError(
            f"Cannot infer format from extension '{p.suffix}'. "
            f"Pass format='json'|'text'|'svg'|'dot'|'html'."
        )
    result = _ensure_result(plan, language=language, sql_kwargs=sql_kwargs)
    if fmt == "json":
        text = _json.dumps(result.to_dict(), ensure_ascii=False, indent=2)
    elif fmt == "text":
        text = render_text(result)
    elif fmt == "svg":
        text = render_svg(result)
    elif fmt == "dot":
        text = render_dot(result)
    elif fmt == "html":
        text = render_html(result)
    else:
        raise ValueError(f"Unknown format: {fmt!r}")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p
