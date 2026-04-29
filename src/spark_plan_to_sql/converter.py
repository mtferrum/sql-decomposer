"""Spark Catalyst LogicalPlan (JSON) to SQL converter.

A fresh, self-contained implementation that walks the JSON serialization of
Spark's logical plan and produces a logically equivalent SQL statement. It is
designed to be general purpose: any node it does not recognize is rendered
through a best-effort generic translation rather than crashing the pipeline.

Entry points:
    plan_to_sql(plan: str | bytes | dict | list) -> str
    dict_to_sql(plan: dict) -> str            -- dict-only convenience wrapper
    main()                                     -- batch CLI
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union


# ---------------------------------------------------------------------------
# Tree parsing
# ---------------------------------------------------------------------------


@dataclass
class Node:
    """Node of a Spark plan/expression tree."""

    cls: str
    full: str
    p: Dict[str, Any]
    kids: List["Node"] = field(default_factory=list)


def _parse_one(items: Sequence[Dict[str, Any]], idx: int) -> Tuple[Node, int]:
    item = items[idx]
    full = str(item.get("class", ""))
    cls = full.rsplit(".", 1)[-1].rstrip("$")
    n = int(item.get("num-children", 0))
    kids: List[Node] = []
    nxt = idx + 1
    for _ in range(n):
        child, nxt = _parse_one(items, nxt)
        kids.append(child)
    return Node(cls, full, item, kids), nxt


def parse_plan(items: Sequence[Dict[str, Any]]) -> Node:
    return _parse_one(items, 0)[0]


def parse_expr_chain(chain: Sequence[Dict[str, Any]]) -> Node:
    """Parse a single inline expression chain (pre-order DFS)."""
    return _parse_one(chain, 0)[0]


# ---------------------------------------------------------------------------
# Helpers for identifiers / qualifiers
# ---------------------------------------------------------------------------


_BARE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _column_name_of(item: str) -> Optional[str]:
    """Return the public output name of a SELECT-list SQL fragment.

    Handles ``expr AS name`` aliases, ``alias.col`` references and bare
    identifiers (with or without backtick quoting).  Returns ``None`` if no
    name can be determined."""

    s = item.strip()
    m = re.search(r"\bAS\s+`([^`]+)`\s*$", s, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bAS\s+([A-Za-z_][\w]*)\s*$", s, re.IGNORECASE)
    if m:
        return m.group(1)
    if "(" in s or " " in s or "+" in s or "*" == s.strip():
        return None
    m = re.search(r"\.`([^`]+)`\s*$", s)
    if m:
        return m.group(1)
    m = re.search(r"\.([A-Za-z_][\w]*)\s*$", s)
    if m:
        return m.group(1)
    m = re.match(r"^`([^`]+)`\s*$", s)
    if m:
        return m.group(1)
    m = re.match(r"^([A-Za-z_][\w]*)\s*$", s)
    if m:
        return m.group(1)
    return None
_RESERVED = {
    "select", "from", "where", "group", "order", "by", "having", "limit",
    "join", "on", "and", "or", "not", "in", "is", "null", "true", "false",
    "case", "when", "then", "else", "end", "as", "with", "union", "intersect",
    "except", "distinct", "all", "any", "exists", "between", "like",
}


def quote_ident(name: str) -> str:
    if not name:
        return "``"
    if _BARE_IDENT.match(name) and name.lower() not in _RESERVED:
        return name
    return "`" + name.replace("`", "``") + "`"


def parse_qualifier(qual: Any) -> List[str]:
    """Spark's serialized qualifier may be a list, an empty list, or a string
    such as ``"[a, b, c]"``.  Returns a list of components."""
    if qual is None:
        return []
    if isinstance(qual, list):
        return [str(x) for x in qual]
    if isinstance(qual, str):
        s = qual.strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].strip()
            if not inner:
                return []
            return [p.strip() for p in inner.split(",") if p.strip()]
        return [s]
    return []


def qualifier_alias(qual: Any) -> Optional[str]:
    parts = parse_qualifier(qual)
    return parts[-1] if parts else None


def parse_namespace(ns: Any) -> List[str]:
    """``namespace`` may be ``"[db]"`` or a real list."""
    if ns is None:
        return []
    if isinstance(ns, list):
        return [str(x) for x in ns]
    return parse_qualifier(ns)


# ---------------------------------------------------------------------------
# Data type rendering
# ---------------------------------------------------------------------------


_TYPE_ALIASES = {
    "integer": "INT",
    "int": "INT",
    "long": "BIGINT",
    "short": "SMALLINT",
    "byte": "TINYINT",
    "double": "DOUBLE",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "string": "STRING",
    "binary": "BINARY",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ",
    "void": "VOID",
}


def render_type(t: Any) -> str:
    if isinstance(t, str):
        s = t.strip()
        if s.lower() in _TYPE_ALIASES:
            return _TYPE_ALIASES[s.lower()]
        m = re.match(r"^decimal\((\d+),\s*(\d+)\)$", s, re.IGNORECASE)
        if m:
            return f"DECIMAL({m.group(1)},{m.group(2)})"
        return s.upper()
    if isinstance(t, dict):
        kind = t.get("type")
        if kind == "array":
            return f"ARRAY<{render_type(t.get('elementType'))}>"
        if kind == "map":
            return (
                f"MAP<{render_type(t.get('keyType'))}, "
                f"{render_type(t.get('valueType'))}>"
            )
        if kind == "struct":
            fields = []
            for f in t.get("fields", []):
                fields.append(f"{quote_ident(f['name'])}: {render_type(f['type'])}")
            return f"STRUCT<{', '.join(fields)}>"
    return "STRING"


# ---------------------------------------------------------------------------
# Literal formatting
# ---------------------------------------------------------------------------


_INTERVAL_DT_UNITS: List[Tuple[str, int, str]] = [
    ("DAYS", 86_400_000_000, "DAY"),
    ("HOURS", 3_600_000_000, "HOUR"),
    ("MINUTES", 60_000_000, "MINUTE"),
    ("SECONDS", 1_000_000, "SECOND"),
]
_INTERVAL_YM_UNITS: List[Tuple[str, int, str]] = [
    ("YEARS", 12, "YEAR"),
    ("MONTHS", 1, "MONTH"),
]


def _format_interval(value: Any, dtype: str) -> str:
    dt = dtype.lower()
    try:
        v = int(value)
    except (TypeError, ValueError):
        return f"INTERVAL '{value}' {dt[len('interval '):].upper()}"
    if dt in {"interval year", "interval month"} or "year-month" in dt:
        if v % 12 == 0 and v != 0:
            return f"INTERVAL {v // 12} YEARS"
        return f"INTERVAL {v} MONTHS"
    # day-time intervals: value is in microseconds
    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    for name, factor, _ in _INTERVAL_DT_UNITS:
        if factor == 0:
            continue
        if abs_v % factor == 0 and abs_v >= factor:
            return f"INTERVAL {sign}{abs_v // factor} {name}"
    if v == 0:
        return "INTERVAL 0 SECONDS"
    return f"INTERVAL {sign}{abs_v / 1_000_000} SECONDS"


def format_literal(value: Any, dtype: Any) -> str:
    if value is None:
        return "NULL"
    dtype_str = render_type(dtype) if isinstance(dtype, dict) else str(dtype or "")
    dt_lower = dtype_str.lower()

    if dt_lower.startswith("interval"):
        return _format_interval(value, dt_lower)

    if dt_lower == "string":
        s = str(value).replace("\\", "\\\\").replace("'", "''")
        return f"'{s}'"
    if dt_lower == "boolean":
        return "TRUE" if str(value).lower() in {"true", "1"} else "FALSE"
    if dt_lower == "date":
        return f"DATE '{value}'"
    if dt_lower in {"timestamp", "timestamp_ntz"}:
        return f"TIMESTAMP '{value}'"
    if dt_lower in {"binary"}:
        return f"X'{value}'"
    if dt_lower in {"int", "integer", "bigint", "long", "smallint", "short",
                    "tinyint", "byte"}:
        return str(value)
    if dt_lower in {"double", "float"}:
        s = str(value)
        if s in {"Infinity", "inf"}:
            return "double('inf')"
        if s in {"-Infinity", "-inf"}:
            return "double('-inf')"
        if s.lower() == "nan":
            return "double('nan')"
        return s
    if dt_lower.startswith("decimal"):
        s = str(value)
        if "BD" in s:
            s = s.replace("BD", "")
        return s
    if dt_lower in {"void", "null"}:
        return "NULL"
    return str(value)


# ---------------------------------------------------------------------------
# Expression compiler
# ---------------------------------------------------------------------------


_BINARY_OPS = {
    "Add": "+",
    "Subtract": "-",
    "Multiply": "*",
    "Divide": "/",
    "IntegralDivide": "DIV",
    "Remainder": "%",
    "Pmod": None,  # function call
    "BitwiseAnd": "&",
    "BitwiseOr": "|",
    "BitwiseXor": "^",
    "EqualTo": "=",
    "EqualNullSafe": "<=>",
    "GreaterThan": ">",
    "GreaterThanOrEqual": ">=",
    "LessThan": "<",
    "LessThanOrEqual": "<=",
    "And": "AND",
    "Or": "OR",
}

_UNARY_OPS = {
    "UnaryMinus": "-",
    "UnaryPositive": "+",
    "Not": "NOT ",
    "BitwiseNot": "~",
}

_FUNC_MAP = {
    # aggregate
    "Count": "count",
    "Sum": "sum",
    "Average": "avg",
    "Avg": "avg",
    "Max": "max",
    "Min": "min",
    "First": "first",
    "Last": "last",
    "CollectList": "collect_list",
    "CollectSet": "collect_set",
    "CountDistinct": "count",
    "ApproxCountDistinct": "approx_count_distinct",
    "StddevSamp": "stddev",
    "StddevPop": "stddev_pop",
    "VarianceSamp": "var_samp",
    "VariancePop": "var_pop",
    "Corr": "corr",
    # date/time
    "Year": "year",
    "Quarter": "quarter",
    "Month": "month",
    "DayOfMonth": "day",
    "DayOfWeek": "dayofweek",
    "DayOfYear": "dayofyear",
    "WeekOfYear": "weekofyear",
    "Hour": "hour",
    "Minute": "minute",
    "Second": "second",
    "DateAdd": "date_add",
    "DateSub": "date_sub",
    "DateDiff": "datediff",
    "AddMonths": "add_months",
    "MonthsBetween": "months_between",
    "ParseToDate": "to_date",
    "ParseToTimestamp": "to_timestamp",
    "TruncDate": "trunc",
    "TruncTimestamp": "date_trunc",
    "FromUnixTime": "from_unixtime",
    "UnixTimestamp": "unix_timestamp",
    "ToUnixTimestamp": "to_unix_timestamp",
    "DateFormatClass": "date_format",
    "CurrentDate": "current_date",
    "CurrentTimestamp": "current_timestamp",
    "Now": "now",
    # string
    "Upper": "upper",
    "Lower": "lower",
    "Length": "length",
    "Substring": "substring",
    "Trim": "trim",
    "Ltrim": "ltrim",
    "Rtrim": "rtrim",
    "StringTrim": "trim",
    "StringTrimLeft": "ltrim",
    "StringTrimRight": "rtrim",
    "Replace": "replace",
    "Reverse": "reverse",
    "Split": "split",
    "RegExpReplace": "regexp_replace",
    "RegExpExtract": "regexp_extract",
    "Concat": "concat",
    "ConcatWs": "concat_ws",
    "Like": None,  # special
    "RLike": "rlike",
    "FormatString": "format_string",
    "FormatNumber": "format_number",
    "Md5": "md5",
    "Sha1": "sha1",
    "Sha2": "sha2",
    "Hash": "hash",
    "Crc32": "crc32",
    "Base64": "base64",
    "UnBase64": "unbase64",
    # math
    "Abs": "abs",
    "Ceil": "ceil",
    "Floor": "floor",
    "Round": "round",
    "BRound": "bround",
    "Sqrt": "sqrt",
    "Exp": "exp",
    "Log": "log",
    "Log2": "log2",
    "Log10": "log10",
    "Pow": "pow",
    "Sin": "sin",
    "Cos": "cos",
    "Tan": "tan",
    "Acos": "acos",
    "Asin": "asin",
    "Atan": "atan",
    "Atan2": "atan2",
    "ToRadians": "radians",
    "ToDegrees": "degrees",
    "Greatest": "greatest",
    "Least": "least",
    "Rand": "rand",
    "Randn": "randn",
    "ShiftLeft": "shiftleft",
    "ShiftRight": "shiftright",
    "Conv": "conv",
    "Bin": "bin",
    "Hex": "hex",
    "Unhex": "unhex",
    # null / conversion
    "Coalesce": "coalesce",
    "IfNull": "ifnull",
    "NullIf": "nullif",
    "Nvl": "nvl",
    "Nvl2": "nvl2",
    # collections
    "Size": "size",
    "Sort_Array": "sort_array",
    "ArrayContains": "array_contains",
    "ArrayDistinct": "array_distinct",
    "ArrayUnion": "array_union",
    "ArrayIntersect": "array_intersect",
    "ArrayExcept": "array_except",
    "ArrayJoin": "array_join",
    "ArrayMax": "array_max",
    "ArrayMin": "array_min",
    "Flatten": "flatten",
    "Slice": "slice",
    "MapKeys": "map_keys",
    "MapValues": "map_values",
    "MapEntries": "map_entries",
    "MapFromArrays": "map_from_arrays",
    "MapFromEntries": "map_from_entries",
    "MapConcat": "map_concat",
    "ElementAt": "element_at",
    "ArrayPosition": "array_position",
    "CreateArray": "array",
    "CreateMap": "map",
    "CreateStruct": "struct",
    "CreateNamedStruct": None,  # special
    "ArrayTransform": None,     # special
    "ArrayFilter": None,        # special
    "ArrayExists": None,        # special
    "ArrayAggregate": None,     # special
    "ArrayForAll": None,        # special
    # JSON / interval / cast / misc
    "GetJsonObject": "get_json_object",
    "JsonTuple": "json_tuple",
    "FromJson": None,           # JsonToStructs handles this
    "JsonToStructs": None,      # special
    "StructsToJson": "to_json",
    "Explode": "explode",
    "ExplodeOuter": "explode_outer",
    "PosExplode": "posexplode",
    "PosExplodeOuter": "posexplode_outer",
    "Inline": "inline",
    "InlineOuter": "inline_outer",
    "Stack": "stack",
    "Sequence": "sequence",
    # window helpers
    "Lag": "lag",
    "Lead": "lead",
    "RowNumber": "row_number",
    "Rank": "rank",
    "DenseRank": "dense_rank",
    "PercentRank": "percent_rank",
    "CumeDist": "cume_dist",
    "Ntile": "ntile",
    "NthValue": "nth_value",
    "FirstValue": "first_value",
    "LastValue": "last_value",
    # interval extraction
    "ExtractANSIIntervalDays": "extract_days",
    "ExtractANSIIntervalHours": "extract_hours",
    "ExtractANSIIntervalMinutes": "extract_minutes",
    "ExtractANSIIntervalSeconds": "extract_seconds",
    "ExtractANSIIntervalMonths": "extract_months",
    "ExtractANSIIntervalYears": "extract_years",
}


def _spark_func_name(cls: str) -> str:
    """Camel/Pascal case to snake case as a default function name."""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", cls)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


@dataclass
class ExprContext:
    """Context for compiling expressions: alias map and current outer aliases."""

    alias_map: Dict[int, str] = field(default_factory=dict)
    # exprId -> sql fragment (for window/aggregate output substitution)
    inline_aliases: Dict[int, str] = field(default_factory=dict)


class ExprCompiler:
    """Compile an expression tree to a SQL fragment."""

    def __init__(self, ctx: Optional[ExprContext] = None):
        self.ctx = ctx or ExprContext()

    # entry points -----------------------------------------------------

    def compile(self, node: Node) -> str:
        cls = node.cls
        method = getattr(self, f"e_{cls}", None)
        if method:
            return method(node)
        if cls in _UNARY_OPS:
            return self._unary(node, _UNARY_OPS[cls])
        if cls in _BINARY_OPS and _BINARY_OPS[cls] is not None:
            return self._binary(node, _BINARY_OPS[cls])
        if cls in _FUNC_MAP and _FUNC_MAP[cls] is not None:
            return self._call(_FUNC_MAP[cls], node.kids)
        return self._call(_spark_func_name(cls), node.kids)

    def compile_chain(self, chain: Sequence[Dict[str, Any]]) -> str:
        return self.compile(parse_expr_chain(chain))

    # generic helpers --------------------------------------------------

    def _call(self, name: str, kids: Sequence[Node]) -> str:
        args = ", ".join(self.compile(k) for k in kids)
        return f"{name}({args})"

    def _unary(self, node: Node, op: str) -> str:
        inner = self.compile(node.kids[0])
        if op.endswith(" "):
            return f"({op}{inner})"
        return f"({op}{inner})"

    def _binary(self, node: Node, op: str) -> str:
        left = self.compile(node.kids[0])
        right = self.compile(node.kids[1])
        if op in {"AND", "OR"}:
            return f"({left} {op} {right})"
        return f"({left} {op} {right})"

    # specific expressions --------------------------------------------

    def e_Literal(self, node: Node) -> str:
        return format_literal(node.p.get("value"), node.p.get("dataType"))

    def e_AttributeReference(self, node: Node) -> str:
        name = node.p.get("name", "")
        alias = qualifier_alias(node.p.get("qualifier"))
        if alias:
            return f"{quote_ident(alias)}.{quote_ident(name)}"
        return quote_ident(name)

    def e_OuterReference(self, node: Node) -> str:
        if node.kids:
            return self.compile(node.kids[0])
        # ``OuterReference`` stores its referenced expression as an inline
        # chain in property ``e`` when serialized.
        chain = node.p.get("e") or node.p.get("child")
        if isinstance(chain, list) and chain:
            return self.compile_chain(chain)
        return ""

    def e_Alias(self, node: Node) -> str:
        inner = self.compile(node.kids[0])
        name = node.p.get("name", "")
        return f"{inner} AS {quote_ident(name)}"

    def e_Cast(self, node: Node) -> str:
        inner = self.compile(node.kids[0])
        dtype = render_type(node.p.get("dataType"))
        return f"CAST({inner} AS {dtype})"

    def e_Coalesce(self, node: Node) -> str:
        return self._call("coalesce", node.kids)

    def e_If(self, node: Node) -> str:
        cond = self.compile(node.kids[0])
        a = self.compile(node.kids[1])
        b = self.compile(node.kids[2])
        return f"if({cond}, {a}, {b})"

    def e_CaseWhen(self, node: Node) -> str:
        # Spark stores CaseWhen as flat children: branches (cond, val) pairs
        # then optional else value. branches/elseValue lists give the indices.
        branches = node.p.get("branches", [])
        else_value = node.p.get("elseValue")
        # In serialized JSON, branches is typically a list of [cond_idx,
        # val_idx] pairs. Iterate over kids in order: pairs then else.
        kids = node.kids
        parts = ["CASE"]
        if branches:
            for i in range(0, len(branches)):
                cond = self.compile(kids[2 * i])
                val = self.compile(kids[2 * i + 1])
                parts.append(f"WHEN {cond} THEN {val}")
            if else_value:
                parts.append(f"ELSE {self.compile(kids[-1])}")
        else:
            # Best-effort: pair up kids
            i = 0
            while i + 1 < len(kids):
                parts.append(f"WHEN {self.compile(kids[i])} THEN "
                             f"{self.compile(kids[i + 1])}")
                i += 2
            if i < len(kids):
                parts.append(f"ELSE {self.compile(kids[i])}")
        parts.append("END")
        return "(" + " ".join(parts) + ")"

    def e_IsNull(self, node: Node) -> str:
        return f"({self.compile(node.kids[0])} IS NULL)"

    def e_IsNotNull(self, node: Node) -> str:
        return f"({self.compile(node.kids[0])} IS NOT NULL)"

    def e_In(self, node: Node) -> str:
        v = self.compile(node.kids[0])
        rest = ", ".join(self.compile(k) for k in node.kids[1:])
        return f"({v} IN ({rest}))"

    def e_InSet(self, node: Node) -> str:
        v = self.compile(node.kids[0])
        items = node.p.get("hset", [])
        rendered = ", ".join(format_literal(x, "string") for x in items)
        return f"({v} IN ({rendered}))"

    def e_Like(self, node: Node) -> str:
        return f"({self.compile(node.kids[0])} LIKE {self.compile(node.kids[1])})"

    def e_Pmod(self, node: Node) -> str:
        return self._call("pmod", node.kids)

    def e_Substring(self, node: Node) -> str:
        return self._call("substring", node.kids)

    def e_Concat(self, node: Node) -> str:
        return self._call("concat", node.kids)

    def e_TimeAdd(self, node: Node) -> str:
        a = self.compile(node.kids[0])
        b = self.compile(node.kids[1])
        return f"({a} + {b})"

    def e_TimeSub(self, node: Node) -> str:
        a = self.compile(node.kids[0])
        b = self.compile(node.kids[1])
        return f"({a} - {b})"

    def e_DateAddInterval(self, node: Node) -> str:
        return f"({self.compile(node.kids[0])} + {self.compile(node.kids[1])})"

    def e_AddMonths(self, node: Node) -> str:
        return self._call("add_months", node.kids)

    def e_DatetimeSub(self, node: Node) -> str:
        # Spark surfaces ``DatetimeSub`` with a ``replacement`` child that is
        # equivalent to the user-visible expression.  Fall back to building a
        # subtraction directly otherwise.
        if node.kids:
            return self.compile(node.kids[0])
        return self._call("datetime_sub", node.kids)

    def e_TruncTimestamp(self, node: Node) -> str:
        # ``date_trunc(format, timestamp)``.  In the serialized form the
        # children come out as (format, timestamp).
        return self._call("date_trunc", node.kids)

    def e_TruncDate(self, node: Node) -> str:
        return self._call("trunc", node.kids)

    # ANSI interval extraction --------------------------------------
    # ``ExtractANSIInterval*`` is a Catalyst-internal node with no public
    # SQL function name.  Render it as ``EXTRACT(unit FROM ...)`` instead so
    # the resulting SQL is portable.

    def _extract_unit(self, unit: str, node: Node) -> str:
        return f"extract({unit} FROM {self.compile(node.kids[0])})"

    def e_ExtractANSIIntervalDays(self, node: Node) -> str:
        return self._extract_unit("DAY", node)

    def e_ExtractANSIIntervalHours(self, node: Node) -> str:
        return self._extract_unit("HOUR", node)

    def e_ExtractANSIIntervalMinutes(self, node: Node) -> str:
        return self._extract_unit("MINUTE", node)

    def e_ExtractANSIIntervalSeconds(self, node: Node) -> str:
        return self._extract_unit("SECOND", node)

    def e_ExtractANSIIntervalMonths(self, node: Node) -> str:
        return self._extract_unit("MONTH", node)

    def e_ExtractANSIIntervalYears(self, node: Node) -> str:
        return self._extract_unit("YEAR", node)

    def e_DateAdd(self, node: Node) -> str:
        # Spark rewrites ``date + INTERVAL n DAYS`` as
        # ``DateAdd(date, ExtractANSIIntervalDays(interval))``; render it back
        # as a plain ``date + interval`` expression so the SQL stays portable.
        if (
            len(node.kids) == 2
            and node.kids[1].cls == "ExtractANSIIntervalDays"
            and node.kids[1].kids
            and node.kids[1].kids[0].cls == "Literal"
        ):
            interval = self.compile(node.kids[1].kids[0])
            base = self.compile(node.kids[0])
            return f"({base} + {interval})"
        return self._call("date_add", node.kids)

    def e_GetStructField(self, node: Node) -> str:
        struct = self.compile(node.kids[0])
        name = node.p.get("name") or node.p.get("ordinal")
        if isinstance(name, int):
            return f"{struct}[{name}]"
        return f"{struct}.{quote_ident(str(name))}"

    def e_GetArrayItem(self, node: Node) -> str:
        arr = self.compile(node.kids[0])
        idx = self.compile(node.kids[1]) if len(node.kids) > 1 else "0"
        return f"{arr}[{idx}]"

    def e_GetMapValue(self, node: Node) -> str:
        m = self.compile(node.kids[0])
        k = self.compile(node.kids[1])
        return f"element_at({m}, {k})"

    def e_ElementAt(self, node: Node) -> str:
        return self._call("element_at", node.kids)

    def e_CreateNamedStruct(self, node: Node) -> str:
        # children are alternating name, value (Literal name + value)
        parts = []
        for i in range(0, len(node.kids), 2):
            if i + 1 >= len(node.kids):
                break
            name_node = node.kids[i]
            val_node = node.kids[i + 1]
            if name_node.cls == "Literal":
                key = format_literal(name_node.p.get("value"), "string")
            else:
                key = self.compile(name_node)
            val = self.compile(val_node)
            parts.append(f"{key}, {val}")
        return f"named_struct({', '.join(parts)})"

    def e_LambdaFunction(self, node: Node) -> str:
        body = self.compile(node.kids[0])
        params = [self.compile(k) for k in node.kids[1:]]
        if len(params) == 1:
            return f"{params[0]} -> {body}"
        return f"({', '.join(params)}) -> {body}"

    def e_NamedLambdaVariable(self, node: Node) -> str:
        return quote_ident(node.p.get("name", "x"))

    def e_UnresolvedNamedLambdaVariable(self, node: Node) -> str:
        nm = node.p.get("name") or node.p.get("nameParts") or "x"
        if isinstance(nm, list):
            return quote_ident(nm[-1])
        return quote_ident(str(nm))

    def e_ArrayTransform(self, node: Node) -> str:
        return self._call("transform", node.kids)

    def e_ArrayFilter(self, node: Node) -> str:
        return self._call("filter", node.kids)

    def e_ArrayExists(self, node: Node) -> str:
        return self._call("exists", node.kids)

    def e_ArrayForAll(self, node: Node) -> str:
        return self._call("forall", node.kids)

    def e_ArrayAggregate(self, node: Node) -> str:
        return self._call("aggregate", node.kids)

    def e_TransformKeys(self, node: Node) -> str:
        return self._call("transform_keys", node.kids)

    def e_TransformValues(self, node: Node) -> str:
        return self._call("transform_values", node.kids)

    def e_MapFilter(self, node: Node) -> str:
        return self._call("map_filter", node.kids)

    def e_JsonToStructs(self, node: Node) -> str:
        schema = render_type(node.p.get("schema"))
        inner = self.compile(node.kids[0])
        return f"from_json({inner}, '{schema}')"

    def e_GetJsonObject(self, node: Node) -> str:
        return self._call("get_json_object", node.kids)

    # aggregate / window expressions ---------------------------------

    def e_AggregateExpression(self, node: Node) -> str:
        func = self.compile(node.kids[0])
        if node.p.get("isDistinct"):
            # rebuild as ``func(DISTINCT ...)``
            m = re.match(r"^([\w_]+)\((.*)\)$", func, re.DOTALL)
            if m:
                return f"{m.group(1)}(DISTINCT {m.group(2)})"
        return func

    def e_Count(self, node: Node) -> str:
        if not node.kids:
            return "count(*)"
        if (
            len(node.kids) == 1
            and node.kids[0].cls == "Literal"
            and str(node.kids[0].p.get("value")) == "1"
        ):
            return "count(*)"
        args = ", ".join(self.compile(k) for k in node.kids)
        return f"count({args})"

    _NO_FRAME_WINDOW_FUNCS = {
        "Lag", "Lead", "RowNumber", "Rank", "DenseRank",
        "PercentRank", "CumeDist", "Ntile", "NthValue",
    }

    def _inner_window_func_cls(self, n: Node) -> str:
        cur = n
        while cur.cls in {"AggregateExpression", "Alias"} and cur.kids:
            cur = cur.kids[0]
        return cur.cls

    def e_WindowExpression(self, node: Node) -> str:
        func = self.compile(node.kids[0])
        func_cls = self._inner_window_func_cls(node.kids[0])
        prev_no_frame = getattr(self, "_no_frame", False)
        self._no_frame = func_cls in self._NO_FRAME_WINDOW_FUNCS
        try:
            spec = self.compile(node.kids[1])
        finally:
            self._no_frame = prev_no_frame
        return f"{func} OVER {spec}"

    def e_WindowSpecDefinition(self, node: Node) -> str:
        partition = node.p.get("partitionSpec", [])
        order = node.p.get("orderSpec", [])
        kids = node.kids
        # children order: partition expressions, then order specs, then frame
        n_part = len(partition) if isinstance(partition, list) else 0
        n_ord = len(order) if isinstance(order, list) else 0
        idx = 0
        parts: List[str] = []
        if n_part:
            ps = [self.compile(kids[i]) for i in range(idx, idx + n_part)]
            idx += n_part
            parts.append("PARTITION BY " + ", ".join(ps))
        if n_ord:
            os_ = [self.compile(kids[i]) for i in range(idx, idx + n_ord)]
            idx += n_ord
            parts.append("ORDER BY " + ", ".join(os_))
        if idx < len(kids) and not getattr(self, "_no_frame", False):
            frame = self.compile(kids[idx])
            if frame:
                parts.append(frame)
        return "(" + " ".join(parts) + ")"

    def e_SpecifiedWindowFrame(self, node: Node) -> str:
        ftype = node.p.get("frameType", {})
        if isinstance(ftype, dict):
            kind = ftype.get("object", "").rsplit(".", 1)[-1].rstrip("$")
        else:
            kind = str(ftype)
        kind_word = "ROWS" if kind == "RowFrame" else "RANGE"
        lower = self._frame_bound(node.kids[0]) if len(node.kids) > 0 else ""
        upper = self._frame_bound(node.kids[1]) if len(node.kids) > 1 else ""
        return f"{kind_word} BETWEEN {lower} AND {upper}"

    def _frame_bound(self, node: Node) -> str:
        cls = node.cls
        if cls == "UnboundedPreceding":
            return "UNBOUNDED PRECEDING"
        if cls == "UnboundedFollowing":
            return "UNBOUNDED FOLLOWING"
        if cls == "CurrentRow":
            return "CURRENT ROW"
        if cls == "Literal":
            v = node.p.get("value")
            try:
                n = int(v)
            except (TypeError, ValueError):
                return self.compile(node)
            if n < 0:
                return f"{abs(n)} PRECEDING"
            if n > 0:
                return f"{n} FOLLOWING"
            return "CURRENT ROW"
        return self.compile(node)

    def e_UnboundedPreceding(self, node: Node) -> str:
        return "UNBOUNDED PRECEDING"

    def e_UnboundedFollowing(self, node: Node) -> str:
        return "UNBOUNDED FOLLOWING"

    def e_CurrentRow(self, node: Node) -> str:
        return "CURRENT ROW"

    def e_SortOrder(self, node: Node) -> str:
        inner = self.compile(node.kids[0])
        direction = node.p.get("direction", {})
        d = direction.get("object", "") if isinstance(direction, dict) else ""
        d_short = d.rsplit(".", 1)[-1].rstrip("$")
        order_word = "DESC" if d_short.startswith("Descending") else "ASC"
        nulls = node.p.get("nullOrdering", {})
        n = nulls.get("object", "") if isinstance(nulls, dict) else ""
        n_short = n.rsplit(".", 1)[-1].rstrip("$")
        nulls_word = "NULLS LAST" if n_short.startswith("NullsLast") else "NULLS FIRST"
        return f"{inner} {order_word} {nulls_word}"

    def e_Lag(self, node: Node) -> str:
        return self._call("lag", node.kids)

    def e_Lead(self, node: Node) -> str:
        return self._call("lead", node.kids)

    def e_Rank(self, node: Node) -> str:
        return "rank()"

    def e_DenseRank(self, node: Node) -> str:
        return "dense_rank()"

    def e_RowNumber(self, node: Node) -> str:
        return "row_number()"

    def e_PercentRank(self, node: Node) -> str:
        return "percent_rank()"

    def e_CumeDist(self, node: Node) -> str:
        return "cume_dist()"


# ---------------------------------------------------------------------------
# Plan compilation
# ---------------------------------------------------------------------------


@dataclass
class Select:
    ctes: List[Tuple[str, str]] = field(default_factory=list)
    distinct: bool = False
    select_list: Optional[List[str]] = None  # None means SELECT *
    from_clause: Optional[str] = None
    where: List[str] = field(default_factory=list)
    group_by: Optional[List[str]] = None
    grouping_kind: str = "GROUP BY"  # ``GROUP BY``/``ROLLUP``/``CUBE``/``GROUPING SETS``
    grouping_sets: Optional[List[List[str]]] = None
    having: List[str] = field(default_factory=list)
    order_by: Optional[List[str]] = None
    limit: Optional[str] = None
    set_op_sql: Optional[str] = None  # raw SQL for set-op queries
    has_aggregate: bool = False  # set when an Aggregate has been compiled

    # ----- rendering --------------------------------------------------

    def render(self) -> str:
        if self.set_op_sql is not None:
            sql = self.set_op_sql
            if self.order_by:
                sql += "\nORDER BY " + ", ".join(self.order_by)
            if self.limit is not None:
                sql += f"\nLIMIT {self.limit}"
            if self.ctes:
                cte_sql = ",\n".join(f"{n} AS (\n{q}\n)" for n, q in self.ctes)
                sql = f"WITH {cte_sql}\n{sql}"
            return sql

        parts: List[str] = []
        if self.ctes:
            cte_sql = ",\n".join(f"{n} AS (\n{q}\n)" for n, q in self.ctes)
            parts.append(f"WITH {cte_sql}")

        sel = "*" if self.select_list is None else ", ".join(self.select_list)
        if self.distinct:
            parts.append(f"SELECT DISTINCT {sel}")
        else:
            parts.append(f"SELECT {sel}")
        if self.from_clause:
            parts.append(f"FROM {self.from_clause}")
        if self.where:
            parts.append("WHERE " + " AND ".join(self.where))
        if self.group_by is not None:
            kind = self.grouping_kind
            if kind == "GROUPING SETS" and self.grouping_sets is not None:
                rendered = ", ".join(
                    "(" + ", ".join(s) + ")" for s in self.grouping_sets
                )
                parts.append(f"GROUP BY GROUPING SETS ({rendered})")
            elif kind in {"ROLLUP", "CUBE"}:
                parts.append(f"GROUP BY {kind}({', '.join(self.group_by)})")
            else:
                if self.group_by:
                    parts.append("GROUP BY " + ", ".join(self.group_by))
        if self.having:
            parts.append("HAVING " + " AND ".join(self.having))
        if self.order_by:
            parts.append("ORDER BY " + ", ".join(self.order_by))
        if self.limit is not None:
            parts.append(f"LIMIT {self.limit}")
        return "\n".join(parts)

    # ----- predicates -------------------------------------------------

    def is_simple_passthrough(self) -> bool:
        """True if this select adds no clauses beyond a bare FROM."""
        return (
            self.select_list is None
            and not self.distinct
            and not self.where
            and self.group_by is None
            and not self.having
            and not self.order_by
            and self.limit is None
            and self.set_op_sql is None
            and not self.has_aggregate
        )


def _wrap(sel: Select, alias: Optional[str] = None) -> str:
    sql = sel.render()
    inner = sql.replace("\n", "\n  ")
    if alias:
        return f"(\n  {inner}\n) {quote_ident(alias)}"
    return f"(\n  {inner}\n)"


def _is_terminal(sel: Select) -> bool:
    """A terminal select is one that already has a SELECT list / WHERE /
    GROUP BY / ORDER BY / LIMIT / DISTINCT applied -- additional projections
    or filters cannot be merged at the same level."""
    return not sel.is_simple_passthrough()


# ---------- main compiler ----------------------------------------------------


@dataclass
class CTERef:
    name: str
    sql: str


class PlanCompiler:
    def __init__(self) -> None:
        self.cte_defs: Dict[int, CTERef] = {}
        self.expr = ExprCompiler()

    # ---- entry --------------------------------------------------------

    def compile(self, root: Node) -> str:
        cls = root.cls
        method = getattr(self, f"c_{cls}", None)
        if method:
            result = method(root)
            if isinstance(result, Select):
                return result.render()
            return str(result)
        # Fallback: compile as Select
        sel = self._as_select(root)
        return sel.render()

    # ---- helpers ------------------------------------------------------

    def _as_select(self, node: Node) -> Select:
        method = getattr(self, f"s_{node.cls}", None)
        if method:
            return method(node)
        # Last resort: emit a placeholder
        return Select(
            select_list=None,
            from_clause=f"/* unsupported: {node.cls} */",
        )

    def _terminal_to_subquery(self, sel: Select, alias: Optional[str]) -> str:
        return _wrap(sel, alias)

    def _expr_chain(self, chain: Sequence[Dict[str, Any]]) -> str:
        return self.expr.compile_chain(chain)

    def _expr(self, node: Node) -> str:
        return self.expr.compile(node)

    # ---- root commands ------------------------------------------------

    def c_CreateNamespace(self, node: Node) -> str:
        ns = self._namespace_from_resolved(node.kids[0])
        if_not = " IF NOT EXISTS" if node.p.get("ifNotExists") else ""
        name = ".".join(quote_ident(p) for p in ns) if ns else "/*missing*/"
        return f"CREATE DATABASE{if_not} {name};"

    def c_DropNamespace(self, node: Node) -> str:
        ns = self._namespace_from_resolved(node.kids[0]) if node.kids else []
        if_ex = " IF EXISTS" if node.p.get("ifExists") else ""
        cascade = " CASCADE" if node.p.get("cascade") else ""
        name = ".".join(quote_ident(p) for p in ns) if ns else "/*missing*/"
        return f"DROP DATABASE{if_ex} {name}{cascade};"

    def c_SetCatalogAndNamespace(self, node: Node) -> str:
        ns = self._namespace_from_resolved(node.kids[0]) if node.kids else []
        catalog = node.p.get("catalog")
        path: List[str] = []
        if catalog:
            path.append(catalog)
        path.extend(ns)
        if not path:
            return "USE /*missing*/;"
        return f"USE {'.'.join(quote_ident(p) for p in path)};"

    def _namespace_from_resolved(self, node: Node) -> List[str]:
        if node.cls == "ResolvedNamespace":
            return parse_namespace(node.p.get("namespace"))
        # Recurse if wrapped
        for k in node.kids:
            ns = self._namespace_from_resolved(k)
            if ns:
                return ns
        return []

    def c_CreateDataSourceTableCommand(self, node: Node) -> str:
        return self._render_create_table(node.p.get("table") or {},
                                          node.p.get("ignoreIfExists"))

    def c_CreateTableCommand(self, node: Node) -> str:
        return self._render_create_table(node.p.get("table") or {},
                                          node.p.get("ignoreIfExists"))

    def c_CreateTable(self, node: Node) -> str:
        # newer Spark variant uses ``CreateTable`` LogicalPlan with name child
        ident = self._table_ident_from_resolved(
            node.kids[0] if node.kids else None)
        schema = node.p.get("tableSchema") or node.p.get("schema") or {}
        if isinstance(schema, dict):
            cols = self._render_columns(schema.get("fields", []))
        else:
            cols = "/* unknown schema */"
        provider = node.p.get("provider", "parquet")
        return (
            f"CREATE TABLE {ident} (\n  {cols}\n) USING {provider};"
        )

    def _table_ident_from_resolved(self, node: Optional[Node]) -> str:
        if node is None:
            return "/*missing*/"
        if node.cls == "ResolvedIdentifier":
            ident = node.p.get("identifier")
            if isinstance(ident, dict):
                parts = [
                    p
                    for p in [ident.get("namespace"), ident.get("name")]
                    if p
                ]
                if parts:
                    return ".".join(quote_ident(p) for p in parts)
        return "/*missing*/"

    def _render_create_table(self, table: Dict[str, Any], ignore: bool) -> str:
        ident = table.get("identifier", {}) if isinstance(table, dict) else {}
        name_parts: List[str] = []
        for k in ("catalog", "database", "table"):
            v = ident.get(k)
            if v:
                name_parts.append(v)
        # SQL convention is db.table without catalog; keep just the table for
        # readability.
        name = quote_ident(name_parts[-1]) if name_parts else "/*missing*/"
        schema = table.get("schema") or {}
        cols = self._render_columns(schema.get("fields", []))
        provider = table.get("provider", "parquet")
        if_not = " IF NOT EXISTS" if ignore else ""
        return (
            f"CREATE TABLE{if_not} {name} (\n  {cols}\n) USING {provider};"
        )

    def _render_columns(self, fields: Sequence[Dict[str, Any]]) -> str:
        rendered = []
        for f in fields:
            rendered.append(
                f"{quote_ident(f.get('name', ''))} {render_type(f.get('type'))}"
            )
        return ",\n  ".join(rendered)

    def c_DropTable(self, node: Node) -> str:
        ident = self._table_ident_from_resolved(
            node.kids[0] if node.kids else None)
        if_ex = " IF EXISTS" if node.p.get("ifExists") else ""
        purge = " PURGE" if node.p.get("purge") else ""
        return f"DROP TABLE{if_ex} {ident}{purge};"

    def c_DropTableCommand(self, node: Node) -> str:
        ti = node.p.get("tableName") or {}
        is_view = node.p.get("isView")
        if_ex = " IF EXISTS" if node.p.get("ifExists") else ""
        purge = " PURGE" if node.p.get("purge") else ""
        # Keep only the table name to mirror typical USE-database scripts.
        name = ti.get("table")
        rendered = quote_ident(name) if name else "/*missing*/"
        kind = "VIEW" if is_view else "TABLE"
        if is_view:
            purge = ""
        return f"DROP {kind}{if_ex} {rendered}{purge};"

    def c_DropView(self, node: Node) -> str:
        ident = self._table_ident_from_resolved(
            node.kids[0] if node.kids else None)
        if_ex = " IF EXISTS" if node.p.get("ifExists") else ""
        return f"DROP VIEW{if_ex} {ident};"

    def c_CreateViewCommand(self, node: Node) -> str:
        name_obj = node.p.get("name") or {}
        if isinstance(name_obj, dict):
            tbl = name_obj.get("table") or "view"
            db = name_obj.get("database")
            view_name = (
                f"{quote_ident(db)}.{quote_ident(tbl)}" if db else quote_ident(tbl)
            )
        else:
            view_name = "view"
        view_type = node.p.get("viewType", {})
        vt = view_type.get("object", "") if isinstance(view_type, dict) else ""
        is_temp = "TempView" in vt
        is_global = "Global" in vt
        replace = node.p.get("replace")
        kind = []
        if replace:
            kind.append("CREATE OR REPLACE")
        else:
            allow_existing = node.p.get("allowExisting")
            if allow_existing:
                kind.append("CREATE")
            else:
                kind.append("CREATE")
        if is_global:
            kind.append("GLOBAL")
        if is_temp:
            kind.append("TEMP")
        kind.append("VIEW")
        prefix = " ".join(kind)
        # Body
        body_sql: str
        if node.kids:
            body_sql = self.compile(node.kids[0])
            body_sql = body_sql.rstrip(";")
        else:
            original = node.p.get("originalText")
            if original:
                body_sql = original.rstrip(";")
            else:
                body_sql = "SELECT NULL"
        return f"{prefix} {view_name} AS\n{body_sql};"

    def c_CacheTable(self, node: Node) -> str:
        # not always serialized; best effort
        ident = node.p.get("multipartIdentifier") or node.p.get("table") or ""
        if isinstance(ident, list):
            name = ".".join(quote_ident(x) for x in ident)
        else:
            name = quote_ident(str(ident))
        return f"CACHE TABLE {name};"

    def c_UncacheTable(self, node: Node) -> str:
        ident = node.p.get("multipartIdentifier") or node.p.get("table") or ""
        if isinstance(ident, list):
            name = ".".join(quote_ident(x) for x in ident)
        else:
            name = quote_ident(str(ident))
        return f"UNCACHE TABLE {name};"

    def c_InsertIntoHadoopFsRelationCommand(self, node: Node) -> str:
        catalog_table = node.p.get("catalogTable") or {}
        ident = catalog_table.get("identifier") or {}
        name = ident.get("table") or "/*missing*/"
        body = self.compile(node.kids[0]) if node.kids else "SELECT NULL"
        body = body.rstrip(";")
        # If the body is purely a Project on top of LocalRelation (i.e. a
        # ``VALUES`` clause), the underlying row data is intentionally not
        # captured by Spark's serialization.  We render it as INSERT ... SELECT
        # so the SQL stays well-formed.
        return f"INSERT INTO {quote_ident(name)}\n{body};"

    def c_InsertIntoStatement(self, node: Node) -> str:
        return self.c_InsertIntoHadoopFsRelationCommand(node)

    def c_InsertIntoDataSourceCommand(self, node: Node) -> str:
        return self.c_InsertIntoHadoopFsRelationCommand(node)

    def c_AppendData(self, node: Node) -> str:
        return self.c_InsertIntoHadoopFsRelationCommand(node)

    # WithCTE handled also as Select compiler entry
    def c_WithCTE(self, node: Node) -> Select:
        return self.s_WithCTE(node)

    # ---- query plan -> Select ----------------------------------------

    def s_OneRowRelation(self, node: Node) -> Select:
        return Select(select_list=None, from_clause=None)

    def s_LocalRelation(self, node: Node) -> Select:
        # Spark's serialization frequently strips the literal rows from a
        # ``LocalRelation``.  Without them we cannot rebuild the original
        # ``VALUES`` clause; emit a dummy that yields one NULL row per output
        # column so the surrounding query stays valid.
        outputs = node.p.get("output", [])
        cols = []
        for o in outputs:
            try:
                col_name = o[0]["name"]
            except Exception:
                col_name = "col"
            cols.append(f"NULL AS {quote_ident(col_name)}")
        if not cols:
            cols = ["NULL AS col"]
        return Select(select_list=cols, from_clause=None)

    def s_LogicalRelation(self, node: Node) -> Select:
        ct = node.p.get("catalogTable") or {}
        ident = ct.get("identifier") or {}
        name = ident.get("table") or "/*table*/"
        return Select(select_list=None, from_clause=quote_ident(name))

    def s_DataSourceV2Relation(self, node: Node) -> Select:
        ident = node.p.get("identifier") or {}
        if isinstance(ident, dict):
            ns = ident.get("namespace") or []
            nm = ident.get("name") or "/*table*/"
            if isinstance(ns, list) and ns:
                full = ".".join([*ns, nm])
            else:
                full = nm
        else:
            full = str(ident)
        return Select(select_list=None, from_clause=quote_ident(full))

    def s_UnresolvedRelation(self, node: Node) -> Select:
        ident = node.p.get("multipartIdentifier", [])
        if isinstance(ident, list) and ident:
            name = ".".join(quote_ident(x) for x in ident)
        else:
            name = "/*table*/"
        return Select(select_list=None, from_clause=name)

    def s_SubqueryAlias(self, node: Node) -> Select:
        ident = node.p.get("identifier") or {}
        alias = ident.get("name") if isinstance(ident, dict) else str(ident)
        child = self._as_select(node.kids[0])
        if (
            child.is_simple_passthrough()
            and child.from_clause
            and "(" not in child.from_clause
        ):
            # Bare table -- attach alias only if it differs from the table.
            base = child.from_clause.strip("`")
            if alias and alias != base:
                child.from_clause = f"{child.from_clause} {quote_ident(alias)}"
            return child
        # Wrap as subquery alias.
        wrapped = self._terminal_to_subquery(child, alias)
        return Select(select_list=None, from_clause=wrapped, ctes=child.ctes)

    def s_Project(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        proj_list = node.p.get("projectList", [])
        sql_items: List[str] = []
        for chain in proj_list:
            sql_items.append(self._expr_chain(chain))
        if (
            child.set_op_sql is None
            and child.select_list is None
            and not child.has_aggregate
            and child.order_by is None
            and child.limit is None
            and not child.distinct
        ):
            child.select_list = sql_items
            return child
        # When the outer Project is just a re-projection (all bare
        # AttributeReferences) of columns the child already exposes, we want
        # to fold the projection into the child rather than wrap it in a
        # subquery.  Wrapping would either drop alias bindings (`s.col` would
        # no longer resolve) or force qualifier rewriting -- both fragile.
        if (
            child.set_op_sql is None
            and not child.has_aggregate
            and not child.distinct
            and self._all_attribute_refs(proj_list)
        ):
            folded = self._fold_attribute_projection(child, proj_list, sql_items)
            if folded is not None:
                return folded
        # otherwise wrap the child as derived table
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(
            ctes=child.ctes,
            select_list=sql_items,
            from_clause=wrapped,
        )

    def _fold_attribute_projection(
        self,
        child: "Select",
        proj_list: Sequence[List[Dict[str, Any]]],
        sql_items: Sequence[str],
    ) -> Optional["Select"]:
        """Try to satisfy the outer Project by reusing items from the child's
        SELECT list (matching by output column name).  Returns the updated
        child Select if successful, or ``None`` to indicate the caller should
        fall back to wrapping."""

        inner_items = list(child.select_list) if child.select_list is not None else []
        inner_has_star = (child.select_list is None) or any(
            item.strip() == "*" for item in inner_items
        )
        name_to_item: Dict[str, str] = {}
        for item in inner_items:
            nm = _column_name_of(item)
            if nm and nm not in name_to_item:
                name_to_item[nm] = item
        new_items: List[str] = []
        for chain, raw in zip(proj_list, sql_items):
            head = chain[0]
            nm = head.get("name")
            if nm in name_to_item:
                new_items.append(name_to_item[nm])
            elif inner_has_star:
                # Underlying scope still exposes the column; emit it directly.
                new_items.append(raw)
            else:
                return None
        child.select_list = new_items
        return child

    @staticmethod
    def _all_attribute_refs(chains: Sequence[List[Dict[str, Any]]]) -> bool:
        for chain in chains:
            if not chain:
                return False
            head = chain[0]
            cls = str(head.get("class", "")).rsplit(".", 1)[-1].rstrip("$")
            if cls != "AttributeReference":
                return False
        return True

    @staticmethod
    def _is_alias_dup_passthrough(
        proj_list: Sequence[List[Dict[str, Any]]],
    ) -> bool:
        """A 'Spark internal' Project that exists only to relabel columns
        before an Expand: every chain is either an AttributeReference or an
        Alias-of-AttributeReference where the alias name equals the source
        column name, AND there is at least one duplicate output name.

        Such a Project would otherwise produce ambiguous columns (e.g.
        two ``dept_id`` outputs); skip it instead."""

        seen: Dict[str, int] = {}
        has_alias_self = False
        for chain in proj_list:
            if not chain:
                return False
            head = chain[0]
            cls = str(head.get("class", "")).rsplit(".", 1)[-1].rstrip("$")
            if cls == "AttributeReference":
                nm = head.get("name", "")
            elif cls == "Alias":
                if len(chain) < 2:
                    return False
                inner = chain[1]
                inner_cls = str(inner.get("class", "")).rsplit(".", 1)[-1].rstrip("$")
                if inner_cls != "AttributeReference":
                    return False
                if head.get("name") != inner.get("name"):
                    return False
                nm = head.get("name", "")
                has_alias_self = True
            else:
                return False
            seen[nm] = seen.get(nm, 0) + 1
        return has_alias_self and any(c > 1 for c in seen.values())

    def _skip_synthetic_aggregate_project(self, node: Node) -> Node:
        """Skip wrappers around an Expand-input that are pure relabelling."""

        cur = node
        while cur.cls == "Project" and self._is_alias_dup_passthrough(
            cur.p.get("projectList", [])
        ):
            if not cur.kids:
                break
            cur = cur.kids[0]
        return cur

    def s_Filter(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        cond_sql = self._expr_chain(node.p.get("condition", []))
        if child.order_by is None and child.limit is None and not child.distinct:
            if child.has_aggregate or child.group_by is not None:
                child.having.append(cond_sql)
            else:
                if child.select_list is not None and child.where:
                    # Already projected; don't push WHERE through projection
                    wrapped = self._terminal_to_subquery(child, alias=None)
                    return Select(
                        ctes=child.ctes,
                        from_clause=wrapped,
                        where=[cond_sql],
                    )
                if child.select_list is not None:
                    # Filter on already projected child -> wrap to keep semantics
                    wrapped = self._terminal_to_subquery(child, alias=None)
                    return Select(
                        ctes=child.ctes,
                        from_clause=wrapped,
                        where=[cond_sql],
                    )
                child.where.append(cond_sql)
            return child
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(
            ctes=child.ctes,
            from_clause=wrapped,
            where=[cond_sql],
        )

    def s_Sort(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        orders = [self._expr_chain(c) for c in node.p.get("order", [])]
        if child.order_by is None and child.limit is None and child.set_op_sql is None:
            child.order_by = orders
            return child
        if child.set_op_sql is not None and child.order_by is None and child.limit is None:
            child.order_by = orders
            return child
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(
            ctes=child.ctes,
            from_clause=wrapped,
            order_by=orders,
        )

    def s_GlobalLimit(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        # Limit expression is the second child of GlobalLimit/LocalLimit
        limit_expr = node.p.get("limitExpr")
        if isinstance(limit_expr, list):
            lim_sql = self._expr_chain(limit_expr)
        elif node.kids and len(node.kids) == 1 and child is None:
            lim_sql = "1"
        else:
            # Try sibling chain stored as ``limit`` field
            lim_sql = ""
            cand = node.p.get("limit")
            if isinstance(cand, list):
                lim_sql = self._expr_chain(cand)
        if not lim_sql:
            # Fall back to walking the inline expression chain placed after
            # the child plan in some serializations.
            lim_sql = "1"
        if child.limit is None and child.set_op_sql is None:
            child.limit = lim_sql
            return child
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(
            ctes=child.ctes,
            from_clause=wrapped,
            limit=lim_sql,
        )

    def s_LocalLimit(self, node: Node) -> Select:
        return self._as_select(node.kids[0])  # global limit handles it

    def s_Distinct(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        # ``Distinct`` directly on top of a ``UNION ALL`` is equivalent to a
        # plain ``UNION``.  Rewrite the set operation rather than wrapping it
        # in an outer ``SELECT DISTINCT``.
        if (
            child.set_op_sql is not None
            and child.order_by is None
            and child.limit is None
            and "\nUNION ALL\n" in child.set_op_sql
        ):
            child.set_op_sql = child.set_op_sql.replace("\nUNION ALL\n", "\nUNION\n")
            return child
        if (
            child.distinct is False
            and child.order_by is None
            and child.limit is None
            and child.set_op_sql is None
        ):
            child.distinct = True
            return child
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(
            ctes=child.ctes,
            from_clause=wrapped,
            distinct=True,
        )

    def s_Deduplicate(self, node: Node) -> Select:
        return self.s_Distinct(node)

    # ---- aggregate ---------------------------------------------------

    def s_Aggregate(self, node: Node) -> Select:
        child_node = node.kids[0]
        grouping_chains = node.p.get("groupingExpressions", [])
        agg_chains = node.p.get("aggregateExpressions", [])

        # Detect ROLLUP/CUBE/GROUPING SETS pattern: child is Expand and last
        # grouping expression is ``spark_grouping_id``.
        special_grouping: Optional[Tuple[str, List[str], Optional[List[List[str]]]]] = None
        grouping_used = grouping_chains
        effective_child = child_node
        if (
            child_node.cls == "Expand"
            and grouping_chains
            and self._is_grouping_id(grouping_chains[-1])
        ):
            grouping_used = grouping_chains[:-1]
            grp_cols = [self._expr_chain(c) for c in grouping_used]
            sets = self._extract_grouping_sets(child_node, len(grouping_used))
            kind = self._classify_grouping_sets(sets, len(grouping_used))
            special_grouping = (kind, grp_cols, sets if kind == "GROUPING SETS" else None)
            effective_child = self._skip_synthetic_aggregate_project(
                child_node.kids[0]
            )

        child_sel = self._as_select(effective_child)
        if (
            child_sel.select_list is not None
            or child_sel.distinct
            or child_sel.has_aggregate
            or child_sel.group_by is not None
            or child_sel.order_by is not None
            or child_sel.limit is not None
            or child_sel.having
        ):
            wrapped = self._terminal_to_subquery(child_sel, alias=None)
            base = Select(
                ctes=child_sel.ctes,
                from_clause=wrapped,
            )
        else:
            base = child_sel

        if special_grouping is None:
            grp_sql = [self._expr_chain(c) for c in grouping_chains]
            base.group_by = grp_sql
            base.grouping_kind = "GROUP BY"
        else:
            kind, grp_cols, sets = special_grouping
            base.group_by = grp_cols
            base.grouping_kind = kind
            base.grouping_sets = sets

        select_items: List[str] = []
        for chain in agg_chains:
            select_items.append(self._expr_chain(chain))
        # ``Aggregate`` always produces a SELECT list (even GROUP BY without
        # aggregate functions still needs explicit columns).
        if not select_items and base.group_by:
            select_items = list(base.group_by)
        base.select_list = select_items
        base.has_aggregate = True
        return base

    def _is_grouping_id(self, chain: List[Dict[str, Any]]) -> bool:
        if not chain:
            return False
        head = chain[0]
        cls = str(head.get("class", "")).rsplit(".", 1)[-1].rstrip("$")
        return cls == "AttributeReference" and head.get("name") == "spark_grouping_id"

    def _extract_grouping_sets(self, expand: Node, n_group_cols: int) -> List[List[str]]:
        projs = expand.p.get("projections", [])
        sets: List[List[str]] = []
        for proj in projs:
            # Each projection is a list of expression chains.  The last n_group_cols+1
            # entries are: grouping projections (col or NULL) + grouping_id literal.
            group_part = proj[-(n_group_cols + 1) : -1]
            cur: List[str] = []
            for chain in group_part:
                head = chain[0]
                cls = str(head.get("class", "")).rsplit(".", 1)[-1].rstrip("$")
                if cls == "Literal" and head.get("value") is None:
                    continue  # rolled-up
                cur.append(self._expr_chain(chain))
            sets.append(cur)
        return sets

    def _classify_grouping_sets(
        self,
        sets: List[List[str]],
        n: int,
    ) -> str:
        # sort by length descending then by content
        if n == 0:
            return "GROUPING SETS"
        # Build a comparable representation of the sets as tuples of the
        # column names in their original ordering.  We compare to ROLLUP/CUBE
        # patterns derived from the first observed column ordering.
        # ROLLUP(a, b) -> [(a, b), (a), ()]
        # CUBE(a, b) -> [(a, b), (a), (b), ()]
        if not sets:
            return "GROUPING SETS"
        # Use the first non-empty set to determine column order.
        ref = next((s for s in sets if s), [])
        if len(ref) != n:
            return "GROUPING SETS"
        rollup_pattern = [tuple(ref[:i]) for i in range(n, -1, -1)]
        if len(sets) == n + 1 and sorted(map(tuple, sets)) == sorted(rollup_pattern):
            return "ROLLUP"
        # CUBE: 2^n subsets, in any order
        if len(sets) == 2 ** n:
            from itertools import combinations
            cube_pattern = []
            for r in range(n + 1):
                for combo in combinations(ref, r):
                    cube_pattern.append(tuple(combo))
            if sorted(map(tuple, sets)) == sorted(cube_pattern):
                return "CUBE"
        return "GROUPING SETS"

    # ---- joins -------------------------------------------------------

    _JOIN_TYPES = {
        "Inner": "INNER JOIN",
        "Cross": "CROSS JOIN",
        "LeftOuter": "LEFT JOIN",
        "RightOuter": "RIGHT JOIN",
        "FullOuter": "FULL JOIN",
        "LeftSemi": "LEFT SEMI JOIN",
        "LeftAnti": "LEFT ANTI JOIN",
    }

    def s_Join(self, node: Node) -> Select:
        left = self._as_select(node.kids[0])
        right = self._as_select(node.kids[1])
        jt = node.p.get("joinType", {})
        jt_name = jt.get("object", "") if isinstance(jt, dict) else str(jt)
        jt_short = jt_name.rsplit(".", 1)[-1].rstrip("$")
        join_kw = self._JOIN_TYPES.get(jt_short, jt_short.upper() + " JOIN")
        cond_chain = node.p.get("condition", [])
        cond_sql = self._expr_chain(cond_chain) if cond_chain else None

        left_from = self._select_to_from_fragment(left)
        right_from = self._select_to_from_fragment(right)
        on_part = f" ON {cond_sql}" if cond_sql else ""
        from_clause = f"{left_from}\n  {join_kw} {right_from}{on_part}"
        merged_ctes = left.ctes + right.ctes
        return Select(ctes=merged_ctes, from_clause=from_clause)

    def _select_to_from_fragment(self, sel: Select) -> str:
        if sel.is_simple_passthrough() and sel.from_clause:
            return sel.from_clause
        return self._terminal_to_subquery(sel, alias=None)

    # ---- set ops -----------------------------------------------------

    def s_Union(self, node: Node) -> Select:
        return self._set_op(node, "UNION ALL")

    def s_Except(self, node: Node) -> Select:
        return self._set_op(node, "EXCEPT")

    def s_Intersect(self, node: Node) -> Select:
        return self._set_op(node, "INTERSECT")

    def _set_op(self, node: Node, kw: str) -> Select:
        kid_sels = [self._as_select(k) for k in node.kids]
        rendered = []
        ctes: List[Tuple[str, str]] = []
        for s in kid_sels:
            ctes.extend(s.ctes)
            s.ctes = []  # CTEs hoisted to outer
            rendered.append(s.render())
        sep = f"\n{kw}\n"
        sql = sep.join(f"{r}" for r in rendered)
        return Select(ctes=ctes, set_op_sql=sql)

    # ---- distinct around set op -- handled by Distinct above

    # ---- generate (lateral view) ------------------------------------

    def s_Generate(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        gen_chain = node.p.get("generator", [])
        if isinstance(gen_chain, list) and gen_chain:
            gen_node = parse_expr_chain(gen_chain)
            gen_sql = self._expr(gen_node)
        else:
            gen_sql = "/*generator*/"
        outputs = node.p.get("generatorOutput", []) or []
        out_names = []
        for chain in outputs:
            try:
                out_names.append(chain[0].get("name", "col"))
            except Exception:
                out_names.append("col")
        qualifier = node.p.get("qualifier") or "_g"
        # Build a LATERAL VIEW clause and append to FROM.
        cols = ", ".join(quote_ident(n) for n in out_names) or "col"
        lv = f"LATERAL VIEW {gen_sql} {quote_ident(qualifier)} AS {cols}"
        # Take child as plain FROM if possible
        if child.is_simple_passthrough() and child.from_clause:
            new_from = f"{child.from_clause}\n  {lv}"
            return Select(ctes=child.ctes, from_clause=new_from)
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(ctes=child.ctes, from_clause=f"{wrapped}\n  {lv}")

    # ---- expand without grouping -- emit as union of projections ----

    def s_Expand(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        # Without an enclosing Aggregate we can't decode meaningfully -- wrap.
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(ctes=child.ctes, from_clause=wrapped)

    # ---- window ------------------------------------------------------

    def s_Window(self, node: Node) -> Select:
        child = self._as_select(node.kids[0])
        window_chains = node.p.get("windowExpressions", [])
        wexprs = [self._expr_chain(c) for c in window_chains]
        if (
            child.set_op_sql is None
            and child.order_by is None
            and child.limit is None
            and not child.distinct
            and not child.has_aggregate
        ):
            if child.select_list is None:
                child.select_list = ["*"] + wexprs
            else:
                child.select_list = child.select_list + wexprs
            return child
        wrapped = self._terminal_to_subquery(child, alias=None)
        return Select(
            ctes=child.ctes,
            select_list=["*"] + wexprs,
            from_clause=wrapped,
        )

    # ---- CTE ---------------------------------------------------------

    def s_WithCTE(self, node: Node) -> Select:
        cte_defs_indices = node.p.get("cteDefs", [])
        # Children layout in pre-order: the CTERelationDef nodes and finally the
        # main plan are siblings in a flat list.  But after our recursive
        # parsing the children are ordered as CTERelationDef, CTERelationDef,
        # ..., main_plan.
        n_defs = len(node.kids) - 1
        if n_defs < 0:
            n_defs = 0
        cte_pairs: List[Tuple[str, str]] = []
        for i in range(n_defs):
            cte_node = node.kids[i]
            cte_id = cte_node.p.get("id", i)
            # CTERelationDef child[0] is the body (typically SubqueryAlias)
            body_node = cte_node.kids[0]
            # Try to fetch the alias name from the wrapping SubqueryAlias.
            cte_name = self._cte_name(body_node, fallback=f"cte_{cte_id}")
            inner_node = body_node
            if body_node.cls == "SubqueryAlias":
                inner_node = body_node.kids[0]
            inner_sel = self._as_select(inner_node)
            inner_sel.ctes = []
            cte_pairs.append((cte_name, inner_sel.render()))
            self.cte_defs[cte_id] = CTERef(cte_name, inner_sel.render())

        main_node = node.kids[-1] if node.kids else None
        if main_node is None:
            return Select(ctes=cte_pairs)
        main_sel = self._as_select(main_node)
        main_sel.ctes = cte_pairs + main_sel.ctes
        return main_sel

    def _cte_name(self, node: Node, fallback: str) -> str:
        cur = node
        while cur is not None:
            if cur.cls == "SubqueryAlias":
                ident = cur.p.get("identifier") or {}
                nm = ident.get("name") if isinstance(ident, dict) else None
                if nm:
                    return nm
            cur = cur.kids[0] if cur.kids else None
        return fallback

    def s_CTERelationRef(self, node: Node) -> Select:
        cte_id = node.p.get("cteId")
        if cte_id in self.cte_defs:
            return Select(select_list=None,
                          from_clause=quote_ident(self.cte_defs[cte_id].name))
        return Select(select_list=None, from_clause=f"/*cte:{cte_id}*/")

    def s_CTERelationDef(self, node: Node) -> Select:
        return self._as_select(node.kids[0])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


PlanInput = Union[str, bytes, Dict[str, Any], Sequence[Dict[str, Any]]]


def _normalise_plan_input(plan: PlanInput) -> List[Dict[str, Any]]:
    """Coerce any supported plan input into a flat list of node dicts."""

    if isinstance(plan, (str, bytes)):
        plan = json.loads(plan)

    if isinstance(plan, dict):
        # Common wrapping shapes:
        #   {"plan": [...]} / {"logicalPlan": [...]} / {"nodes": [...]}.
        # If none of the well-known wrappers match but there is a single
        # list-valued key, use that; otherwise treat the dict itself as the
        # single root node.
        for key in ("plan", "logicalPlan", "logical_plan", "tree", "nodes"):
            inner = plan.get(key)
            if isinstance(inner, list):
                return list(inner)
        list_values = [v for v in plan.values() if isinstance(v, list)]
        if len(list_values) == 1 and all(
            isinstance(x, dict) for x in list_values[0]
        ):
            return list(list_values[0])
        return [plan]

    if isinstance(plan, Sequence):
        return list(plan)

    raise TypeError(
        f"plan_to_sql: unsupported input type {type(plan).__name__}; "
        "expected dict, list of dicts, or JSON string"
    )


def plan_to_sql(plan: PlanInput) -> str:
    """Convert a Spark logical plan into a SQL string.

    The plan can be supplied in any of the following forms:

    * a JSON-encoded ``str`` / ``bytes`` blob
    * a ``list`` of node dicts (Spark's native pre-order serialization)
    * a single ``dict`` -- either a leaf node, or a wrapper such as
      ``{"plan": [...]}`` / ``{"logicalPlan": [...]}`` / ``{"nodes": [...]}``

    Returns a SQL string terminated with ``;``.  Returns an empty string for
    empty input.

    Example::

        import json
        from plan_to_sql import plan_to_sql

        with open("plan.json") as f:
            sql = plan_to_sql(json.load(f))
    """

    items = _normalise_plan_input(plan)
    if not items:
        return ""
    root = parse_plan(items)
    compiler = PlanCompiler()
    out = compiler.compile(root)
    out = out.strip()
    if not out.endswith(";"):
        out += ";"
    return out


def dict_to_sql(plan: Dict[str, Any]) -> str:
    """Convenience wrapper accepting a single ``dict`` (or wrapper dict).

    Equivalent to :func:`plan_to_sql` but with a narrower signature for
    callers that want explicit ``dict``-typed input."""

    if not isinstance(plan, dict):
        raise TypeError(
            f"dict_to_sql expects a dict, got {type(plan).__name__}"
        )
    return plan_to_sql(plan)
