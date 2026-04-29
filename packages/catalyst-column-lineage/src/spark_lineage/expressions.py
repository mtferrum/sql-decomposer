"""Expression-level evaluator for column lineage.

Given a parsed Catalyst expression tree (`ExprNode`) and an environment
mapping from `ExprId` to `ColInfo` (the lineage of every column already
visible at the surrounding plan node's child), this module computes:

* `sources`:  the set of base-table columns that feed into the expression,
* `text`:     a SQL-like rendering of the expression for human inspection,
* `kind`:     a coarse `TransformationType` classifying the expression as
              IDENTITY / LITERAL / EXPRESSION / AGGREGATE / WINDOW / GROUPING /
              GENERATOR / OUTER_REFERENCE.

Lineage is propagated bottom-up:  every expression is the union of the
sources of its sub-expressions, with leaves contributing either an
attribute lookup against the env or nothing (literals, special markers).

Coverage targets the full Catalyst expression vocabulary that Spark SQL
can emit.  Operators with a known SQL symbol are rendered symbolically
(`a + b`, `a = b`, ...) so the output reads like SQL; everything else
falls back to a generic `FunctionName(arg, arg, ...)` form, which always
yields correct lineage even for expressions we have not whitelisted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from .models import ColInfo, ExprNode, ExprIdKey, SourceColumn, TransformationType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def expr_id_key(d: dict) -> ExprIdKey:
    return (int(d["id"]), str(d.get("jvmId", "")))


def referenced_child(node: ExprNode, field: str) -> Optional[ExprNode]:
    idx = node.data.get(field)
    if not isinstance(idx, int):
        return None
    if 0 <= idx < len(node.children):
        return node.children[idx]
    return None


def referenced_children(node: ExprNode, field: str) -> List[ExprNode]:
    idx = node.data.get(field)
    if isinstance(idx, list):
        out: List[ExprNode] = []
        for i in idx:
            if isinstance(i, int) and 0 <= i < len(node.children):
                out.append(node.children[i])
        return out
    if isinstance(idx, int) and 0 <= idx < len(node.children):
        return [node.children[idx]]
    return []


@dataclass
class ExprResult:
    sources: FrozenSet[SourceColumn]
    text: str
    kind: str


EMPTY_SOURCES: FrozenSet[SourceColumn] = frozenset()


# ---------------------------------------------------------------------------
# Symbolic operators
# ---------------------------------------------------------------------------


_BINARY_OPS: Dict[str, str] = {
    # Arithmetic
    "Add": "+",
    "Subtract": "-",
    "Multiply": "*",
    "Divide": "/",
    "IntegralDivide": "DIV",
    "Remainder": "%",
    "Pmod": "PMOD",
    "BitwiseAnd": "&",
    "BitwiseOr": "|",
    "BitwiseXor": "^",
    "ShiftLeft": "<<",
    "ShiftRight": ">>",
    "ShiftRightUnsigned": ">>>",
    # Comparison
    "EqualTo": "=",
    "EqualNullSafe": "<=>",
    "GreaterThan": ">",
    "GreaterThanOrEqual": ">=",
    "LessThan": "<",
    "LessThanOrEqual": "<=",
    # Logical
    "And": "AND",
    "Or": "OR",
    # Other
    "Concat": "||",
}


_UNARY_OPS: Dict[str, str] = {
    "Not": "NOT",
    "UnaryMinus": "-",
    "UnaryPositive": "+",
    "BitwiseNot": "~",
}


_UNARY_POSTFIX: Dict[str, str] = {
    "IsNull": "IS NULL",
    "IsNotNull": "IS NOT NULL",
    "IsNaN": "IS NAN",
    "IsTrue": "IS TRUE",
    "IsFalse": "IS FALSE",
}


# Aggregate function classes (under expressions.aggregate.*)
_AGGREGATE_FUNCS = {
    "Count",
    "CountIf",
    "Sum",
    "Average",
    "Min",
    "Max",
    "First",
    "Last",
    "FirstValue",
    "LastValue",
    "CollectList",
    "CollectSet",
    "ApproxCountDistinct",
    "HyperLogLogPlusPlus",
    "VariancePop",
    "VarianceSamp",
    "StddevPop",
    "StddevSamp",
    "Skewness",
    "Kurtosis",
    "Corr",
    "CovPopulation",
    "CovSample",
    "Percentile",
    "PercentileApprox",
    "PercentileDisc",
    "PercentileCont",
    "Median",
    "Mode",
    "RegrAvgX",
    "RegrAvgY",
    "RegrCount",
    "RegrIntercept",
    "RegrR2",
    "RegrSlope",
    "RegrSXX",
    "RegrSXY",
    "RegrSYY",
    "BoolAnd",
    "BoolOr",
    "EveryAgg",
    "AnyAgg",
    "SomeAgg",
    "Grouping",
    "GroupingID",
    "BitAndAgg",
    "BitOrAgg",
    "BitXorAgg",
    "AnyValue",
}


# Window-only functions (excluding aggregates wrapped via WindowExpression)
_WINDOW_FUNCS = {
    "RowNumber",
    "Rank",
    "DenseRank",
    "PercentRank",
    "CumeDist",
    "Ntile",
    "Lag",
    "Lead",
    "NthValue",
    "FirstValue",
    "LastValue",
}


_FRAME_BOUND_OBJECTS = {
    "UnboundedPreceding": "UNBOUNDED PRECEDING",
    "UnboundedFollowing": "UNBOUNDED FOLLOWING",
    "CurrentRow": "CURRENT ROW",
}


def _aggregate_func_name(short_class: str) -> str:
    mapping = {
        "Average": "AVG",
        "ApproxCountDistinct": "APPROX_COUNT_DISTINCT",
        "HyperLogLogPlusPlus": "APPROX_COUNT_DISTINCT",
        "VariancePop": "VAR_POP",
        "VarianceSamp": "VAR_SAMP",
        "StddevPop": "STDDEV_POP",
        "StddevSamp": "STDDEV_SAMP",
        "CovPopulation": "COVAR_POP",
        "CovSample": "COVAR_SAMP",
        "PercentileApprox": "PERCENTILE_APPROX",
        "GroupingID": "GROUPING_ID",
        "BoolAnd": "BOOL_AND",
        "BoolOr": "BOOL_OR",
        "EveryAgg": "EVERY",
        "AnyAgg": "ANY",
        "SomeAgg": "SOME",
        "BitAndAgg": "BIT_AND",
        "BitOrAgg": "BIT_OR",
        "BitXorAgg": "BIT_XOR",
        "CollectList": "COLLECT_LIST",
        "CollectSet": "COLLECT_SET",
        "AnyValue": "ANY_VALUE",
        "FirstValue": "FIRST_VALUE",
        "LastValue": "LAST_VALUE",
        "RowNumber": "ROW_NUMBER",
        "DenseRank": "DENSE_RANK",
        "PercentRank": "PERCENT_RANK",
        "CumeDist": "CUME_DIST",
        "NthValue": "NTH_VALUE",
    }
    if short_class in mapping:
        return mapping[short_class]
    return _camel_to_snake(short_class).upper()


def _generic_func_name(short_class: str) -> str:
    """Map Catalyst camel-case class names to UPPER_SNAKE SQL function names."""

    mapping = {
        "DateAdd": "DATE_ADD",
        "DateSub": "DATE_SUB",
        "DateDiff": "DATEDIFF",
        "AddMonths": "ADD_MONTHS",
        "MonthsBetween": "MONTHS_BETWEEN",
        "TruncTimestamp": "DATE_TRUNC",
        "TruncDate": "TRUNC",
        "ParseToDate": "TO_DATE",
        "ParseToTimestamp": "TO_TIMESTAMP",
        "DayOfMonth": "DAY",
        "DayOfWeek": "DAYOFWEEK",
        "DayOfYear": "DAYOFYEAR",
        "WeekOfYear": "WEEKOFYEAR",
        "DayName": "DAYNAME",
        "MonthName": "MONTHNAME",
        "CurrentDate": "CURRENT_DATE",
        "CurrentTimestamp": "CURRENT_TIMESTAMP",
        "Now": "NOW",
        "FromUnixTime": "FROM_UNIXTIME",
        "UnixTimestamp": "UNIX_TIMESTAMP",
        "ToUnixTimestamp": "TO_UNIX_TIMESTAMP",
        "DateFormatClass": "DATE_FORMAT",
        "TimeAdd": "+",
        "TimeSub": "-",
        "DatetimeSub": "-",
        "DatetimeAdd": "+",
        "ExtractANSIIntervalDays": "EXTRACT_DAYS",
        "ElementAt": "ELEMENT_AT",
        "GetArrayItem": "[]",
        "GetMapValue": "[]",
        "MapKeys": "MAP_KEYS",
        "MapValues": "MAP_VALUES",
        "MapEntries": "MAP_ENTRIES",
        "MapFromArrays": "MAP_FROM_ARRAYS",
        "MapFromEntries": "MAP_FROM_ENTRIES",
        "ArrayContains": "ARRAY_CONTAINS",
        "ArrayDistinct": "ARRAY_DISTINCT",
        "ArrayUnion": "ARRAY_UNION",
        "ArrayIntersect": "ARRAY_INTERSECT",
        "ArrayExcept": "ARRAY_EXCEPT",
        "ArrayJoin": "ARRAY_JOIN",
        "ArrayMax": "ARRAY_MAX",
        "ArrayMin": "ARRAY_MIN",
        "ArraySort": "ARRAY_SORT",
        "ArrayPosition": "ARRAY_POSITION",
        "ArrayRemove": "ARRAY_REMOVE",
        "ArrayRepeat": "ARRAY_REPEAT",
        "ArraysZip": "ARRAYS_ZIP",
        "ArraysOverlap": "ARRAYS_OVERLAP",
        "ArrayTransform": "TRANSFORM",
        "ArrayFilter": "FILTER",
        "ArrayExists": "EXISTS",
        "ArrayForAll": "FORALL",
        "ArrayAggregate": "AGGREGATE",
        "MapFilter": "MAP_FILTER",
        "TransformKeys": "TRANSFORM_KEYS",
        "TransformValues": "TRANSFORM_VALUES",
        "MapZipWith": "MAP_ZIP_WITH",
        "ZipWith": "ZIP_WITH",
        "Size": "SIZE",
        "ArraySize": "ARRAY_SIZE",
        "MapSize": "SIZE",
        "Reverse": "REVERSE",
        "Slice": "SLICE",
        "Sequence": "SEQUENCE",
        "Flatten": "FLATTEN",
        "Concat": "CONCAT",
        "ConcatWs": "CONCAT_WS",
        "JsonToStructs": "FROM_JSON",
        "StructsToJson": "TO_JSON",
        "GetJsonObject": "GET_JSON_OBJECT",
        "JsonTuple": "JSON_TUPLE",
        "JsonArrayLength": "JSON_ARRAY_LENGTH",
        "JsonObjectKeys": "JSON_OBJECT_KEYS",
        "CreateNamedStruct": "STRUCT",
        "CreateStruct": "STRUCT",
        "CreateArray": "ARRAY",
        "CreateMap": "MAP",
        "Coalesce": "COALESCE",
        "IfNull": "IFNULL",
        "Nvl": "NVL",
        "Nvl2": "NVL2",
        "NullIf": "NULLIF",
        "Greatest": "GREATEST",
        "Least": "LEAST",
        "If": "IF",
        "RegExpReplace": "REGEXP_REPLACE",
        "RegExpExtract": "REGEXP_EXTRACT",
        "RegExpExtractAll": "REGEXP_EXTRACT_ALL",
        "Like": "LIKE",
        "RLike": "RLIKE",
        "ILike": "ILIKE",
        "Substring": "SUBSTRING",
        "SubstringIndex": "SUBSTRING_INDEX",
        "StringTrim": "TRIM",
        "StringTrimLeft": "LTRIM",
        "StringTrimRight": "RTRIM",
        "Lower": "LOWER",
        "Upper": "UPPER",
        "InitCap": "INITCAP",
        "Length": "LENGTH",
        "OctetLength": "OCTET_LENGTH",
        "BitLength": "BIT_LENGTH",
        "FormatString": "FORMAT_STRING",
        "FormatNumber": "FORMAT_NUMBER",
        "Repeat": "REPEAT",
        "StringReplace": "REPLACE",
        "StringSplit": "SPLIT",
        "Encode": "ENCODE",
        "Decode": "DECODE",
        "Hex": "HEX",
        "Unhex": "UNHEX",
        "Md5": "MD5",
        "Sha1": "SHA1",
        "Sha2": "SHA2",
        "Crc32": "CRC32",
        "Hash": "HASH",
        "XxHash64": "XXHASH64",
    }
    return mapping.get(short_class, _camel_to_snake(short_class).upper())


def _camel_to_snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0 and (
            not name[i - 1].isupper()
            or (i + 1 < len(name) and name[i + 1].islower())
        ):
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


# ---------------------------------------------------------------------------
# Literal rendering
# ---------------------------------------------------------------------------


def _render_literal(node: ExprNode) -> str:
    value = node.data.get("value")
    dt = str(node.data.get("dataType", ""))
    if value is None:
        return "NULL"
    if dt == "string":
        # value is the string itself
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
    if dt.startswith("decimal"):
        return f"{value}BD"
    if dt == "boolean":
        return str(value).upper()
    if dt == "date":
        return f"DATE '{value}'"
    if dt.startswith("timestamp"):
        return f"TIMESTAMP '{value}'"
    if dt.startswith("interval") or dt == "calendarinterval":
        return f"INTERVAL '{value}'"
    return str(value)


# ---------------------------------------------------------------------------
# Main evaluator
# ---------------------------------------------------------------------------


def eval_expr(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    cls = node.short_class

    handler = _DISPATCH.get(cls)
    if handler is not None:
        return handler(node, env)

    # Symbolic dispatch
    if cls in _BINARY_OPS:
        return _eval_binary_op(node, env, _BINARY_OPS[cls])
    if cls in _UNARY_OPS:
        return _eval_unary_prefix(node, env, _UNARY_OPS[cls])
    if cls in _UNARY_POSTFIX:
        return _eval_unary_postfix(node, env, _UNARY_POSTFIX[cls])

    # Aggregates / windows
    if cls in _AGGREGATE_FUNCS or cls.endswith("Agg") or _looks_like_aggregate(node):
        return _eval_aggregate_func(node, env)
    if cls in _WINDOW_FUNCS:
        return _eval_window_func(node, env)

    # Generic fallback
    return _eval_generic(node, env)


def collect_sources(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> FrozenSet[SourceColumn]:
    return eval_expr(node, env).sources


def collect_attr_names(node: ExprNode) -> List[str]:
    """Pre-order list of every AttributeReference name in `node` (debug aid)."""
    names: List[str] = []
    stack = [node]
    while stack:
        n = stack.pop()
        if n.short_class in ("AttributeReference", "OuterReference"):
            names.append(str(n.data.get("name", "")))
        stack.extend(reversed(n.children))
    return names


# ---------------------------------------------------------------------------
# Specific handlers
# ---------------------------------------------------------------------------


def _eval_attribute_reference(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    name = str(node.data.get("name", ""))
    eid_dict = node.data.get("exprId") or {}
    if not isinstance(eid_dict, dict) or "id" not in eid_dict:
        return ExprResult(EMPTY_SOURCES, name, TransformationType.IDENTITY)
    key = expr_id_key(eid_dict)
    info = env.get(key)
    if info is None:
        # Reference not bound in current scope (correlated / unresolved):
        # return the attribute name; sources empty.
        return ExprResult(EMPTY_SOURCES, _qualified_name(node), TransformationType.IDENTITY)
    return ExprResult(info.sources, _qualified_name(node), info.transformation_type)


def _eval_outer_reference(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    # Spark serialises OuterReference's inner NamedExpression in the `e`
    # field as a separate inline expression list (not as a structural
    # child), so num-children is 0 and we have to look it up explicitly.
    e_field = node.data.get("e")
    if isinstance(e_field, list) and e_field:
        from .parser import parse_expr_list

        try:
            inner = parse_expr_list(e_field)
        except ValueError:
            inner = None
        if inner is not None:
            sub = eval_expr(inner, env)
            return ExprResult(sub.sources, sub.text, TransformationType.OUTER_REFERENCE)
    if node.children:
        sub = eval_expr(node.children[0], env)
        return ExprResult(sub.sources, sub.text, TransformationType.OUTER_REFERENCE)
    return ExprResult(EMPTY_SOURCES, "<outer>", TransformationType.OUTER_REFERENCE)


def _eval_literal(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    return ExprResult(EMPTY_SOURCES, _render_literal(node), TransformationType.LITERAL)


def _eval_alias(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, str(node.data.get("name", "")), TransformationType.IDENTITY)
    return eval_expr(node.children[0], env)


def _eval_cast(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "CAST()", TransformationType.EXPRESSION)
    sub = eval_expr(node.children[0], env)
    target = str(node.data.get("dataType", ""))
    return ExprResult(
        sub.sources,
        f"CAST({sub.text} AS {target})",
        sub.kind if sub.kind in (TransformationType.IDENTITY, TransformationType.LITERAL)
        else TransformationType.EXPRESSION,
    )


def _eval_in(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "IN()", TransformationType.EXPRESSION)
    head = eval_expr(node.children[0], env)
    rest = [eval_expr(c, env) for c in node.children[1:]]
    sources = head.sources.union(*[r.sources for r in rest])
    return ExprResult(
        frozenset(sources),
        f"{head.text} IN ({', '.join(r.text for r in rest)})",
        TransformationType.EXPRESSION,
    )


def _eval_inset(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "InSet()", TransformationType.EXPRESSION)
    sub = eval_expr(node.children[0], env)
    hset = node.data.get("hset")
    return ExprResult(sub.sources, f"{sub.text} IN ({hset})", TransformationType.EXPRESSION)


def _eval_case_when(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    """CaseWhen has children = [cond1, val1, cond2, val2, ..., (else)?]"""
    children = node.children
    pairs: List[Tuple[ExprResult, ExprResult]] = []
    else_res: Optional[ExprResult] = None
    n = len(children)
    i = 0
    while i + 1 < n:
        cond = eval_expr(children[i], env)
        val = eval_expr(children[i + 1], env)
        pairs.append((cond, val))
        i += 2
    if i < n:
        else_res = eval_expr(children[i], env)

    sources = set()
    for c, v in pairs:
        sources |= c.sources
        sources |= v.sources
    if else_res is not None:
        sources |= else_res.sources

    parts = ["CASE"]
    for c, v in pairs:
        parts.append(f"WHEN {c.text} THEN {v.text}")
    if else_res is not None:
        parts.append(f"ELSE {else_res.text}")
    parts.append("END")
    return ExprResult(frozenset(sources), " ".join(parts), TransformationType.EXPRESSION)


def _eval_if(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if len(node.children) < 3:
        return _eval_generic(node, env)
    c, t, e = (eval_expr(node.children[i], env) for i in range(3))
    return ExprResult(
        frozenset(c.sources | t.sources | e.sources),
        f"IF({c.text}, {t.text}, {e.text})",
        TransformationType.EXPRESSION,
    )


def _eval_aggregate_expression(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "AGGREGATE()", TransformationType.AGGREGATE)
    sub = eval_expr(node.children[0], env)  # the wrapped function
    is_distinct = bool(node.data.get("isDistinct", False))
    text = sub.text
    if is_distinct:
        # Re-render the wrapped function with DISTINCT modifier
        text = _inject_distinct(sub.text)
    return ExprResult(sub.sources, text, TransformationType.AGGREGATE)


def _inject_distinct(text: str) -> str:
    paren = text.find("(")
    if paren < 0:
        return text
    return text[: paren + 1] + "DISTINCT " + text[paren + 1 :]


def _eval_window_expression(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if len(node.children) < 2:
        return _eval_generic(node, env)
    func = eval_expr(node.children[0], env)
    spec = eval_expr(node.children[1], env)
    return ExprResult(
        frozenset(func.sources | spec.sources),
        f"{func.text} OVER ({spec.text})",
        TransformationType.WINDOW,
    )


def _eval_window_spec_definition(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    parts: List[str] = []
    sources: set = set()
    pcs = referenced_children(node, "partitionSpec")
    if pcs:
        rendered = [eval_expr(c, env) for c in pcs]
        parts.append("PARTITION BY " + ", ".join(r.text for r in rendered))
        for r in rendered:
            sources |= r.sources
    ocs = referenced_children(node, "orderSpec")
    if ocs:
        rendered = [eval_expr(c, env) for c in ocs]
        parts.append("ORDER BY " + ", ".join(r.text for r in rendered))
        for r in rendered:
            sources |= r.sources
    frame = referenced_child(node, "frameSpecification")
    if frame is not None:
        fr = eval_expr(frame, env)
        if fr.text:
            parts.append(fr.text)
        sources |= fr.sources
    return ExprResult(frozenset(sources), " ".join(parts), TransformationType.WINDOW)


def _eval_specified_window_frame(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    frame_type = node.data.get("frameType")
    ft_name = (
        frame_type.get("object", "").rsplit(".", 1)[-1].rstrip("$")
        if isinstance(frame_type, dict)
        else "RowFrame"
    )
    kind = "ROWS" if ft_name == "RowFrame" else "RANGE"

    lower = referenced_child(node, "lower")
    upper = referenced_child(node, "upper")

    def render_bound(b: Optional[ExprNode], side: str) -> Tuple[FrozenSet[SourceColumn], str]:
        if b is None:
            return EMPTY_SOURCES, "UNBOUNDED PRECEDING" if side == "lower" else "UNBOUNDED FOLLOWING"
        sc = b.short_class
        if sc in _FRAME_BOUND_OBJECTS:
            return EMPTY_SOURCES, _FRAME_BOUND_OBJECTS[sc]
        sub = eval_expr(b, env)
        # If literal, decide PRECEDING/FOLLOWING from sign of value.
        if sc == "Literal":
            try:
                v = int(str(b.data.get("value", "0")))
                if v < 0:
                    return sub.sources, f"{abs(v)} PRECEDING"
                if v == 0:
                    return sub.sources, "CURRENT ROW"
                return sub.sources, f"{v} FOLLOWING"
            except (TypeError, ValueError):
                pass
        suffix = "PRECEDING" if side == "lower" else "FOLLOWING"
        return sub.sources, f"{sub.text} {suffix}"

    lo_src, lo_txt = render_bound(lower, "lower")
    up_src, up_txt = render_bound(upper, "upper")
    return ExprResult(
        frozenset(lo_src | up_src),
        f"{kind} BETWEEN {lo_txt} AND {up_txt}",
        TransformationType.WINDOW,
    )


def _eval_unspecified_frame(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    return ExprResult(EMPTY_SOURCES, "", TransformationType.WINDOW)


def _eval_frame_bound_marker(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    return ExprResult(
        EMPTY_SOURCES,
        _FRAME_BOUND_OBJECTS.get(node.short_class, node.short_class),
        TransformationType.WINDOW,
    )


def _eval_sort_order(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "", TransformationType.IDENTITY)
    sub = eval_expr(node.children[0], env)
    direction = node.data.get("direction") or {}
    null_ord = node.data.get("nullOrdering") or {}
    dir_name = direction.get("object", "").rsplit(".", 1)[-1].rstrip("$") if isinstance(direction, dict) else "Ascending"
    null_name = null_ord.get("object", "").rsplit(".", 1)[-1].rstrip("$") if isinstance(null_ord, dict) else "NullsFirst"
    direction_kw = "ASC" if dir_name == "Ascending" else "DESC"
    null_kw = "NULLS FIRST" if null_name == "NullsFirst" else "NULLS LAST"
    return ExprResult(sub.sources, f"{sub.text} {direction_kw} {null_kw}", sub.kind)


def _eval_lambda_function(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "() -> ()", TransformationType.EXPRESSION)
    body = node.children[0]
    params = node.children[1:]
    # Augment env with lambda variables (they have ExprIds).
    new_env = dict(env)
    rendered_params: List[str] = []
    for p in params:
        if p.short_class == "NamedLambdaVariable":
            eid_dict = p.data.get("exprId") or {}
            if isinstance(eid_dict, dict) and "id" in eid_dict:
                key = expr_id_key(eid_dict)
                pname = str(p.data.get("name", "x"))
                rendered_params.append(pname)
                new_env[key] = ColInfo(
                    expr_id=key,
                    name=pname,
                    data_type=str(p.data.get("dataType", "")),
                    sources=EMPTY_SOURCES,
                    transformation=pname,
                    transformation_type=TransformationType.IDENTITY,
                )
        else:
            rendered_params.append(eval_expr(p, env).text)
    body_res = eval_expr(body, new_env)
    if len(rendered_params) == 1:
        return ExprResult(body_res.sources, f"{rendered_params[0]} -> {body_res.text}", TransformationType.EXPRESSION)
    return ExprResult(
        body_res.sources,
        f"({', '.join(rendered_params)}) -> {body_res.text}",
        TransformationType.EXPRESSION,
    )


def _eval_named_lambda_variable(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    eid_dict = node.data.get("exprId") or {}
    name = str(node.data.get("name", "x"))
    if isinstance(eid_dict, dict) and "id" in eid_dict:
        key = expr_id_key(eid_dict)
        info = env.get(key)
        if info is not None:
            return ExprResult(info.sources, name, TransformationType.IDENTITY)
    return ExprResult(EMPTY_SOURCES, name, TransformationType.IDENTITY)


def _eval_array_transform(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if len(node.children) < 2:
        return _eval_generic(node, env)
    arr = eval_expr(node.children[0], env)
    fn = eval_expr(node.children[1], env)
    return ExprResult(
        frozenset(arr.sources | fn.sources),
        f"TRANSFORM({arr.text}, {fn.text})",
        TransformationType.EXPRESSION,
    )


def _eval_array_filter(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if len(node.children) < 2:
        return _eval_generic(node, env)
    arr = eval_expr(node.children[0], env)
    fn = eval_expr(node.children[1], env)
    return ExprResult(
        frozenset(arr.sources | fn.sources),
        f"FILTER({arr.text}, {fn.text})",
        TransformationType.EXPRESSION,
    )


def _eval_get_struct_field(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return _eval_generic(node, env)
    inner = eval_expr(node.children[0], env)
    field_name = node.data.get("name")
    if field_name is None:
        ordinal = node.data.get("ordinal")
        field_name = f"_{ordinal}" if ordinal is not None else "?"
    return ExprResult(inner.sources, f"{inner.text}.{field_name}", TransformationType.EXPRESSION)


def _eval_get_array_item(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if len(node.children) < 2:
        return _eval_generic(node, env)
    arr = eval_expr(node.children[0], env)
    idx = eval_expr(node.children[1], env)
    return ExprResult(
        frozenset(arr.sources | idx.sources),
        f"{arr.text}[{idx.text}]",
        TransformationType.EXPRESSION,
    )


def _eval_get_map_value(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if len(node.children) < 2:
        return _eval_generic(node, env)
    m = eval_expr(node.children[0], env)
    k = eval_expr(node.children[1], env)
    return ExprResult(
        frozenset(m.sources | k.sources),
        f"{m.text}[{k.text}]",
        TransformationType.EXPRESSION,
    )


def _eval_create_named_struct(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    """CreateNamedStruct children alternate name-literal, value-expr."""
    sources: set = set()
    pieces: List[str] = []
    children = node.children
    i = 0
    while i + 1 < len(children):
        n_res = eval_expr(children[i], env)
        v_res = eval_expr(children[i + 1], env)
        pieces.append(f"{n_res.text} : {v_res.text}")
        sources |= v_res.sources
        i += 2
    return ExprResult(frozenset(sources), "STRUCT(" + ", ".join(pieces) + ")", TransformationType.EXPRESSION)


def _eval_explode(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "EXPLODE()", TransformationType.GENERATOR)
    sub = eval_expr(node.children[0], env)
    return ExprResult(sub.sources, f"EXPLODE({sub.text})", TransformationType.GENERATOR)


def _eval_pos_explode(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "POSEXPLODE()", TransformationType.GENERATOR)
    sub = eval_expr(node.children[0], env)
    return ExprResult(sub.sources, f"POSEXPLODE({sub.text})", TransformationType.GENERATOR)


def _eval_inline(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    if not node.children:
        return ExprResult(EMPTY_SOURCES, "INLINE()", TransformationType.GENERATOR)
    sub = eval_expr(node.children[0], env)
    return ExprResult(sub.sources, f"INLINE({sub.text})", TransformationType.GENERATOR)


def _eval_stack(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    args = [eval_expr(c, env) for c in node.children]
    sources: set = set()
    for r in args:
        sources |= r.sources
    return ExprResult(
        frozenset(sources),
        "STACK(" + ", ".join(r.text for r in args) + ")",
        TransformationType.GENERATOR,
    )


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _eval_binary_op(node: ExprNode, env: Dict[ExprIdKey, ColInfo], op: str) -> ExprResult:
    if len(node.children) < 2:
        return _eval_generic(node, env)
    l = eval_expr(node.children[0], env)
    r = eval_expr(node.children[1], env)
    return ExprResult(
        frozenset(l.sources | r.sources),
        f"({l.text} {op} {r.text})",
        TransformationType.EXPRESSION,
    )


def _eval_unary_prefix(node: ExprNode, env: Dict[ExprIdKey, ColInfo], op: str) -> ExprResult:
    if not node.children:
        return _eval_generic(node, env)
    sub = eval_expr(node.children[0], env)
    if op == "NOT":
        return ExprResult(sub.sources, f"(NOT {sub.text})", TransformationType.EXPRESSION)
    return ExprResult(sub.sources, f"({op}{sub.text})", TransformationType.EXPRESSION)


def _eval_unary_postfix(node: ExprNode, env: Dict[ExprIdKey, ColInfo], op: str) -> ExprResult:
    if not node.children:
        return _eval_generic(node, env)
    sub = eval_expr(node.children[0], env)
    return ExprResult(sub.sources, f"({sub.text} {op})", TransformationType.EXPRESSION)


def _eval_aggregate_func(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    func = _aggregate_func_name(node.short_class)
    if not node.children:
        if node.short_class in ("Count", "RowNumber"):
            return ExprResult(EMPTY_SOURCES, f"{func}(*)", TransformationType.AGGREGATE)
        return ExprResult(EMPTY_SOURCES, f"{func}()", TransformationType.AGGREGATE)
    args = [eval_expr(c, env) for c in node.children]
    sources: set = set()
    for r in args:
        sources |= r.sources
    text = f"{func}({', '.join(r.text for r in args)})"
    if node.short_class == "Count" and len(args) == 1 and args[0].kind == TransformationType.LITERAL and args[0].text == "1":
        text = "COUNT(*)"
    return ExprResult(frozenset(sources), text, TransformationType.AGGREGATE)


def _eval_window_func(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    func = _aggregate_func_name(node.short_class)
    if not node.children:
        return ExprResult(EMPTY_SOURCES, f"{func}()", TransformationType.WINDOW)
    # Lag/Lead use children = [input, offset, default]; their value depends on
    # `input` only; offset / default are constants but we still propagate.
    args = [eval_expr(c, env) for c in node.children]
    sources: set = set()
    for r in args:
        sources |= r.sources
    return ExprResult(
        frozenset(sources),
        f"{func}({', '.join(r.text for r in args)})",
        TransformationType.WINDOW,
    )


def _eval_generic(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ExprResult:
    func = _generic_func_name(node.short_class)
    if not node.children:
        return ExprResult(EMPTY_SOURCES, f"{func}()", TransformationType.EXPRESSION)
    args = [eval_expr(c, env) for c in node.children]
    sources: set = set()
    for r in args:
        sources |= r.sources
    return ExprResult(
        frozenset(sources),
        f"{func}({', '.join(r.text for r in args)})",
        TransformationType.EXPRESSION,
    )


def _looks_like_aggregate(node: ExprNode) -> bool:
    return ".aggregate." in node.class_name


def _qualified_name(node: ExprNode) -> str:
    name = str(node.data.get("name", ""))
    qual = node.data.get("qualifier")
    if isinstance(qual, str):
        # qualifier sometimes serialized as "[a, b, c]"
        s = qual.strip("[]").strip()
        if s:
            parts = [p.strip() for p in s.split(",") if p.strip()]
            if parts:
                return ".".join(parts + [name])
    if isinstance(qual, list) and qual:
        parts = [str(p) for p in qual if p]
        if parts:
            return ".".join(parts + [name])
    return name


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------


_DISPATCH: Dict[str, Callable[[ExprNode, Dict[ExprIdKey, ColInfo]], ExprResult]] = {
    "AttributeReference": _eval_attribute_reference,
    "OuterReference": _eval_outer_reference,
    "Literal": _eval_literal,
    "Alias": _eval_alias,
    "Cast": _eval_cast,
    "AnsiCast": _eval_cast,
    "TryCast": _eval_cast,
    "In": _eval_in,
    "InSet": _eval_inset,
    "InSubquery": _eval_in,
    "CaseWhen": _eval_case_when,
    "If": _eval_if,
    "AggregateExpression": _eval_aggregate_expression,
    "WindowExpression": _eval_window_expression,
    "WindowSpecDefinition": _eval_window_spec_definition,
    "SpecifiedWindowFrame": _eval_specified_window_frame,
    "UnspecifiedFrame": _eval_unspecified_frame,
    "UnboundedPreceding": _eval_frame_bound_marker,
    "UnboundedFollowing": _eval_frame_bound_marker,
    "CurrentRow": _eval_frame_bound_marker,
    "SortOrder": _eval_sort_order,
    "LambdaFunction": _eval_lambda_function,
    "NamedLambdaVariable": _eval_named_lambda_variable,
    "ArrayTransform": _eval_array_transform,
    "ArrayFilter": _eval_array_filter,
    "GetStructField": _eval_get_struct_field,
    "GetArrayItem": _eval_get_array_item,
    "GetMapValue": _eval_get_map_value,
    "ElementAt": _eval_get_array_item,
    "CreateNamedStruct": _eval_create_named_struct,
    "CreateStruct": _eval_create_named_struct,
    "Explode": _eval_explode,
    "ExplodeOuter": _eval_explode,
    "PosExplode": _eval_pos_explode,
    "PosExplodeOuter": _eval_pos_explode,
    "Inline": _eval_inline,
    "InlineOuter": _eval_inline,
    "Stack": _eval_stack,
}
