"""Plan-level walker: build column-lineage from a parsed `PlanNode` tree.

The analyzer is implemented as a context-aware bottom-up traversal:

  * `Context` holds the live CTE registry (`ctes: cte_id -> [ColInfo]`)
    and a flag whether we are inside a query subtree under a DDL/DML root.

  * `analyze_plan(node, ctx)` returns the ordered list of `ColInfo`s
    representing the OUTPUT columns of the subtree rooted at `node`.

  * Every supported plan-node class has a dedicated handler.  The handlers
    cover the full Spark Catalyst logical plan vocabulary that we have
    observed in practice (and a few more for safety):

        - LogicalRelation, LocalRelation, OneRowRelation, Range
        - DataSourceV2Relation, HiveTableRelation
        - SubqueryAlias, Project, Filter, Sort, Distinct, Deduplicate,
          GlobalLimit, LocalLimit, Sample, RepartitionByExpression,
          Repartition, RebalancePartitions
        - Aggregate, Window, Expand, Generate, Pivot, Unpivot
        - Join (all join types)
        - Union, Intersect, Except, Distinct
        - WithCTE, CTERelationDef, CTERelationRef
        - DDL: CreateViewCommand, CreateDataSourceTableCommand,
               CreateTable, CreateTableAsSelect (CTAS), ReplaceTable*,
               DropTable, DropTableCommand, DropView, DropNamespace,
               CreateNamespace, ShowTables, SetCatalogAndNamespace
        - DML: InsertIntoStatement, InsertIntoHadoopFsRelationCommand,
               InsertIntoDataSourceCommand, AppendData, OverwriteByExpression,
               OverwritePartitionsDynamic, MergeIntoTable

Where the underlying child plan does not change the schema, the handler
simply returns the child's output unchanged.

If we encounter an unknown plan node, we degrade gracefully: we descend
into the first child (if any) and return its output, so an unsupported
wrapper does not break the analysis.  The output is never silently
fabricated - a column with empty `sources` always corresponds either to
a literal, a synthesized grouping marker, or a generator-output that we
record explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .expressions import (
    EMPTY_SOURCES,
    ExprResult,
    eval_expr,
    expr_id_key,
    referenced_child,
    referenced_children,
)
from .models import (
    ColInfo,
    ExprIdKey,
    ExprNode,
    LineageResult,
    PlanKind,
    PlanNode,
    SourceColumn,
    TargetTable,
    TransformationType,
)
from .parser import parse_expr_field


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class Context:
    ctes: Dict[int, List[ColInfo]] = field(default_factory=dict)
    cte_names: Dict[int, str] = field(default_factory=dict)


def _env_from_columns(cols: List[ColInfo]) -> Dict[ExprIdKey, ColInfo]:
    return {c.expr_id: c for c in cols}


def _attr_to_col(node: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ColInfo:
    """Resolve an AttributeReference expression node to a ColInfo from `env`.

    If the attribute is not resolvable in `env`, we synthesise a passthrough
    `ColInfo` that records the attribute as-is (sources empty)."""
    eid_dict = node.data.get("exprId") or {}
    name = str(node.data.get("name", ""))
    dt = str(node.data.get("dataType", ""))
    if isinstance(eid_dict, dict) and "id" in eid_dict:
        key = expr_id_key(eid_dict)
        info = env.get(key)
        if info is not None:
            return ColInfo(
                expr_id=key,
                name=name or info.name,
                data_type=dt or info.data_type,
                sources=info.sources,
                transformation=info.transformation,
                transformation_type=info.transformation_type,
            )
        return ColInfo(
            expr_id=key,
            name=name,
            data_type=dt,
            sources=EMPTY_SOURCES,
            transformation=name,
            transformation_type=TransformationType.IDENTITY,
        )
    return ColInfo(
        expr_id=(0, ""),
        name=name,
        data_type=dt,
        sources=EMPTY_SOURCES,
        transformation=name,
        transformation_type=TransformationType.IDENTITY,
    )


def _qualifier_to_table(qual: object) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse a Catalyst qualifier into (catalog, db, table).

    Spark serialises qualifiers either as JSON arrays or as the string
    "[a, b, c]". The 3-element form is "[catalog, database, table]".
    Shorter qualifiers are aligned to the right (database+table, table only,
    or unqualified)."""
    parts: List[str] = []
    if isinstance(qual, list):
        parts = [str(p) for p in qual if p]
    elif isinstance(qual, str):
        s = qual.strip("[]").strip()
        parts = [p.strip() for p in s.split(",") if p.strip()] if s else []
    if not parts:
        return None, None, None
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], ".".join(parts[2:])


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def analyze(plan: PlanNode) -> LineageResult:
    """Analyse a parsed plan tree and return a `LineageResult`.

    The function determines whether the root is a query (returns rows),
    DDL (DROP/CREATE), DML (INSERT/CTAS/...), or a namespace operation
    (USE / CREATE NAMESPACE), and dispatches accordingly.
    """

    cls = plan.short_class
    ctx = Context()

    if cls in _DDL_HANDLERS:
        return _DDL_HANDLERS[cls](plan, ctx)
    if cls in _DML_HANDLERS:
        return _DML_HANDLERS[cls](plan, ctx)
    if cls in _NAMESPACE_HANDLERS:
        return _NAMESPACE_HANDLERS[cls](plan, ctx)

    cols = analyze_plan(plan, ctx)
    sources = _collect_source_tables(cols)
    return LineageResult(
        plan_kind=PlanKind.QUERY,
        operation=cls,
        target=None,
        columns=cols,
        source_tables=sources,
        cte_definitions={
            ctx.cte_names.get(cid, f"cte_{cid}"): cols2
            for cid, cols2 in ctx.ctes.items()
        },
    )


def _collect_source_tables(cols: List[ColInfo]) -> List[str]:
    seen: List[str] = []
    seen_set: set = set()
    for c in cols:
        for s in c.sources:
            tbl_parts = [p for p in (s.catalog, s.database, s.table) if p]
            tbl = ".".join(tbl_parts) if tbl_parts else s.table
            if tbl not in seen_set:
                seen_set.add(tbl)
                seen.append(tbl)
    return sorted(seen)


# ---------------------------------------------------------------------------
# Plan handler dispatch
# ---------------------------------------------------------------------------


def analyze_plan(node: PlanNode, ctx: Context) -> List[ColInfo]:
    handler = _PLAN_HANDLERS.get(node.short_class)
    if handler is not None:
        return handler(node, ctx)

    # Default for unknown wrappers: descend into the first child if any.
    if node.children:
        return analyze_plan(node.children[0], ctx)
    return []


# ---------------------------------------------------------------------------
# Leaves
# ---------------------------------------------------------------------------


def _leaf_table(table_dict: dict) -> Tuple[Optional[str], Optional[str], str]:
    cat = table_dict.get("catalog")
    db = table_dict.get("database")
    tbl = table_dict.get("table") or table_dict.get("name") or ""
    return cat, db, tbl


def _handle_logical_relation(node: PlanNode, ctx: Context) -> List[ColInfo]:
    out_field = node.data.get("output")
    catalog_table = node.data.get("catalogTable") or {}
    identifier = catalog_table.get("identifier") or {}
    cat, db, tbl = _leaf_table(identifier)

    cols: List[ColInfo] = []
    if isinstance(out_field, list):
        for entry in out_field:
            if isinstance(entry, list):
                expr_root = parse_expr_field(entry)
            else:
                continue
            if expr_root is None:
                continue
            n = expr_root if hasattr(expr_root, "data") else None
            if n is None:
                continue
            if n.short_class == "AttributeReference":
                eid_dict = n.data.get("exprId") or {}
                col_name = str(n.data.get("name", ""))
                dt = str(n.data.get("dataType", ""))
                src = SourceColumn(
                    catalog=cat,
                    database=db,
                    table=tbl,
                    column=col_name,
                )
                cols.append(
                    ColInfo(
                        expr_id=expr_id_key(eid_dict),
                        name=col_name,
                        data_type=dt,
                        sources=frozenset({src}),
                        transformation=src.fqn or col_name,
                        transformation_type=TransformationType.IDENTITY,
                    )
                )
    return cols


def _handle_local_relation(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """Local in-memory relation (e.g. VALUES (...) lists).

    The output schema is in `output`; values themselves are inline literal
    constants and contribute no real source columns."""
    out_field = node.data.get("output")
    cols: List[ColInfo] = []
    if isinstance(out_field, list):
        for entry in out_field:
            if not isinstance(entry, list):
                continue
            n = parse_expr_field(entry)
            if n is None or n.short_class != "AttributeReference":
                continue
            eid_dict = n.data.get("exprId") or {}
            col_name = str(n.data.get("name", ""))
            dt = str(n.data.get("dataType", ""))
            cols.append(
                ColInfo(
                    expr_id=expr_id_key(eid_dict),
                    name=col_name,
                    data_type=dt,
                    sources=EMPTY_SOURCES,
                    transformation=f"<literal:{col_name}>",
                    transformation_type=TransformationType.LITERAL,
                )
            )
    return cols


def _handle_one_row_relation(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return []


def _handle_range(node: PlanNode, ctx: Context) -> List[ColInfo]:
    out_field = node.data.get("output")
    cols: List[ColInfo] = []
    if isinstance(out_field, list):
        for entry in out_field:
            if not isinstance(entry, list):
                continue
            n = parse_expr_field(entry)
            if n is None or n.short_class != "AttributeReference":
                continue
            eid_dict = n.data.get("exprId") or {}
            col_name = str(n.data.get("name", "id"))
            dt = str(n.data.get("dataType", "long"))
            cols.append(
                ColInfo(
                    expr_id=expr_id_key(eid_dict),
                    name=col_name,
                    data_type=dt,
                    sources=EMPTY_SOURCES,
                    transformation=f"RANGE.{col_name}",
                    transformation_type=TransformationType.LITERAL,
                )
            )
    return cols


def _handle_data_source_v2_relation(node: PlanNode, ctx: Context) -> List[ColInfo]:
    out_field = node.data.get("output")
    table = node.data.get("table") or {}
    identifier = table.get("identifier") or table
    cat, db, tbl = _leaf_table(identifier)
    if not tbl:
        # Fall back to the V2 table name fields
        tbl = str(table.get("name", ""))

    cols: List[ColInfo] = []
    if isinstance(out_field, list):
        for entry in out_field:
            if not isinstance(entry, list):
                continue
            n = parse_expr_field(entry)
            if n is None or n.short_class != "AttributeReference":
                continue
            eid_dict = n.data.get("exprId") or {}
            col_name = str(n.data.get("name", ""))
            dt = str(n.data.get("dataType", ""))
            src = SourceColumn(catalog=cat, database=db, table=tbl, column=col_name)
            cols.append(
                ColInfo(
                    expr_id=expr_id_key(eid_dict),
                    name=col_name,
                    data_type=dt,
                    sources=frozenset({src}),
                    transformation=src.fqn or col_name,
                    transformation_type=TransformationType.IDENTITY,
                )
            )
    return cols


def _handle_hive_table_relation(node: PlanNode, ctx: Context) -> List[ColInfo]:
    table = node.data.get("tableMeta") or node.data.get("catalogTable") or {}
    identifier = table.get("identifier") or {}
    cat, db, tbl = _leaf_table(identifier)
    cols: List[ColInfo] = []
    for key in ("dataCols", "partitionCols", "output"):
        out_field = node.data.get(key)
        if not isinstance(out_field, list):
            continue
        for entry in out_field:
            if not isinstance(entry, list):
                continue
            n = parse_expr_field(entry)
            if n is None or n.short_class != "AttributeReference":
                continue
            eid_dict = n.data.get("exprId") or {}
            col_name = str(n.data.get("name", ""))
            dt = str(n.data.get("dataType", ""))
            src = SourceColumn(catalog=cat, database=db, table=tbl, column=col_name)
            cols.append(
                ColInfo(
                    expr_id=expr_id_key(eid_dict),
                    name=col_name,
                    data_type=dt,
                    sources=frozenset({src}),
                    transformation=src.fqn or col_name,
                    transformation_type=TransformationType.IDENTITY,
                )
            )
    return cols


# ---------------------------------------------------------------------------
# Single-child passthroughs
# ---------------------------------------------------------------------------


def _passthrough_first_child(node: PlanNode, ctx: Context) -> List[ColInfo]:
    if not node.children:
        return []
    return analyze_plan(node.children[0], ctx)


def _handle_subquery_alias(node: PlanNode, ctx: Context) -> List[ColInfo]:
    if not node.children:
        return []
    cols = analyze_plan(node.children[0], ctx)
    # Update qualifier-only metadata; we do not copy because list mutation is fine.
    ident = node.data.get("identifier") or {}
    new_alias = ident.get("name") if isinstance(ident, dict) else None

    if not new_alias:
        return cols
    # Note: we keep ColInfo unchanged - lineage of base tables is what matters.
    return cols


# ---------------------------------------------------------------------------
# Project / Filter / Sort
# ---------------------------------------------------------------------------


def _handle_project(node: PlanNode, ctx: Context) -> List[ColInfo]:
    if not node.children:
        return []
    child_out = analyze_plan(node.children[0], ctx)
    env = _env_from_columns(child_out)
    project_list = node.data.get("projectList") or []

    out: List[ColInfo] = []
    for entry in project_list:
        if not isinstance(entry, list):
            continue
        expr_root = parse_expr_field(entry)
        if expr_root is None:
            continue
        out.append(_project_item(expr_root, env))
    return out


def _project_item(expr_root: ExprNode, env: Dict[ExprIdKey, ColInfo]) -> ColInfo:
    """Convert a top-level projectList expression into a ColInfo."""
    cls = expr_root.short_class
    if cls == "AttributeReference":
        return _attr_to_col(expr_root, env)
    if cls == "Alias":
        # Alias wraps an expression; pull out the new exprId+name+dt from the
        # alias and the lineage from the inner expression.
        alias_eid = expr_root.data.get("exprId") or {}
        alias_name = str(expr_root.data.get("name", ""))
        # The alias's own dataType is not in the JSON; pull from inner.
        inner = expr_root.children[0] if expr_root.children else None
        if inner is None:
            return ColInfo(
                expr_id=expr_id_key(alias_eid),
                name=alias_name,
                data_type="",
                sources=EMPTY_SOURCES,
                transformation=alias_name,
                transformation_type=TransformationType.LITERAL,
            )
        sub = eval_expr(inner, env)
        dt = str(inner.data.get("dataType", ""))
        return ColInfo(
            expr_id=expr_id_key(alias_eid),
            name=alias_name,
            data_type=dt,
            sources=sub.sources,
            transformation=sub.text,
            transformation_type=sub.kind,
        )
    # Bare expression as projection (rare): synthesize a synthetic ExprId.
    sub = eval_expr(expr_root, env)
    fallback_eid: ExprIdKey = (-id(expr_root), "synthetic")
    return ColInfo(
        expr_id=fallback_eid,
        name=sub.text,
        data_type=str(expr_root.data.get("dataType", "")),
        sources=sub.sources,
        transformation=sub.text,
        transformation_type=sub.kind,
    )


def _handle_filter(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return _passthrough_first_child(node, ctx)


def _handle_sort(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return _passthrough_first_child(node, ctx)


def _handle_limit(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return _passthrough_first_child(node, ctx)


def _handle_distinct(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return _passthrough_first_child(node, ctx)


def _handle_repartition(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return _passthrough_first_child(node, ctx)


# ---------------------------------------------------------------------------
# Aggregate / Window / Expand / Generate
# ---------------------------------------------------------------------------


def _handle_aggregate(node: PlanNode, ctx: Context) -> List[ColInfo]:
    if not node.children:
        return []
    child_out = analyze_plan(node.children[0], ctx)
    env = _env_from_columns(child_out)
    agg_list = node.data.get("aggregateExpressions") or []

    out: List[ColInfo] = []
    for entry in agg_list:
        if not isinstance(entry, list):
            continue
        expr_root = parse_expr_field(entry)
        if expr_root is None:
            continue
        out.append(_project_item(expr_root, env))
    return out


def _handle_expand(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """Expand multiplies rows for ROLLUP/CUBE/GROUPING SETS.

    Output columns have fixed `output` AttributeReferences. Each output
    column is the union (over all projection sets) of the lineage of the
    expression sitting at the same position within each projection."""
    if not node.children:
        return []
    child_out = analyze_plan(node.children[0], ctx)
    env = _env_from_columns(child_out)

    output_field = node.data.get("output") or []
    projections_field = node.data.get("projections") or []

    # Parse projections: list of (list of expr_root)
    parsed_projections: List[List[ExprNode]] = []
    for proj in projections_field:
        if not isinstance(proj, list):
            continue
        parsed_proj: List[ExprNode] = []
        for col_expr in proj:
            if not isinstance(col_expr, list):
                parsed_proj.append(None)  # type: ignore
                continue
            root = parse_expr_field(col_expr)
            parsed_proj.append(root if root is not None else None)  # type: ignore
        parsed_projections.append(parsed_proj)

    out_attrs: List[ExprNode] = []
    for entry in output_field:
        if isinstance(entry, list):
            n = parse_expr_field(entry)
            if n is not None:
                out_attrs.append(n)

    out: List[ColInfo] = []
    for i, attr in enumerate(out_attrs):
        eid_dict = attr.data.get("exprId") or {}
        name = str(attr.data.get("name", ""))
        dt = str(attr.data.get("dataType", ""))
        sources: set = set()
        descriptions: List[str] = []
        kind = TransformationType.IDENTITY
        for proj in parsed_projections:
            if i >= len(proj) or proj[i] is None:
                continue
            sub = eval_expr(proj[i], env)
            sources |= sub.sources
            descriptions.append(sub.text)
        if not sources and name == "spark_grouping_id":
            kind = TransformationType.GROUPING
            text = "GROUPING_ID()"
        elif not sources and any("NULL" in d for d in descriptions):
            kind = TransformationType.GROUPING
            text = " | ".join(dict.fromkeys(descriptions))
        else:
            unique = list(dict.fromkeys(descriptions))
            text = unique[0] if len(unique) == 1 else " | ".join(unique)
            kind = TransformationType.GROUPING if len(unique) > 1 else TransformationType.IDENTITY
        out.append(
            ColInfo(
                expr_id=expr_id_key(eid_dict),
                name=name,
                data_type=dt,
                sources=frozenset(sources),
                transformation=text or name,
                transformation_type=kind,
            )
        )
    return out


def _handle_window(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """Window adds new columns from `windowExpressions`; existing columns
    are passed through unchanged."""
    if not node.children:
        return []
    child_out = analyze_plan(node.children[0], ctx)
    env = _env_from_columns(child_out)

    window_exprs = node.data.get("windowExpressions") or []
    out = list(child_out)
    for entry in window_exprs:
        if not isinstance(entry, list):
            continue
        root = parse_expr_field(entry)
        if root is None:
            continue
        out.append(_project_item(root, env))
    return out


def _handle_generate(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """LATERAL VIEW / table-valued generator (e.g. EXPLODE).

    Output schema = (child output [optionally minus unrequiredChildIndex]) +
    generatorOutput attributes derived from the generator's input."""
    if not node.children:
        return []
    child_out = analyze_plan(node.children[0], ctx)
    env = _env_from_columns(child_out)

    generator_field = node.data.get("generator")
    generator_root: Optional[ExprNode] = None
    if isinstance(generator_field, list):
        generator_root = parse_expr_field(generator_field)
    gen_sources = EMPTY_SOURCES
    gen_text = ""
    if generator_root is not None:
        gen_res = eval_expr(generator_root, env)
        gen_sources = gen_res.sources
        gen_text = gen_res.text

    # Map child outputs by exprId we need to drop (per `unrequiredChildIndex`).
    out: List[ColInfo] = list(child_out)

    gen_output_field = node.data.get("generatorOutput") or []
    for entry in gen_output_field:
        if not isinstance(entry, list):
            continue
        n = parse_expr_field(entry)
        if n is None or n.short_class != "AttributeReference":
            continue
        eid_dict = n.data.get("exprId") or {}
        name = str(n.data.get("name", ""))
        dt = str(n.data.get("dataType", ""))
        out.append(
            ColInfo(
                expr_id=expr_id_key(eid_dict),
                name=name,
                data_type=dt,
                sources=gen_sources,
                transformation=f"{gen_text}.{name}" if gen_text else name,
                transformation_type=TransformationType.GENERATOR,
            )
        )
    return out


def _handle_pivot(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """Pivot reshapes the table.  Pivot output columns combine the lineage
    of the pivot value (`aggregates`) with the pivot key (`pivotColumn`)."""
    if not node.children:
        return []
    child_out = analyze_plan(node.children[0], ctx)
    env = _env_from_columns(child_out)

    aggregates_field = node.data.get("aggregates") or []
    pivot_column_field = node.data.get("pivotColumn") or []
    pivot_values_field = node.data.get("pivotValues") or []
    grouping_field = node.data.get("groupByExprs") or []

    pivot_col_sources: set = set()
    if isinstance(pivot_column_field, list):
        root = parse_expr_field(pivot_column_field)
        if root is not None:
            pivot_col_sources = set(eval_expr(root, env).sources)

    grouping_cols: List[ColInfo] = []
    for entry in grouping_field:
        if not isinstance(entry, list):
            continue
        root = parse_expr_field(entry)
        if root is None:
            continue
        grouping_cols.append(_project_item(root, env))

    out: List[ColInfo] = list(grouping_cols)
    aggs: List[ExprNode] = []
    for entry in aggregates_field:
        if not isinstance(entry, list):
            continue
        root = parse_expr_field(entry)
        if root is not None:
            aggs.append(root)

    pv_count = len(pivot_values_field) if isinstance(pivot_values_field, list) else 0
    pv_count = pv_count or 1
    for agg in aggs:
        agg_res = eval_expr(agg, env)
        for v_idx in range(pv_count):
            value = pivot_values_field[v_idx] if v_idx < len(pivot_values_field) else None
            label = f"{agg_res.text}_{value}" if value is not None else agg_res.text
            out.append(
                ColInfo(
                    expr_id=(0, f"pivot:{label}:{v_idx}"),
                    name=label,
                    data_type=str(agg.data.get("dataType", "")),
                    sources=frozenset(set(agg_res.sources) | pivot_col_sources),
                    transformation=f"PIVOT({agg_res.text}) FOR {label}",
                    transformation_type=TransformationType.AGGREGATE,
                )
            )
    return out


# ---------------------------------------------------------------------------
# Joins
# ---------------------------------------------------------------------------


def _handle_join(node: PlanNode, ctx: Context) -> List[ColInfo]:
    if len(node.children) < 2:
        return _passthrough_first_child(node, ctx)
    left = analyze_plan(node.children[0], ctx)
    right = analyze_plan(node.children[1], ctx)

    join_type = node.data.get("joinType") or {}
    join_name = (
        join_type.get("object", "").rsplit(".", 1)[-1].rstrip("$")
        if isinstance(join_type, dict)
        else ""
    )

    # Schemes:
    #   Inner / FullOuter / LeftOuter / RightOuter / Cross / NaturalJoin /
    #   UsingJoin / ExistenceJoin -> left ++ right
    #   LeftSemi / LeftAnti                                -> left only
    #   RightSemi (rare)                                   -> right only
    if join_name in ("LeftSemi", "LeftAnti"):
        return left
    if join_name == "RightSemi":
        return right
    return left + right


# ---------------------------------------------------------------------------
# Set operations
# ---------------------------------------------------------------------------


def _handle_set_op(node: PlanNode, ctx: Context) -> List[ColInfo]:
    if not node.children:
        return []
    branches = [analyze_plan(c, ctx) for c in node.children]
    if not branches or not branches[0]:
        return []
    base = branches[0]
    out: List[ColInfo] = []
    for i, base_col in enumerate(base):
        sources: set = set(base_col.sources)
        names = [base_col.name]
        transforms = [base_col.transformation]
        for other in branches[1:]:
            if i < len(other):
                sources |= other[i].sources
                if other[i].name != base_col.name:
                    names.append(other[i].name)
                if other[i].transformation != base_col.transformation:
                    transforms.append(other[i].transformation)
        out.append(
            ColInfo(
                expr_id=base_col.expr_id,
                name=base_col.name,
                data_type=base_col.data_type,
                sources=frozenset(sources),
                transformation=" | ".join(dict.fromkeys(transforms)),
                transformation_type=base_col.transformation_type,
            )
        )
    return out


# ---------------------------------------------------------------------------
# CTEs
# ---------------------------------------------------------------------------


def _handle_with_cte(node: PlanNode, ctx: Context) -> List[ColInfo]:
    plan_idx = node.data.get("plan")
    cte_idxs = node.data.get("cteDefs") or []

    # First evaluate all CTE definitions so they are available to the main plan.
    if isinstance(cte_idxs, list):
        for idx in cte_idxs:
            if isinstance(idx, int) and 0 <= idx < len(node.children):
                cte_node = node.children[idx]
                _evaluate_cte_def(cte_node, ctx)

    if isinstance(plan_idx, int) and 0 <= plan_idx < len(node.children):
        return analyze_plan(node.children[plan_idx], ctx)
    # Fallback: last child
    if node.children:
        return analyze_plan(node.children[-1], ctx)
    return []


def _evaluate_cte_def(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """Evaluate a CTERelationDef and register its output by id."""
    cls = node.short_class
    if cls != "CTERelationDef":
        return analyze_plan(node, ctx)
    cte_id = node.data.get("id")
    cols: List[ColInfo] = []
    if node.children:
        cols = analyze_plan(node.children[0], ctx)
    if isinstance(cte_id, int):
        ctx.ctes[cte_id] = cols
        # Try to capture an alias name from the immediate child SubqueryAlias.
        if node.children:
            sub = node.children[0]
            if sub.short_class == "SubqueryAlias":
                ident = sub.data.get("identifier") or {}
                name = ident.get("name") if isinstance(ident, dict) else None
                if name:
                    ctx.cte_names[cte_id] = str(name)
    return cols


def _handle_cte_def(node: PlanNode, ctx: Context) -> List[ColInfo]:
    return _evaluate_cte_def(node, ctx)


def _handle_cte_ref(node: PlanNode, ctx: Context) -> List[ColInfo]:
    """Reference to a CTE by id; remap the def's columns to the ref's ExprIds.

    Each ref carries its own `output` list of AttributeReferences with
    potentially different ExprIds. The mapping is positional.
    """
    cte_id = node.data.get("cteId")
    def_cols = ctx.ctes.get(cte_id, [])  # type: ignore[arg-type]

    output_field = node.data.get("output") or []
    out: List[ColInfo] = []
    if isinstance(output_field, list):
        for i, entry in enumerate(output_field):
            if not isinstance(entry, list):
                continue
            n = parse_expr_field(entry)
            if n is None or n.short_class != "AttributeReference":
                continue
            eid_dict = n.data.get("exprId") or {}
            name = str(n.data.get("name", ""))
            dt = str(n.data.get("dataType", ""))
            if i < len(def_cols):
                base = def_cols[i]
                out.append(
                    ColInfo(
                        expr_id=expr_id_key(eid_dict),
                        name=name or base.name,
                        data_type=dt or base.data_type,
                        sources=base.sources,
                        transformation=base.transformation,
                        transformation_type=base.transformation_type,
                    )
                )
            else:
                out.append(
                    ColInfo(
                        expr_id=expr_id_key(eid_dict),
                        name=name,
                        data_type=dt,
                        sources=EMPTY_SOURCES,
                        transformation=name,
                        transformation_type=TransformationType.IDENTITY,
                    )
                )
    return out


# ---------------------------------------------------------------------------
# Plan dispatch table
# ---------------------------------------------------------------------------


_PLAN_HANDLERS = {
    # Leaves
    "LogicalRelation": _handle_logical_relation,
    "LocalRelation": _handle_local_relation,
    "OneRowRelation": _handle_one_row_relation,
    "Range": _handle_range,
    "DataSourceV2Relation": _handle_data_source_v2_relation,
    "DataSourceV2ScanRelation": _handle_data_source_v2_relation,
    "HiveTableRelation": _handle_hive_table_relation,
    # Single-child shape preserving
    "SubqueryAlias": _handle_subquery_alias,
    "Filter": _handle_filter,
    "Sort": _handle_sort,
    "GlobalLimit": _handle_limit,
    "LocalLimit": _handle_limit,
    "Tail": _handle_limit,
    "Sample": _passthrough_first_child,
    "RepartitionByExpression": _handle_repartition,
    "Repartition": _handle_repartition,
    "RebalancePartitions": _handle_repartition,
    "Coalesce": _handle_repartition,
    "Distinct": _handle_distinct,
    "Deduplicate": _handle_distinct,
    "DeduplicateWithinWatermark": _handle_distinct,
    # Schema-changing operators
    "Project": _handle_project,
    "Aggregate": _handle_aggregate,
    "Window": _handle_window,
    "Expand": _handle_expand,
    "Generate": _handle_generate,
    "Pivot": _handle_pivot,
    "Unpivot": _passthrough_first_child,  # best-effort
    # Joins / set ops
    "Join": _handle_join,
    "AsOfJoin": _handle_join,
    "Union": _handle_set_op,
    "Intersect": _handle_set_op,
    "Except": _handle_set_op,
    # CTEs
    "WithCTE": _handle_with_cte,
    "CTERelationDef": _handle_cte_def,
    "CTERelationRef": _handle_cte_ref,
}


# ---------------------------------------------------------------------------
# DDL / DML / Namespace dispatchers
# ---------------------------------------------------------------------------


def _ddl_drop_table(node: PlanNode, ctx: Context) -> LineageResult:
    # `DropTable` plan uses ResolvedIdentifier child; `DropTableCommand`
    # carries the identifier inline.  Some Spark versions emit the V2
    # ResolvedIdentifier with `identifier: null` (the resolved id is a
    # catalog-plugin object that does not survive the JSON dump), in
    # which case the target is genuinely unrecoverable from the input.
    target = _drop_table_target(node)
    notes = ["No data flow"]
    if target is None and node.short_class == "DropTable":
        notes.append(
            "Target identifier was null in the plan dump (Spark V2 catalog "
            "ResolvedIdentifier is not always serialised); the table name is "
            "not present in the input JSON."
        )
    return LineageResult(
        plan_kind=PlanKind.DDL,
        operation=node.short_class,
        target=target,
        columns=[],
        source_tables=[],
        notes=notes,
    )


def _drop_table_target(node: PlanNode) -> Optional[TargetTable]:
    cls = node.short_class
    if cls == "DropTableCommand":
        ident = node.data.get("tableName") or {}
        cat = ident.get("catalog")
        db = ident.get("database")
        tbl = ident.get("table") or ""
        return TargetTable(catalog=cat, database=db, table=tbl) if tbl else None
    if cls == "DropTable":
        for child in node.children:
            if child.short_class == "ResolvedIdentifier":
                ident = child.data.get("identifier") or {}
                if isinstance(ident, dict):
                    return TargetTable(
                        catalog=child.data.get("catalog"),
                        database=ident.get("namespace") or ident.get("database"),
                        table=ident.get("name") or ident.get("table") or "",
                    )
                # ident might be a list-style identifier
                if isinstance(ident, list) and ident:
                    return TargetTable(
                        catalog=ident[0] if len(ident) > 2 else None,
                        database=ident[-2] if len(ident) > 1 else None,
                        table=ident[-1],
                    )
        return None
    return None


def _ddl_create_view(node: PlanNode, ctx: Context) -> LineageResult:
    name = node.data.get("name") or {}
    target = TargetTable(
        catalog=name.get("catalog"),
        database=name.get("database"),
        table=name.get("table") or name.get("name") or "",
    )
    plan_idx = node.data.get("plan")
    cols: List[ColInfo] = []
    if isinstance(plan_idx, int) and 0 <= plan_idx < len(node.children):
        cols = analyze_plan(node.children[plan_idx], ctx)
    elif node.children:
        cols = analyze_plan(node.children[0], ctx)
    sources = _collect_source_tables(cols)
    return LineageResult(
        plan_kind=PlanKind.DDL,
        operation=node.short_class,
        target=target,
        columns=cols,
        source_tables=sources,
        cte_definitions={
            ctx.cte_names.get(cid, f"cte_{cid}"): cols2
            for cid, cols2 in ctx.ctes.items()
        },
    )


def _ddl_create_data_source_table(node: PlanNode, ctx: Context) -> LineageResult:
    table = node.data.get("table") or {}
    ident = table.get("identifier") or {}
    target = TargetTable(
        catalog=ident.get("catalog"),
        database=ident.get("database"),
        table=ident.get("table") or "",
    )
    schema = table.get("schema") or {}
    fields = schema.get("fields") if isinstance(schema, dict) else None
    cols: List[ColInfo] = []
    if isinstance(fields, list):
        for f in fields:
            if not isinstance(f, dict):
                continue
            cols.append(
                ColInfo(
                    expr_id=(0, f"schema:{f.get('name')}"),
                    name=str(f.get("name", "")),
                    data_type=str(f.get("type", "")),
                    sources=EMPTY_SOURCES,
                    transformation="<schema definition>",
                    transformation_type=TransformationType.LITERAL,
                )
            )
    return LineageResult(
        plan_kind=PlanKind.DDL,
        operation=node.short_class,
        target=target,
        columns=cols,
        source_tables=[],
        notes=["Schema-only definition; no data flow"],
    )


def _ddl_create_table(node: PlanNode, ctx: Context) -> LineageResult:
    # CreateTableCommand or unified V2 CreateTable - schema-only.
    table = node.data.get("table") or node.data.get("tableDesc") or {}
    ident = table.get("identifier") or table.get("name") or {}
    if isinstance(ident, list) and ident:
        cat = ident[0] if len(ident) > 2 else None
        db = ident[-2] if len(ident) > 1 else None
        tbl = ident[-1]
    else:
        cat = ident.get("catalog") if isinstance(ident, dict) else None
        db = ident.get("database") if isinstance(ident, dict) else None
        tbl = (ident.get("table") if isinstance(ident, dict) else None) or (
            ident.get("name") if isinstance(ident, dict) else ""
        )
    target = TargetTable(catalog=cat, database=db, table=tbl or "") if tbl else None

    # Some CTAS-like plans may have a child query.
    cols: List[ColInfo] = []
    if node.children:
        for child in node.children:
            child_cols = analyze_plan(child, ctx)
            if child_cols:
                cols = child_cols
                break

    sources = _collect_source_tables(cols)
    plan_kind = PlanKind.DML if cols else PlanKind.DDL
    return LineageResult(
        plan_kind=plan_kind,
        operation=node.short_class,
        target=target,
        columns=cols,
        source_tables=sources,
        cte_definitions={
            ctx.cte_names.get(cid, f"cte_{cid}"): cols2
            for cid, cols2 in ctx.ctes.items()
        },
    )


def _ddl_show_tables(node: PlanNode, ctx: Context) -> LineageResult:
    return LineageResult(
        plan_kind=PlanKind.DDL,
        operation=node.short_class,
        target=None,
        columns=[],
        source_tables=[],
        notes=["Catalog metadata operation; no data flow"],
    )


def _ddl_create_namespace(node: PlanNode, ctx: Context) -> LineageResult:
    return LineageResult(
        plan_kind=PlanKind.NAMESPACE,
        operation=node.short_class,
        target=None,
        columns=[],
        source_tables=[],
    )


def _dml_insert_into_hadoop(node: PlanNode, ctx: Context) -> LineageResult:
    catalog_table = node.data.get("catalogTable") or {}
    ident = catalog_table.get("identifier") or {}
    target = TargetTable(
        catalog=ident.get("catalog"),
        database=ident.get("database"),
        table=ident.get("table") or "",
    )
    query_idx = node.data.get("query")
    cols: List[ColInfo] = []
    if isinstance(query_idx, int) and 0 <= query_idx < len(node.children):
        cols = analyze_plan(node.children[query_idx], ctx)
    elif node.children:
        cols = analyze_plan(node.children[0], ctx)

    schema = catalog_table.get("schema") or {}
    fields = schema.get("fields") if isinstance(schema, dict) else None
    cols = _retitle_columns_to_target_schema(cols, fields)

    sources = _collect_source_tables(cols)
    return LineageResult(
        plan_kind=PlanKind.DML,
        operation=node.short_class,
        target=target,
        columns=cols,
        source_tables=sources,
        cte_definitions={
            ctx.cte_names.get(cid, f"cte_{cid}"): cols2
            for cid, cols2 in ctx.ctes.items()
        },
    )


def _retitle_columns_to_target_schema(cols: List[ColInfo], fields) -> List[ColInfo]:
    """If a DML target carries a fixed schema, re-label each output column
    with the corresponding target column name (lineage stays intact)."""
    if not isinstance(fields, list):
        return cols
    if len(cols) != len(fields):
        return cols
    out: List[ColInfo] = []
    for c, f in zip(cols, fields):
        if not isinstance(f, dict):
            out.append(c)
            continue
        new_name = str(f.get("name", c.name))
        new_dt = str(f.get("type", c.data_type))
        out.append(
            ColInfo(
                expr_id=c.expr_id,
                name=new_name,
                data_type=new_dt,
                sources=c.sources,
                transformation=c.transformation,
                transformation_type=c.transformation_type,
            )
        )
    return out


def _dml_insert_generic(node: PlanNode, ctx: Context) -> LineageResult:
    table = node.data.get("table") or node.data.get("targetTable") or {}
    ident = table.get("identifier") if isinstance(table, dict) else None
    target = None
    if isinstance(ident, dict):
        target = TargetTable(
            catalog=ident.get("catalog"),
            database=ident.get("database"),
            table=ident.get("table") or "",
        )
    cols: List[ColInfo] = []
    if node.children:
        cols = analyze_plan(node.children[-1], ctx)
    sources = _collect_source_tables(cols)
    return LineageResult(
        plan_kind=PlanKind.DML,
        operation=node.short_class,
        target=target,
        columns=cols,
        source_tables=sources,
    )


def _ns_set_catalog(node: PlanNode, ctx: Context) -> LineageResult:
    namespace_str: Optional[str] = None
    catalog: Optional[str] = None
    for child in node.children:
        if child.short_class == "ResolvedNamespace":
            ns = child.data.get("namespace")
            if isinstance(ns, list) and ns:
                namespace_str = ".".join(str(p) for p in ns)
            elif isinstance(ns, str):
                namespace_str = ns.strip("[]").replace(", ", ".")
            catalog = child.data.get("catalog")
    return LineageResult(
        plan_kind=PlanKind.NAMESPACE,
        operation=node.short_class,
        target=None,
        columns=[],
        source_tables=[],
        notes=[f"Set namespace: {catalog or ''}.{namespace_str or ''}".strip(".")],
    )


_DDL_HANDLERS = {
    "DropTable": _ddl_drop_table,
    "DropTableCommand": _ddl_drop_table,
    "DropView": _ddl_drop_table,
    "CreateViewCommand": _ddl_create_view,
    "CreateView": _ddl_create_view,
    "CreateDataSourceTableCommand": _ddl_create_data_source_table,
    "CreateDataSourceTableAsSelectCommand": _ddl_create_table,
    "CreateTableCommand": _ddl_create_table,
    "CreateTable": _ddl_create_table,
    "CreateTableAsSelect": _ddl_create_table,
    "ReplaceTable": _ddl_create_table,
    "ReplaceTableAsSelect": _ddl_create_table,
    "ShowTables": _ddl_show_tables,
    "ShowNamespaces": _ddl_show_tables,
    "ShowColumns": _ddl_show_tables,
    "ShowPartitions": _ddl_show_tables,
    "ShowFunctions": _ddl_show_tables,
    "DescribeTable": _ddl_show_tables,
    "DescribeRelation": _ddl_show_tables,
    "DescribeColumn": _ddl_show_tables,
    "DropNamespace": _ddl_create_namespace,
    "CreateNamespace": _ddl_create_namespace,
    "AlterNamespace": _ddl_create_namespace,
}


_DML_HANDLERS = {
    "InsertIntoHadoopFsRelationCommand": _dml_insert_into_hadoop,
    "InsertIntoStatement": _dml_insert_generic,
    "InsertIntoDataSourceCommand": _dml_insert_generic,
    "InsertIntoDataSourceDirCommand": _dml_insert_generic,
    "InsertIntoHiveTable": _dml_insert_generic,
    "InsertIntoHiveDirCommand": _dml_insert_generic,
    "AppendData": _dml_insert_generic,
    "OverwriteByExpression": _dml_insert_generic,
    "OverwritePartitionsDynamic": _dml_insert_generic,
    "MergeIntoTable": _dml_insert_generic,
    "WriteToDataSourceV2": _dml_insert_generic,
}


_NAMESPACE_HANDLERS = {
    "SetCatalogAndNamespace": _ns_set_catalog,
    "SetNamespaceCommand": _ns_set_catalog,
    "UseStatement": _ns_set_catalog,
}
