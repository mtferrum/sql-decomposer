"""Core data classes used by the lineage analyzer.

Lineage is tracked per output column (`ColInfo`):
    - `expr_id`     - Catalyst's (id, jvmId) for the column at the current node.
    - `name`        - column name as visible at the current scope.
    - `data_type`   - resolved Catalyst data type as it appears at the leaf where
                       the column was last (re)typed.
    - `sources`     - frozen set of base table columns (`SourceColumn`) that
                       contribute to the value (empty for pure literals or
                       synthetic columns like `spark_grouping_id`).
    - `transformation`     - human-readable expression text reconstructed from
                              the Catalyst expression tree.
    - `transformation_type` - coarse classification: IDENTITY / EXPRESSION /
                              AGGREGATE / WINDOW / LITERAL / GROUPING / GENERATOR.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, Tuple


ExprIdKey = Tuple[int, str]


class TransformationType:
    IDENTITY = "IDENTITY"
    EXPRESSION = "EXPRESSION"
    AGGREGATE = "AGGREGATE"
    WINDOW = "WINDOW"
    LITERAL = "LITERAL"
    GROUPING = "GROUPING"
    GENERATOR = "GENERATOR"
    OUTER_REFERENCE = "OUTER_REFERENCE"


class PlanKind:
    QUERY = "QUERY"
    DDL = "DDL"
    DML = "DML"
    NAMESPACE = "NAMESPACE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class SourceColumn:
    catalog: Optional[str]
    database: Optional[str]
    table: str
    column: str

    @property
    def fqn(self) -> str:
        parts = [p for p in (self.catalog, self.database, self.table, self.column) if p]
        return ".".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "catalog": self.catalog,
            "database": self.database,
            "table": self.table,
            "column": self.column,
            "fqn": self.fqn,
        }


@dataclass(frozen=True)
class TargetTable:
    catalog: Optional[str]
    database: Optional[str]
    table: str

    @property
    def fqn(self) -> str:
        parts = [p for p in (self.catalog, self.database, self.table) if p]
        return ".".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "catalog": self.catalog,
            "database": self.database,
            "table": self.table,
            "fqn": self.fqn,
        }


@dataclass
class ColInfo:
    expr_id: ExprIdKey
    name: str
    data_type: str
    sources: FrozenSet[SourceColumn]
    transformation: str
    transformation_type: str

    def with_name(self, new_name: str, new_expr_id: ExprIdKey) -> "ColInfo":
        return ColInfo(
            expr_id=new_expr_id,
            name=new_name,
            data_type=self.data_type,
            sources=self.sources,
            transformation=self.transformation,
            transformation_type=self.transformation_type,
        )

    def to_dict(self, ordinal: int) -> Dict[str, Any]:
        return {
            "ordinal": ordinal,
            "name": self.name,
            "data_type": self.data_type,
            "transformation_type": self.transformation_type,
            "expression": self.transformation,
            "sources": sorted(
                (s.to_dict() for s in self.sources),
                key=lambda d: d["fqn"],
            ),
        }


@dataclass
class PlanNode:
    """Plan node parsed from the pre-order JSON list.

    `data` keeps the raw dict (with inline expression-list fields).
    `children` are the materialized child plan subtrees.
    """

    class_name: str
    short_class: str
    data: Dict[str, Any]
    children: List["PlanNode"] = field(default_factory=list)


@dataclass
class ExprNode:
    """Expression node parsed from an inline pre-order list."""

    class_name: str
    short_class: str
    data: Dict[str, Any]
    children: List["ExprNode"] = field(default_factory=list)


@dataclass
class LineageResult:
    plan_kind: str
    operation: str
    target: Optional[TargetTable]
    columns: List[ColInfo]
    source_tables: List[str]
    notes: List[str] = field(default_factory=list)
    cte_definitions: Dict[str, List[ColInfo]] = field(default_factory=dict)

    # --- Conversions ------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_kind": self.plan_kind,
            "operation": self.operation,
            "target_table": self.target.to_dict() if self.target else None,
            "source_tables": self.source_tables,
            "columns": [c.to_dict(i) for i, c in enumerate(self.columns)],
            "ctes": {
                name: [c.to_dict(i) for i, c in enumerate(cols)]
                for name, cols in self.cte_definitions.items()
            },
            "notes": self.notes,
        }

    def to_json(self, *, indent: Optional[int] = 2, ensure_ascii: bool = False) -> str:
        """Return a JSON string representation of `to_dict()`."""
        import json as _json

        return _json.dumps(self.to_dict(), indent=indent, ensure_ascii=ensure_ascii)

    def to_text(self) -> str:
        """Return the human-readable text summary."""
        from .visualize import render_text

        return render_text(self)

    def to_svg(self) -> str:
        """Return the standalone SVG visualisation."""
        from .visualize import render_svg

        return render_svg(self)

    def to_dot(self) -> str:
        """Return the Graphviz DOT description."""
        from .visualize import render_dot

        return render_dot(self)

    def to_html(self, *, title: str = "Column lineage") -> str:
        """Return an HTML page embedding the SVG and JSON details."""
        from .visualize import render_html

        return render_html(self, title=title)

    def save(self, path, *, format: Optional[str] = None):
        """Write the lineage to `path`, format inferred from extension by default.

        Supported extensions: `.json`, `.txt`, `.svg`, `.dot` / `.gv`, `.html`.
        Use the `format=` argument to override.
        """
        # Local import to avoid a circular dependency.
        from .api import save as _save

        return _save(self, path, format=format)
