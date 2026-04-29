"""Parse Spark Catalyst LogicalPlan JSON dumps into trees.

The Spark `JsonProtocol` for `TreeNode` produces a flat pre-order list of
node dictionaries. Each dictionary carries `num-children: N`, meaning the
next N subtrees in the list are this node's children.

Internal expression trees that live inside a plan node (e.g. `condition`,
`projectList`, `groupingExpressions`, `windowExpressions`, `order`) are
serialized using the very same scheme - their value is itself a flat
pre-order list of expression dictionaries (or a list of such lists for
`Seq[Expression]` fields).

Children are referenced inside an expression node by ORDINAL among that
expression's own children (so `child: 0`, `left: 0`, `right: 1`,
`children: [0, 1]`, ...). Because the pre-order encoding consumes children
in declaration order, the analyzer rarely needs to look at those ordinals
- the resolved `ExprNode.children` already are in the right slots.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, List, Tuple, Union

from .models import ExprNode, PlanNode


PlanInput = Union[str, bytes, list, dict]


def _short(class_name: str) -> str:
    return class_name.rsplit(".", 1)[-1].rstrip("$")


def _is_expression_node(d: Any) -> bool:
    return isinstance(d, dict) and isinstance(d.get("class"), str)


def parse_expr_list(expr_list: List[dict]) -> ExprNode:
    """Parse a flat pre-order expression list into a tree."""

    idx = [0]

    def parse() -> ExprNode:
        if idx[0] >= len(expr_list):
            raise ValueError("Truncated expression list")
        node = expr_list[idx[0]]
        idx[0] += 1
        if not _is_expression_node(node):
            raise ValueError(f"Not an expression node: {node!r}")
        n = int(node.get("num-children", 0))
        children: List[ExprNode] = [parse() for _ in range(n)]
        return ExprNode(
            class_name=node["class"],
            short_class=_short(node["class"]),
            data=node,
            children=children,
        )

    root = parse()
    if idx[0] != len(expr_list):
        # Some Spark versions tail-pad with metadata; warn but ignore.
        # We still keep the root we successfully parsed.
        pass
    return root


def parse_expr_field(value: Any) -> Any:
    """Generic expression-field unpacker.

    A plan node field may be one of:
      * a primitive (None / str / int / bool / number / dict-without-class)
      * a flat pre-order list[dict-with-class]            -> single expression
      * a list[list[dict-with-class]]                     -> Seq[Expression]
      * a list[list[list[dict-with-class]]]               -> Seq[Seq[Expression]]
        (used by Expand.projections)

    Returns the parsed structure recursively, leaving primitives untouched.
    """

    if value is None:
        return None
    if isinstance(value, list):
        if not value:
            return []
        first = value[0]
        if _is_expression_node(first):
            return parse_expr_list(value)
        if isinstance(first, list):
            return [parse_expr_field(item) for item in value]
        # list of primitive ordinals or strings -> leave as-is
        return value
    return value


def parse_plan(plan_input: PlanInput) -> PlanNode:
    """Parse a flat pre-order plan list into a `PlanNode` tree.

    Accepts:
      * str / bytes - JSON text containing the list
      * list - already-decoded pre-order list
      * dict - either a single plan node, or a wrapper such as
        {"plan": [...]} / {"logicalPlan": [...]} / {"nodes": [...]}.
    """

    plan_list = _coerce_plan_list(plan_input)

    idx = [0]

    def parse() -> PlanNode:
        if idx[0] >= len(plan_list):
            raise ValueError("Truncated plan list")
        node = plan_list[idx[0]]
        idx[0] += 1
        if not _is_expression_node(node):
            raise ValueError(f"Not a plan node: {node!r}")
        n = int(node.get("num-children", 0))
        children = [parse() for _ in range(n)]
        return PlanNode(
            class_name=node["class"],
            short_class=_short(node["class"]),
            data=node,
            children=children,
        )

    root = parse()
    return root


def _coerce_plan_list(plan_input: PlanInput) -> List[dict]:
    if isinstance(plan_input, (bytes, bytearray)):
        plan_input = plan_input.decode("utf-8")
    if isinstance(plan_input, str):
        plan_input = json.loads(plan_input)
    if isinstance(plan_input, dict):
        for key in ("plan", "logicalPlan", "nodes", "tree"):
            if key in plan_input and isinstance(plan_input[key], list):
                return plan_input[key]
        if "class" in plan_input:
            return [plan_input]
        raise ValueError(
            "Dict plan input must contain a 'plan' / 'logicalPlan' / 'nodes' "
            "key or be a single plan node dict."
        )
    if isinstance(plan_input, list):
        return plan_input
    raise TypeError(f"Unsupported plan input type: {type(plan_input).__name__}")
