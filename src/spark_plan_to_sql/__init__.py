"""Public API for the ``spark-plan-to-sql`` package.

This package converts Apache Spark Catalyst ``LogicalPlan`` JSON
serializations back into a logically equivalent SQL statement.

Typical usage::

    import json
    from spark_plan_to_sql import plan_to_sql

    with open("plan.json") as f:
        sql = plan_to_sql(json.load(f))
"""

from .converter import (
    PlanCompiler,
    ExprCompiler,
    Node,
    Select,
    parse_plan,
    parse_expr_chain,
    plan_to_sql,
    dict_to_sql,
)

__all__ = [
    "plan_to_sql",
    "dict_to_sql",
    "Node",
    "Select",
    "PlanCompiler",
    "ExprCompiler",
    "parse_plan",
    "parse_expr_chain",
]

__version__ = "0.1.0"
