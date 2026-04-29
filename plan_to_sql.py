"""Backwards-compatible shim.

The implementation moved to the ``spark_plan_to_sql`` package.  This module
re-exports the public API and exposes the CLI entry point so existing scripts
that ``import plan_to_sql`` keep working unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path

# When running the source tree directly (no install), make ``src/`` importable.
_SRC = Path(__file__).resolve().parent / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from spark_plan_to_sql import (  # noqa: E402
    PlanCompiler,
    ExprCompiler,
    Node,
    Select,
    parse_plan,
    parse_expr_chain,
    plan_to_sql,
    dict_to_sql,
)
from spark_plan_to_sql.cli import main  # noqa: E402

__all__ = [
    "plan_to_sql",
    "dict_to_sql",
    "Node",
    "Select",
    "PlanCompiler",
    "ExprCompiler",
    "parse_plan",
    "parse_expr_chain",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
