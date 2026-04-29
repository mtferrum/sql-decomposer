"""Command-line interface for ``spark-plan-to-sql``.

Usage::

    spark-plan-to-sql plan.json [plan2.json ...]      # print restored SQL to stdout
    spark-plan-to-sql --dir json_dir --out sql_dir    # batch convert a directory
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from .converter import plan_to_sql


def _convert_file(path: Path) -> str:
    return plan_to_sql(json.loads(path.read_text()))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spark-plan-to-sql",
        description="Restore SQL from Apache Spark Catalyst LogicalPlan JSON.",
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="One or more JSON plan files to convert (printed to stdout).",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="Directory containing *.json plan files to convert in batch mode.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output directory for batch mode (defaults to ./restored_sql).",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.dir is not None:
        src = args.dir
        if not src.is_dir():
            print(f"No directory {src}", file=sys.stderr)
            return 1
        out_dir = args.out or Path("restored_sql")
        out_dir.mkdir(parents=True, exist_ok=True)
        for f in sorted(src.glob("*.json")):
            try:
                sql = _convert_file(f)
            except Exception as exc:  # pragma: no cover - defensive
                sql = f"-- ERROR: {exc}\n"
            (out_dir / (f.stem + ".sql")).write_text(sql + "\n")
            head = sql.splitlines()[0][:80] if sql else ""
            print(f"{f.name} -> {head}")
        return 0

    if not args.files:
        parser.print_help(sys.stderr)
        return 2

    for a in args.files:
        p = Path(a)
        sql = _convert_file(p)
        print(f"-- {p.name}")
        print(sql)
        print()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
