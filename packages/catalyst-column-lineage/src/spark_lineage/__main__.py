"""CLI entry: `python -m spark_lineage path/to/plan.json|query.sql [...]`.

Examples:
    python -m spark_lineage test_json/031_04_cte_and_aggregations_03.json
    python -m spark_lineage test_sql/031_04_cte_and_aggregations_03.sql
    python -m spark_lineage --dir test_json --out lineage_out
    python -m spark_lineage --dir test_sql  --out lineage_out --format html
    python -m spark_lineage --dir test_sql  --out lineage_svg --format svg \
        --setup-from test_sql/001_*.sql --setup-from test_sql/002_*.sql

Input formats:
    *.json - Spark Catalyst LogicalPlan JSON dump (default)
    *.sql  - SQL query, resolved through PySpark (optional dependency).
             Pass `--language json|sql|auto` to override extension-based
             detection.

Output formats:
    json  - one JSON document per file (default)
    text  - human-readable summary
    svg   - standalone SVG visualisation
    dot   - Graphviz DOT description (render via `dot -Tsvg in.dot -o out.svg`)
    html  - single HTML document embedding the SVG and the JSON details

When `--dir` is used together with SQL inputs the files are processed in
sorted order **inside the same SparkSession**, so a sequence such as
``001_create.sql`` → ``014_insert.sql`` → ``030_select.sql`` shares the
catalog state it needs to resolve. Use `--warehouse` / `--metastore` to
point at an existing Spark catalog.

If `--out` is provided, results for each input file are written into the
output directory using the same base name with a `.lineage.<ext>` suffix;
otherwise everything is dumped to stdout.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

from .api import build_lineage
from .models import LineageResult
from .visualize import render_dot, render_html, render_svg, render_text


_SQL_EXT = {".sql"}
_JSON_EXT = {".json"}


def _detect_language(path: Path, override: str) -> str:
    if override != "auto":
        return override
    suffix = path.suffix.lower()
    if suffix in _SQL_EXT:
        return "sql"
    if suffix in _JSON_EXT:
        return "json"
    return "auto"


def _process_one(
    file_path: Path,
    *,
    language: str,
    sql_kwargs: dict,
) -> Tuple[str, LineageResult]:
    lang = _detect_language(file_path, language)
    text = file_path.read_text(encoding="utf-8")
    if lang == "sql":
        return file_path.name, build_lineage(
            text, language="sql", sql_kwargs=sql_kwargs
        )
    if lang == "json":
        return file_path.name, build_lineage(json.loads(text))
    return file_path.name, build_lineage(text, sql_kwargs=sql_kwargs)


def _files_from_args(args: argparse.Namespace) -> List[Path]:
    files: List[Path] = []
    if args.dir:
        d = Path(args.dir)
        patterns = ("*.json", "*.sql") if args.language in {"auto"} else (
            ("*.sql",) if args.language == "sql" else ("*.json",)
        )
        for pat in patterns:
            files.extend(sorted(d.glob(pat)))
    files.extend(Path(p) for p in args.inputs)
    return files


_EXT = {"json": "json", "text": "txt", "svg": "svg", "dot": "dot", "html": "html"}


def _render(fmt: str, name: str, result: LineageResult) -> str:
    if fmt == "json":
        return json.dumps(
            {"file": name, "lineage": result.to_dict()},
            ensure_ascii=False,
            indent=2,
        )
    if fmt == "text":
        return render_text(result, file_name=name)
    if fmt == "svg":
        return render_svg(result)
    if fmt == "dot":
        return render_dot(result)
    if fmt == "html":
        return render_html(result, title=f"Column lineage — {name}")
    raise ValueError(f"Unknown format: {fmt!r}")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="spark_lineage",
        description="Build column-level lineage from Spark Catalyst LogicalPlan JSON dumps or SQL text.",
    )
    parser.add_argument("inputs", nargs="*", help="Plan JSON files or SQL files to analyse.")
    parser.add_argument("--dir", help="Directory of *.json / *.sql files.")
    parser.add_argument("--out", help="Optional output directory.")
    parser.add_argument(
        "--format",
        choices=tuple(_EXT.keys()),
        default="json",
    )
    parser.add_argument(
        "--language",
        choices=("auto", "json", "sql"),
        default="auto",
        help="How to interpret each input. Default: auto (by file extension).",
    )
    parser.add_argument(
        "--warehouse",
        help="spark.sql.warehouse.dir path (used only for SQL inputs).",
    )
    parser.add_argument(
        "--metastore",
        help="Derby metastore directory (used only for SQL inputs).",
    )
    parser.add_argument(
        "--no-hive",
        action="store_true",
        help="Disable enableHiveSupport() when building the local SparkSession.",
    )
    parser.add_argument(
        "--setup-from",
        action="append",
        default=[],
        metavar="PATH",
        help=(
            "SQL file to execute before each main query (repeatable). "
            "Useful for CREATE/INSERT statements."
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat any analysis error as fatal (default: log and skip).",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    files = _files_from_args(args)
    if not files:
        parser.error("No input files. Pass paths or --dir.")

    out_dir = Path(args.out) if args.out else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    setup_sql: List[str] = []
    for sp in args.setup_from:
        setup_sql.append(Path(sp).read_text(encoding="utf-8"))

    sql_kwargs: dict = {
        "warehouse": args.warehouse,
        "metastore": args.metastore,
        "enable_hive": not args.no_hive,
    }
    if setup_sql:
        sql_kwargs["setup"] = setup_sql

    rc = 0
    for fp in files:
        try:
            name, result = _process_one(
                fp,
                language=args.language,
                sql_kwargs=sql_kwargs,
            )
        except Exception as exc:  # pragma: no cover - depends on input
            rc = 1
            msg = f"[ERROR] {fp}: {exc}"
            if args.strict:
                raise
            print(msg, file=sys.stderr)
            continue

        payload = _render(args.format, name, result)
        ext = _EXT[args.format]

        if out_dir:
            (out_dir / f"{fp.stem}.lineage.{ext}").write_text(payload, encoding="utf-8")
        else:
            print(payload)

    return rc


if __name__ == "__main__":
    sys.exit(main())
