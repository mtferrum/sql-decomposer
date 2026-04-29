"""End-to-end verification: run each test_sql/*.sql against Spark to build
the database state, and for every query that has a captured plan in
test_json/, also run the converted SQL produced by ``plan_to_sql`` and check
that the results match."""

from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any, List, Optional, Tuple

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from plan_to_sql import plan_to_sql  # noqa: E402

# silence verbose logs before we import spark
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 1g pyspark-shell")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402


def make_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("plan_to_sql_verify")
        .config("spark.sql.session.timeZone", "Europe/Moscow")
        .config("spark.sql.warehouse.dir",
                str((ROOT / "spark-warehouse").resolve()))
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={(ROOT / 'spark-metastore').resolve()};create=true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def is_query(sql: str) -> bool:
    s = sql.lstrip().lower()
    return s.startswith("select") or s.startswith("with") or s.startswith("(")


def split_statements(sql: str) -> List[str]:
    """Trivial splitter on top-level ``;``."""
    out: List[str] = []
    cur = []
    for line in sql.splitlines():
        cur.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(cur).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            cur = []
    if cur:
        stmt = "\n".join(cur).strip().rstrip(";").strip()
        if stmt:
            out.append(stmt)
    return out


def normalise(rows: List[Any], ordered: bool) -> List[Tuple[Any, ...]]:
    norm = [tuple(repr(c) for c in r) for r in rows]
    return norm if ordered else sorted(norm)


def run_one(spark: SparkSession, sql: str) -> Optional[List[Any]]:
    df = spark.sql(sql)
    return df.collect()


def main() -> int:
    spark = make_spark()
    test_sql_dir = ROOT / "test_sql"
    test_json_dir = ROOT / "test_json"

    files = sorted(test_sql_dir.glob("*.sql"))

    matches: List[str] = []
    mismatches: List[Tuple[str, str]] = []
    skipped: List[str] = []
    failed_orig: List[Tuple[str, str]] = []
    failed_rest: List[Tuple[str, str]] = []

    for sql_path in files:
        name = sql_path.stem
        original_text = sql_path.read_text()
        statements = split_statements(original_text)
        if not statements:
            skipped.append(name)
            continue
        # Tests are 1 statement per file; if multiple, treat the LAST as the
        # query of interest.
        body_stmt = statements[-1]
        setup_stmts = statements[:-1]

        # Run setup statements (these change state for both versions).
        for st in setup_stmts:
            try:
                spark.sql(st)
            except Exception as exc:
                failed_orig.append((name, f"setup: {exc}"))
                break

        is_select = is_query(body_stmt)

        # Original execution -------------------------------------------------
        original_rows: Optional[List[Any]] = None
        original_failed: Optional[Exception] = None
        try:
            if is_select:
                original_rows = run_one(spark, body_stmt)
            else:
                spark.sql(body_stmt)
        except Exception as exc:
            original_failed = exc
            failed_orig.append((name, str(exc).splitlines()[0]))

        # Restored execution -------------------------------------------------
        json_path = test_json_dir / (name + ".json")
        if not json_path.exists():
            skipped.append(name + " (no json)")
            continue
        plan_text = json_path.read_text()
        if "OuterReference" in plan_text:
            # The capture only contains a correlated subquery fragment; the
            # restored SQL cannot run as a stand-alone query.
            skipped.append(name + " (correlated subquery fragment)")
            continue
        try:
            plan = json.loads(plan_text)
            restored = plan_to_sql(plan).rstrip(";").strip()
        except Exception as exc:
            failed_rest.append((name, f"convert: {exc}"))
            continue

        if not is_select:
            # We only verify SELECT statements -- DDL/DML payloads cannot be
            # round-tripped because the JSON capture omits identifiers / rows.
            skipped.append(name)
            continue

        try:
            restored_rows = run_one(spark, restored)
        except Exception as exc:
            failed_rest.append((name, f"exec: {str(exc).splitlines()[0]}"))
            continue

        if original_failed is not None:
            # If the original failed, the restored should also error out for
            # the same reason -- a successful restore is unexpected.
            mismatches.append((name, "original failed but restored ran"))
            continue

        if original_rows is None:
            skipped.append(name + " (no rows)")
            continue

        ordered = "order by" in body_stmt.lower()
        a = normalise(original_rows, ordered)
        b = normalise(restored_rows, ordered)
        if a == b:
            matches.append(name)
        else:
            mismatches.append((name, f"orig={a[:3]} rest={b[:3]}"))

    total_select = len(matches) + len(mismatches) + sum(
        1 for n, _ in failed_rest if not n.startswith("setup")
    )
    print()
    print("=" * 60)
    print(f"Matched: {len(matches)}/{total_select}")
    print(f"Mismatched: {len(mismatches)}")
    print(f"Restored exec/convert errors: {len(failed_rest)}")
    print(f"Original errors: {len(failed_orig)}")
    print(f"Skipped (DDL/DML/missing): {len(skipped)}")
    print()
    if mismatches:
        print("--- Mismatches ---")
        for name, info in mismatches:
            print(f"  {name}: {info}")
    if failed_rest:
        print("--- Restored failures ---")
        for name, info in failed_rest:
            print(f"  {name}: {info}")
    if failed_orig:
        print("--- Original failures (informational) ---")
        for name, info in failed_orig:
            print(f"  {name}: {info}")

    return 0 if not mismatches and not failed_rest else 1


if __name__ == "__main__":
    raise SystemExit(main())
