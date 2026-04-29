"""SQL ↔ JSON lineage parity test.

For every pair of files that share a stem inside ``test_sql`` and
``test_json`` we:

1. build a `LineageResult` from the JSON dump (the "reference"),
2. open a single Spark session with a **fresh** warehouse / metastore
   so that we control the catalog state from scratch,
3. iterate ``test_sql/*.sql`` in lexicographic order, executing
   each statement against that session — this rebuilds the same
   schema + tables that produced the JSON dumps,
4. for the files we want to verify, capture the analyzed plan from
   PySpark and feed it through ``spark_lineage.build_lineage(...)``,
5. compare the lineage produced from SQL to the lineage produced from
   JSON.

The comparison is on the **stable** payload returned by
``LineageResult.to_dict()`` — column names, data types, transformation
text, transformation type, and the FQN-sorted set of source columns.
ExprIds are intentionally not in that payload so a cold rerun of Spark
matches the original capture.

Run with the project venv:

    .venv/bin/python test_sql_vs_json_parity.py
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from spark_lineage import build_lineage  # noqa: E402
from spark_lineage.sql import default_session  # noqa: E402

TEST_SQL_DIR = ROOT / "test_sql"
TEST_JSON_DIR = ROOT / "test_json"


# Some SQL files in the suite are scratch files unrelated to the JSON
# captures (no JSON twin) — skip them.
_SKIP_NAMES = {"select_1.sql", "show_tabels.sql"}


def _normalise(d: dict) -> dict:
    """Drop fields that are inherently non-deterministic across reruns.

    The current `LineageResult.to_dict()` is already stable (no exprIds,
    no jvmIds, sources sorted by FQN). We additionally:
      * drop the createTime / random suffix bits if any handler added
        them as notes (none currently do, but it makes the test
        forward-compatible);
      * sort columns by name to ignore any reordering inside Aggregate
        when grouping ordinals shift.
    """
    out = json.loads(json.dumps(d, sort_keys=True))
    return out


def _key(col: dict) -> tuple:
    return (col.get("name", ""), col.get("ordinal", 0))


def _diff_columns(
    actual: List[dict], expected: List[dict]
) -> List[str]:
    msgs: List[str] = []
    a_by = {_key(c): c for c in actual}
    e_by = {_key(c): c for c in expected}
    extra = set(a_by) - set(e_by)
    missing = set(e_by) - set(a_by)
    for k in sorted(missing):
        msgs.append(f"missing column {k!r}")
    for k in sorted(extra):
        msgs.append(f"unexpected column {k!r}")
    for k in sorted(set(a_by) & set(e_by)):
        a, e = a_by[k], e_by[k]
        for field in ("name", "data_type", "transformation_type", "transformation"):
            if a.get(field) != e.get(field):
                msgs.append(
                    f"col {k}: {field}: SQL={a.get(field)!r} JSON={e.get(field)!r}"
                )
        if sorted(s.get("fqn", "") for s in a.get("sources", [])) != sorted(
            s.get("fqn", "") for s in e.get("sources", [])
        ):
            msgs.append(
                f"col {k}: sources differ: "
                f"SQL={[s.get('fqn') for s in a.get('sources', [])]} "
                f"JSON={[s.get('fqn') for s in e.get('sources', [])]}"
            )
    return msgs


def _diff_results(actual: dict, expected: dict) -> List[str]:
    msgs: List[str] = []
    for f in ("plan_kind", "operation"):
        if actual.get(f) != expected.get(f):
            msgs.append(f"{f}: SQL={actual.get(f)!r} JSON={expected.get(f)!r}")
    if (actual.get("target_table") or {}).get("fqn") != (
        expected.get("target_table") or {}
    ).get("fqn"):
        msgs.append(
            f"target_table: SQL={actual.get('target_table')} "
            f"JSON={expected.get('target_table')}"
        )
    if sorted(actual.get("source_tables") or []) != sorted(
        expected.get("source_tables") or []
    ):
        msgs.append(
            f"source_tables: SQL={actual.get('source_tables')} "
            f"JSON={expected.get('source_tables')}"
        )
    msgs.extend(_diff_columns(actual.get("columns", []), expected.get("columns", [])))
    a_ctes = actual.get("ctes", {}) or {}
    e_ctes = expected.get("ctes", {}) or {}
    for name in sorted(set(a_ctes) | set(e_ctes)):
        if name not in a_ctes:
            msgs.append(f"missing CTE {name!r}")
            continue
        if name not in e_ctes:
            msgs.append(f"unexpected CTE {name!r}")
            continue
        msgs.extend(
            f"CTE {name}: {m}" for m in _diff_columns(a_ctes[name], e_ctes[name])
        )
    return msgs


def _capture_plan(spark, sql: str) -> dict:
    df = spark.sql(sql)
    plan = df._jdf.queryExecution().analyzed()
    return json.loads(plan.toJSON())


def main() -> int:
    sql_files = sorted(p for p in TEST_SQL_DIR.glob("*.sql") if p.name not in _SKIP_NAMES)
    json_pairs: Dict[str, Path] = {p.stem: p for p in TEST_JSON_DIR.glob("*.json")}

    if not sql_files:
        print("No SQL files found", file=sys.stderr)
        return 1

    tmp = Path(tempfile.mkdtemp(prefix="spark_lineage_parity_"))
    warehouse = tmp / "warehouse"
    metastore = tmp / "metastore"
    print(f"Using fresh Spark catalog at {tmp}")

    try:
        spark = default_session(
            warehouse=str(warehouse),
            metastore=str(metastore),
            enable_hive=True,
            app_name="spark_lineage_parity_test",
        )
        spark.sparkContext.setLogLevel("ERROR")

        total = 0
        compared = 0
        skipped: List[Tuple[str, str]] = []
        failures: List[Tuple[str, List[str]]] = []
        sql_errors: List[Tuple[str, str]] = []

        for sql_path in sql_files:
            total += 1
            stem = sql_path.stem
            sql_text = sql_path.read_text(encoding="utf-8")
            try:
                plan_list = _capture_plan(spark, sql_text)
            except Exception as exc:
                sql_errors.append((sql_path.name, str(exc)))
                continue

            json_path = json_pairs.get(stem)
            if json_path is None:
                skipped.append((sql_path.name, "no JSON twin"))
                continue
            try:
                expected_plan = json.loads(json_path.read_text(encoding="utf-8"))
                expected_lineage = build_lineage(expected_plan)
                actual_lineage = build_lineage(plan_list)
            except Exception as exc:
                failures.append((sql_path.name, [f"build_lineage failed: {exc}"]))
                continue

            diff = _diff_results(
                _normalise(actual_lineage.to_dict()),
                _normalise(expected_lineage.to_dict()),
            )
            compared += 1
            if diff:
                failures.append((sql_path.name, diff))

        print()
        print(f"Processed {total} SQL files, compared {compared} pairs.")
        if skipped:
            print(f"Skipped {len(skipped)} pairs (no JSON twin):")
            for name, why in skipped[:10]:
                print(f"  - {name}: {why}")
            if len(skipped) > 10:
                print(f"  ... +{len(skipped) - 10} more")
        if sql_errors:
            print(f"\n{len(sql_errors)} SQL files raised errors during analysis:")
            for name, msg in sql_errors[:20]:
                print(f"  ! {name}: {msg.splitlines()[0]}")
            if len(sql_errors) > 20:
                print(f"  ... +{len(sql_errors) - 20} more")
        if failures:
            print(f"\n{len(failures)} parity failures:")
            for name, diffs in failures:
                print(f"  ✗ {name}")
                for d in diffs:
                    print(f"      {d}")
            return 1

        print("\nAll lineage results match between SQL and JSON inputs.")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
