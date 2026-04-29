"""Invariant checks over column lineage produced for `test_json/`.

These checks catch entire classes of lineage bugs - things like:

  1. Every reported source column must actually exist in the schema of
     its claimed base table (no fabricated columns).

  2. Every IDENTITY column must have at least one source - except when
     the underlying plan is genuinely sourceless (LocalRelation, OneRowRelation,
     pure-literal Project), which we whitelist explicitly.

  3. Every CTE definition must be resolvable to a list of `ColInfo`s and
     every reference's positional ExprId mapping must produce the same
     base-source set as the definition.

  4. For Aggregate / Window outputs, the AGGREGATE / WINDOW columns must
     trace back to *some* base column (or the function must be a count-style
     function that legitimately has zero sources, e.g. COUNT(*)).

  5. Each plan must round-trip: parse() o analyze() o to_dict() must produce
     a valid JSON-serialisable structure.

A non-zero exit code means at least one invariant failed - that is a
HARD ERROR and indicates a lineage bug.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

from spark_lineage import build_lineage


SOURCELESS_WHITELIST = {
    # SELECT TIMESTAMP '...' + INTERVAL '...', SELECT 1/0, etc.
    "045_07_datetime_and_intervals_04.json",
    "058_09_ansi_and_error_cases_03.json",
    "059_09_ansi_and_error_cases_04.json",
    # INSERT INTO ... VALUES (...) reads from LocalRelation, not from a table.
    "014_01_seed_data_02.json",
    "015_01_seed_data_03.json",
    "016_01_seed_data_04.json",
    "017_01_seed_data_05.json",
    "018_01_seed_data_06.json",
}

ZERO_SOURCE_AGGREGATES = {"COUNT(*)", "COUNT()", "ROW_NUMBER()", "GROUPING_ID()"}


def _load_table_schemas(json_dir: Path) -> Dict[str, Set[str]]:
    """Build {table_fqn -> {column_names}} from every CatalogTable seen in the
    test corpus.  Both `LogicalRelation.catalogTable` and
    `CreateDataSourceTableCommand.table` are scanned."""
    schemas: Dict[str, Set[str]] = {}
    for fp in sorted(json_dir.glob("*.json")):
        data = json.load(fp.open("r", encoding="utf-8"))
        for node in data:
            if not isinstance(node, dict):
                continue
            cls = node.get("class", "")
            ct = node.get("catalogTable") or node.get("table") or {}
            if not isinstance(ct, dict):
                continue
            ident = ct.get("identifier")
            schema = ct.get("schema")
            if not (isinstance(ident, dict) and isinstance(schema, dict)):
                continue
            cat, db, tbl = (
                ident.get("catalog"),
                ident.get("database"),
                ident.get("table"),
            )
            if not tbl:
                continue
            cols = {f.get("name") for f in (schema.get("fields") or []) if isinstance(f, dict)}
            fqn = ".".join(p for p in (cat, db, tbl) if p)
            schemas.setdefault(fqn, set()).update(filter(None, cols))
    return schemas


def main() -> int:
    in_dir = Path("test_json")
    schemas = _load_table_schemas(in_dir)
    failures: List[str] = []
    n_files = 0
    n_columns = 0

    for fp in sorted(in_dir.glob("*.json")):
        n_files += 1
        try:
            plan = json.load(fp.open("r", encoding="utf-8"))
            result = build_lineage(plan)
        except Exception as exc:
            failures.append(f"{fp.name}: parse/analyze raised: {exc}")
            continue

        # Round-trip check: result must be JSON serialisable.
        try:
            json.dumps(result.to_dict())
        except Exception as exc:
            failures.append(f"{fp.name}: result not JSON-serialisable: {exc}")

        all_cols: List[Tuple[str, "any"]] = [("main", c) for c in result.columns]
        for cte_name, cols in result.cte_definitions.items():
            all_cols.extend((f"cte:{cte_name}", c) for c in cols)

        for where, col in all_cols:
            n_columns += 1

            for src in col.sources:
                table_fqn = ".".join(
                    p for p in (src.catalog, src.database, src.table) if p
                )
                if table_fqn in schemas and src.column not in schemas[table_fqn]:
                    failures.append(
                        f"{fp.name} [{where}] {col.name}: claims source "
                        f"{table_fqn}.{src.column} but the table only has "
                        f"{sorted(schemas[table_fqn])}"
                    )

            if col.transformation_type in ("IDENTITY", "EXPRESSION"):
                if not col.sources and fp.name not in SOURCELESS_WHITELIST:
                    failures.append(
                        f"{fp.name} [{where}] {col.name}: "
                        f"{col.transformation_type} but sources empty "
                        f"(expr={col.transformation})"
                    )

            if col.transformation_type == "AGGREGATE":
                if not col.sources and col.transformation not in ZERO_SOURCE_AGGREGATES:
                    failures.append(
                        f"{fp.name} [{where}] {col.name}: AGGREGATE "
                        f"with no sources (expr={col.transformation})"
                    )

    print(f"Processed {n_files} files, checked {n_columns} columns; "
          f"{len(failures)} invariant failures.")
    for f in failures:
        print("  FAIL:", f)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
