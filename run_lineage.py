"""End-to-end runner: build column lineage for every plan in `test_json/`.

Writes per-file JSON and text reports under `lineage_out/`, then prints a
short summary table of operation, target, and column counts.

Usage:
    python run_lineage.py
    python run_lineage.py --print-text          # also dump text bodies
    python run_lineage.py --file test_json/030_04_cte_and_aggregations_02.json

The script is intentionally short - all logic lives in `spark_lineage/`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from spark_lineage import build_lineage


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", default="test_json")
    parser.add_argument("--out-dir", default="lineage_out")
    parser.add_argument("--file", action="append", default=[])
    parser.add_argument("--print-text", action="store_true")
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(Path(p) for p in args.file) or sorted(in_dir.glob("*.json"))
    rows = []
    for fp in files:
        try:
            with fp.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            result = build_lineage(data)
        except Exception as exc:
            print(f"[ERR] {fp.name}: {exc}", file=sys.stderr)
            rows.append((fp.name, "ERROR", "-", str(exc), 0))
            continue

        target_fqn = result.target.fqn if result.target else "-"
        sources = ", ".join(result.source_tables) or "-"
        rows.append(
            (
                fp.name,
                result.plan_kind,
                result.operation,
                target_fqn,
                sources,
                len(result.columns),
                len(result.cte_definitions),
            )
        )

        (out_dir / f"{fp.stem}.lineage.json").write_text(
            json.dumps(
                {"file": fp.name, "lineage": result.to_dict()},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    print(
        f"{'file':<55} {'kind':<10} {'operation':<28} {'target':<40} {'cols':>4} {'ctes':>4}"
    )
    print("-" * 140)
    for r in rows:
        if len(r) == 5:
            file, kind, op, msg, cols = r
            print(f"{file:<55} {kind:<10} {op:<28} {msg:<40} {cols:>4} {0:>4}")
        else:
            file, kind, op, target, sources, cols, ctes = r
            print(f"{file:<55} {kind:<10} {op:<28} {target:<40} {cols:>4} {ctes:>4}")
    print(f"\nProcessed {len(rows)} files; outputs in {out_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
