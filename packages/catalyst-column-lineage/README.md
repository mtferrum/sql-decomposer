# catalyst-column-lineage

[![PyPI version](https://img.shields.io/pypi/v/catalyst-column-lineage.svg)](https://pypi.org/project/catalyst-column-lineage/)
[![Python versions](https://img.shields.io/pypi/pyversions/catalyst-column-lineage.svg)](https://pypi.org/project/catalyst-column-lineage/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Pure-Python, dependency-free **column-level data lineage** extractor for
Apache Spark. It consumes a Catalyst `LogicalPlan` JSON dump (the format
emitted by `LogicalPlan.toJSON` / `JsonProtocol`) and produces:

- a structured `LineageResult` data model (per-column sources and the
  reconstructed expression that produced each column),
- a self-contained **SVG** visualisation,
- a **Graphviz DOT** description,
- an **HTML** page embedding both the SVG and the JSON dump,
- a compact **plain-text** summary.

The package walks the plan tree bottom-up, resolves Catalyst `ExprId`s
through `CTERelationDef` / `CTERelationRef` remappings, and renders
SQL-like expressions for aliases, casts, literals, aggregates, window
functions, generators, and any unknown expression via a generic
fallback.

## Install

```bash
pip install catalyst-column-lineage
```

The import name is **`spark_lineage`** (the distribution name is
`catalyst-column-lineage`):

```python
import spark_lineage
```

No runtime dependencies. Python 3.9+.

## Quick start

Every entry point accepts the plan in any of these shapes:

| Input                                                                      | Notes                                |
|----------------------------------------------------------------------------|--------------------------------------|
| `str` / `bytes`                                                            | JSON text                            |
| `list[dict]`                                                               | Spark's native pre-order plan list   |
| `dict` (single node)                                                       | One plan node with `class` field     |
| `dict` (`{"plan": [...]}`, `{"logicalPlan": [...]}`, `{"nodes": [...]}`)   | wrapper                              |
| `LineageResult`                                                            | already-analysed; returned unchanged |

```python
import json
from spark_lineage import build_lineage, to_svg, to_html

plan = json.load(open("plan.json"))

result = build_lineage(plan)
print(result.plan_kind)          # QUERY | DDL | DML | NAMESPACE
print(result.operation)          # short class name of the root plan
print(result.target)             # TargetTable for DDL/DML, else None
print(result.source_tables)      # ['spark_catalog.default.employees', ...]

for col in result.columns:
    print(col.name, col.transformation_type, col.transformation)
    for src in col.sources:
        print("    <-", src.fqn)

# One-shot helpers — pass the same plan in any supported form:
to_svg(plan, path="lineage.svg")
to_html(plan, path="lineage.html", title="Employees lineage")
```

### Methods on `LineageResult`

```python
result = build_lineage(plan)

result.to_dict()            # plain dict
result.to_json(indent=2)    # str (JSON)
result.to_text()            # str (human-readable summary)
result.to_svg()             # str (SVG)
result.to_dot()             # str (Graphviz DOT)
result.to_html()            # str (HTML embedding the SVG)

# Generic save: format inferred from extension (.svg/.dot/.html/.json/.txt).
result.save("lineage.svg")
result.save("lineage.html")
result.save("forced.bin", format="dot")
```

### Standalone helpers

```python
from spark_lineage import to_json, to_text, to_svg, to_dot, to_html, save

to_json(plan)                       # dict
to_text(plan)                       # str
to_svg(plan, path="out.svg")
to_dot(plan, path="out.dot")
to_html(plan, path="out.html")
save(plan, "out/lineage.svg")       # extension-driven
```

### Low-level building blocks

```python
from spark_lineage import (
    plan_from_json, analyze,                              # parse + analyse separately
    render_svg, render_dot, render_html, render_text,     # operate on LineageResult
)

node    = plan_from_json(plan)        # PlanNode tree
result  = analyze(node)               # LineageResult
svg     = render_svg(result)
```

## Command-line interface

The package installs two equivalent console scripts: `catalyst-column-lineage`
and `spark-lineage`. You can also run it as a module: `python -m spark_lineage`.

```bash
# Single file → stdout
spark-lineage plan.json

# Whole directory → per-file *.lineage.json under lineage_out/
spark-lineage --dir test_json --out lineage_out

# Other formats
spark-lineage --dir test_json --out lineage_text --format text
spark-lineage --dir test_json --out lineage_svg  --format svg
spark-lineage --dir test_json --out lineage_html --format html
spark-lineage --dir test_json --out lineage_dot  --format dot
```

## SVG visualisation

`--format svg` (or `to_svg(...)`) produces a self-contained SVG
(no external CSS / fonts / scripts) that lays out source tables on the
left and output / CTE panels on the right with colour-coded Bezier
edges:

```text
┌───────────────────────────┐                         ┌────────────────────────────┐
│ spark_catalog.default.    │                         │ Output       QUERY :: Sort │
│  employees                │                         │ ────────────────────────── │
│  active                   ├──── IDENTITY ──────────►│ IDENTITY  emp_id :: int    │
│  dept_id                  ├╮                        │           ...              │
│  emp_id                   ├╪──── GROUPING ─────────►│ GROUPING  dept_id          │
│  emp_name                 ├╯                        │ AGGREGATE total_salary     │
│  hire_date                │  ╲ AGGREGATE ──────────►│           SUM(...)         │
│  salary                   ├───╯                     │           ...              │
└───────────────────────────┘                         └────────────────────────────┘
```

Edge / badge colour reflects `transformation_type`:

| Colour | Meaning                          |
|--------|----------------------------------|
| Gray   | IDENTITY (passthrough)           |
| Blue   | EXPRESSION (computed)            |
| Orange | AGGREGATE (`SUM`/`AVG`/...)      |
| Purple | WINDOW (`OVER (...)`)            |
| Green  | LITERAL                          |
| Red    | GROUPING (`GROUPING SETS` slot)  |
| Teal   | GENERATOR (`EXPLODE`/...)        |
| Slate  | OUTER_REFERENCE (correlated ref) |

Every output row carries an SVG `<title>` tooltip with the **untruncated**
expression and full source list, so opening the SVG in a browser gives
full on-hover detail even when the inline rendering is truncated.

## Output schema

`LineageResult.to_dict()` returns:

```jsonc
{
  "plan_kind": "QUERY",                 // QUERY | DDL | DML | NAMESPACE | UNKNOWN
  "operation": "Sort",                  // short class of the root plan node
  "target_table": null,                 // {catalog, database, table, fqn} for DDL/DML
  "source_tables": ["spark_catalog.default.employees"],
  "columns": [
    {
      "ordinal": 0,
      "name": "dept_id",
      "data_type": "int",
      "transformation_type": "IDENTITY", // IDENTITY | EXPRESSION | AGGREGATE | WINDOW | LITERAL | GROUPING | GENERATOR | OUTER_REFERENCE
      "transformation": "dept_id",
      "sources": [
        { "catalog": "spark_catalog", "database": "default",
          "table": "employees", "column": "dept_id",
          "fqn": "spark_catalog.default.employees.dept_id" }
      ]
    }
  ],
  "ctes": { /* same shape as columns, keyed by CTE name */ },
  "notes":  []
}
```

## Capabilities

The analyser handles every common Catalyst node:

- Leaves: `LogicalRelation`, `HiveTableRelation`, `UnresolvedRelation`,
  `OneRowRelation`, `LocalRelation`, `Range`.
- Schema-changing: `Project`, `Aggregate`, `Window`, `Generate`,
  `Expand` (`GROUPING SETS` / `ROLLUP` / `CUBE`), `Pivot`, `Unpivot`.
- Single-child passthroughs: `Filter`, `Sort`, `Distinct`,
  `Repartition*`, `Sample`, `Limit`/`GlobalLimit`/`LocalLimit`,
  `RebalancePartitions`, `Coalesce`, etc.
- Multi-input: `Join` (Inner / LeftOuter / RightOuter / FullOuter /
  LeftSemi / LeftAnti / Cross), `Union`, `Intersect`, `Except`.
- CTEs: `WithCTE`, `CTERelationDef`, `CTERelationRef`
  (with `ExprId` remapping).
- DDL / DML: `CreateTable*`, `ReplaceTable*`, `CreateView*`,
  `AlterView*`, `InsertIntoStatement`, `AppendData`, `OverwriteByExpression`,
  `MergeIntoTable`, `UpdateTable`, `DeleteFromTable`, `DropTable`,
  `RenameTable`, `TruncateTable`, namespace ops.
- Expressions: `Alias`, `AttributeReference`, `Literal`, `Cast`,
  `If`/`CaseWhen`, `WindowExpression` + `WindowSpecDefinition` +
  `SpecifiedWindowFrame`, `AggregateExpression`, `OuterReference`,
  generic fallback for arbitrary functions.

Unknown nodes are handled gracefully: lineage is propagated through them
as a passthrough where the schema is preserved, and a note is added so
nothing silently goes missing.

## Versioning & stability

Semantic versioning. The public API consists of every name re-exported
from the top-level `spark_lineage` module — see
[`spark_lineage.__all__`](https://pypi.org/project/catalyst-column-lineage/).

## License

MIT — see [LICENSE](LICENSE).
