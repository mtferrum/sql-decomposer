# spark-plan-to-sql

Convert Apache Spark Catalyst `LogicalPlan` JSON dumps back into a logically
equivalent SQL statement.

The converter walks the pre-order JSON serialization Spark produces for its
`LogicalPlan` (the same shape exposed via `df.queryExecution.logical.toJSON`)
and emits readable SQL that, when re-executed, returns the same rows as the
original query.

## Install

```bash
pip install spark-plan-to-sql
```

## Python API

```python
import json
from spark_plan_to_sql import plan_to_sql, dict_to_sql

# 1) Plain JSON string
sql = plan_to_sql('[{"class": "...OneRowRelation", "num-children": 0}]')

# 2) Already-parsed Python list (Spark's native format)
with open("plan.json") as f:
    sql = plan_to_sql(json.load(f))

# 3) A dict wrapper, e.g. {"plan": [...]} or {"logicalPlan": [...]}
sql = plan_to_sql({"plan": json.load(open("plan.json"))})

# 4) Strict dict-only helper
sql = dict_to_sql({"logicalPlan": [...]})
```

The function accepts:

- `str` / `bytes` — JSON text
- `list[dict]` — Spark's native pre-order plan list
- `dict` — either a single leaf node or a wrapper such as
  `{"plan": [...]}` / `{"logicalPlan": [...]}` / `{"nodes": [...]}`

## CLI

```bash
# Convert one or more files (prints to stdout)
spark-plan-to-sql plan.json plan2.json

# Batch convert a directory of plans into ./restored_sql/
spark-plan-to-sql --dir test_json --out restored_sql
```

## Supported plan nodes

DDL/DML: `CreateNamespace`, `DropNamespace`, `SetCatalogAndNamespace`,
`CreateTable*`, `DropTable*`, `DropView`, `CreateViewCommand`,
`CacheTable`, `UncacheTable`, `InsertInto*`, `AppendData`.

Query: `Project`, `Filter`, `Sort`, `GlobalLimit/LocalLimit`,
`Distinct/Deduplicate`, `Aggregate` (incl. `ROLLUP`/`CUBE`/`GROUPING SETS`
through the `Expand+spark_grouping_id` pattern), `Join` (Inner / Left / Right
/ Full / LeftSemi / LeftAnti / Cross), `Union/Intersect/Except`, `Window`
(with frame suppression for `LAG`/`LEAD`/`RANK`/`ROW_NUMBER`/...),
`Generate` (`LATERAL VIEW`), `WithCTE`/`CTERelationDef`/`CTERelationRef`,
`SubqueryAlias`, `LogicalRelation`, `DataSourceV2Relation`, `LocalRelation`,
`OneRowRelation`.

Expressions: literals (incl. interval/decimal/date/timestamp), `Cast`,
`Alias`, `AttributeReference`, `OuterReference`, binary/unary operators,
`CaseWhen`/`If`, `Coalesce`/`IfNull`/`Nvl`, aggregates (`Count`/`Sum`/
`Avg`/...), datetime (`Year`/`Month`/`AddMonths`/`DateAdd` rewriting from
`ExtractANSIIntervalDays`/...), windowing (`WindowExpression`/
`WindowSpecDefinition`/`SpecifiedWindowFrame`), array/map/struct
(`GetStructField`/`GetArrayItem`/`ElementAt`/`ArrayTransform`/
`LambdaFunction`/`CreateNamedStruct`/`MapFromArrays`/...), JSON
(`JsonToStructs`/`GetJsonObject`/`StructsToJson`).

## License

MIT
