import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _split_bracket_list(value: Any) -> List[str]:
    if value is None:
        return []
    text = str(value).strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _sql_literal(value: Any, data_type: Optional[str] = None) -> str:
    if value is None:
        return "NULL"

    text = str(value)
    dtype = (data_type or "").lower()

    # Spark Catalyst emits intervals in numeric internal form. Keep as INTERVAL text to avoid
    # invalid raw literals in restored SQL.
    if dtype.startswith("interval"):
        return "INTERVAL '" + text + "'"
    if dtype == "string":
        return "'" + text.replace("'", "''") + "'"
    if dtype == "date":
        return "DATE '" + text + "'"
    if dtype == "timestamp":
        return "TIMESTAMP '" + text + "'"
    if dtype == "boolean":
        return text.lower()
    if text.upper() == "NULL":
        return "NULL"
    return text


def _compact_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


class SparkLogicalPlanToSql:
    """
    Reconstruct SQL from Spark's serialized logical plan JSON.

    The implementation follows the public Catalyst node structure from Spark sources:
    - Project(projectList, child)
    - Filter(condition, child)
    - Aggregate(groupingExpressions, aggregateExpressions, child)
    - Join(left, right, joinType, condition, hint)
    and related logical operators in `org.apache.spark.sql.catalyst.plans.logical`.
    """

    def __init__(self, nodes: List[Dict[str, Any]]) -> None:
        self.nodes = nodes
        self._cte_names: Dict[int, str] = {}

    def restore(self) -> str:
        sql, _ = self._consume_plan(0)
        sql = self._cleanup_sql(sql)
        if not sql.endswith(";"):
            sql += ";"
        return sql + "\n"

    def _cleanup_sql(self, sql: str) -> str:
        out = _compact_sql(sql)
        out = re.sub(r"\s+ASC(\s*,|\s*$)", r"\1", out, flags=re.IGNORECASE)
        out = re.sub(r"\(\s*SELECT \* FROM \((.*?)\) q\s*\)", r"(\1)", out, flags=re.IGNORECASE)
        return out.strip()

    def _consume_plan(self, idx: int) -> Tuple[str, int]:
        node = self.nodes[idx]
        class_name = str(node.get("class", "")).split(".")[-1]
        child_count = int(node.get("num-children", 0))
        children: List[str] = []
        next_idx = idx + 1
        for _ in range(child_count):
            child_sql, next_idx = self._consume_plan(next_idx)
            children.append(child_sql)

        handler = getattr(self, "_plan_" + class_name, None)
        if handler is None:
            # Spark command nodes often carry source SQL in originalText.
            original_text = node.get("originalText")
            if isinstance(original_text, str) and original_text.strip():
                return original_text.strip(), next_idx
            return "/* unsupported plan node: " + class_name + " */ SELECT 1", next_idx
        return handler(node, children), next_idx

    def _table_name(self, table: Optional[Dict[str, Any]]) -> str:
        if not table:
            return "unknown_table"
        ident = table.get("identifier", {})
        if isinstance(ident, dict):
            table_name = ident.get("table") or ident.get("name")
            if table_name:
                return str(table_name)
        direct = table.get("table") or table.get("name")
        if direct:
            return str(direct)
        return "unknown_table"

    def _consume_expr(self, chain: List[Any], idx: int = 0) -> Tuple[str, int]:
        item = chain[idx]
        class_name = str(item.get("class", "")).split(".")[-1]
        child_count = int(item.get("num-children", 0))
        children: List[str] = []
        next_idx = idx + 1
        for _ in range(child_count):
            child_sql, next_idx = self._consume_expr(chain, next_idx)
            children.append(child_sql)

        if class_name == "Alias":
            inner = children[0] if children else "NULL"
            alias = str(item.get("name", "col"))
            return inner + " AS " + alias, next_idx
        if class_name == "AttributeReference":
            return str(item.get("name", "col")), next_idx
        if class_name == "OuterReference":
            return children[0] if children else "NULL", next_idx
        if class_name == "Literal":
            return _sql_literal(item.get("value"), item.get("dataType")), next_idx
        if class_name == "Cast":
            inner = children[0] if children else "NULL"
            dt = str(item.get("dataType", "STRING")).upper().replace("INTEGER", "INT")
            return "CAST(" + inner + " AS " + dt + ")", next_idx
        if class_name == "Coalesce":
            return "COALESCE(" + ", ".join(children) + ")", next_idx

        if class_name == "CaseWhen":
            parts = ["CASE"]
            for branch in item.get("branches", []):
                cond = self._expr(branch["_1"])
                result = self._expr(branch["_2"])
                parts.append("WHEN " + cond + " THEN " + result)
            else_value = item.get("elseValue")
            if else_value:
                parts.append("ELSE " + self._expr(else_value))
            parts.append("END")
            return " ".join(parts), next_idx

        if class_name in ("EqualTo", "GreaterThanOrEqual", "And", "Divide"):
            left = children[0] if len(children) > 0 else "NULL"
            right = children[1] if len(children) > 1 else "NULL"
            op = {
                "EqualTo": "=",
                "GreaterThanOrEqual": ">=",
                "And": "AND",
                "Divide": "/",
            }[class_name]
            return "(" + left + " " + op + " " + right + ")", next_idx

        if class_name in ("Count", "Sum", "Average", "Year", "Month", "DayOfMonth"):
            child = children[0] if children else "*"
            fn = {"Average": "AVG", "DayOfMonth": "DAY"}.get(class_name, class_name.upper())
            return fn + "(" + child + ")", next_idx

        if class_name == "AggregateExpression":
            return children[0] if children else "NULL", next_idx

        if class_name == "GetStructField":
            target = children[0] if children else "NULL"
            name = item.get("name")
            if name:
                return target + "." + str(name), next_idx
            return target + "[" + str(item.get("ordinal", 0)) + "]", next_idx

        if class_name == "GetArrayItem":
            left = children[0] if len(children) > 0 else "NULL"
            right = children[1] if len(children) > 1 else "0"
            return left + "[" + right + "]", next_idx

        if class_name == "ElementAt":
            left = children[0] if len(children) > 0 else "NULL"
            right = children[1] if len(children) > 1 else "NULL"
            return "ELEMENT_AT(" + left + ", " + right + ")", next_idx

        if class_name in ("GetJsonObject", "DateAdd", "AddMonths", "MonthsBetween", "TimeAdd", "DatetimeSub"):
            return class_name.upper() + "(" + ", ".join(children) + ")", next_idx
        if class_name == "ParseToDate":
            return "TO_DATE(" + ", ".join(children[:2]) + ")", next_idx
        if class_name == "TruncTimestamp":
            fmt = children[0] if len(children) > 0 else "'DAY'"
            ts = children[1] if len(children) > 1 else "NULL"
            return "DATE_TRUNC(" + fmt + ", " + ts + ")", next_idx
        if class_name == "CurrentDate":
            return "CURRENT_DATE", next_idx
        if class_name == "UnaryMinus":
            child = children[0] if children else "0"
            return "(-" + child + ")", next_idx
        if class_name == "ExtractANSIIntervalDays":
            child = children[0] if children else "NULL"
            return "EXTRACT(DAY FROM " + child + ")", next_idx
        if class_name in ("Lag", "Lead"):
            return class_name.upper() + "(" + ", ".join(children) + ")", next_idx
        if class_name == "Rank":
            return "RANK()", next_idx
        if class_name == "RowNumber":
            return "ROW_NUMBER()", next_idx

        if class_name == "SortOrder":
            child = children[0] if children else "NULL"
            direction_obj = str(item.get("direction", {}).get("object", "Ascending$"))
            return child + (" DESC" if "Descending" in direction_obj else " ASC"), next_idx

        if class_name == "WindowExpression":
            fn = children[0] if len(children) > 0 else "NULL"
            spec = children[1] if len(children) > 1 else ""
            return fn + " OVER (" + spec + ")", next_idx

        if class_name == "WindowSpecDefinition":
            pieces: List[str] = []
            part_indexes = item.get("partitionSpec", [])
            if part_indexes:
                part_cols = []
                for i in part_indexes:
                    idx_num = int(i)
                    if idx_num < len(children):
                        part_cols.append(children[idx_num])
                if part_cols:
                    pieces.append("PARTITION BY " + ", ".join(part_cols))
            order_indexes = item.get("orderSpec", [])
            if order_indexes:
                order_cols = []
                for i in order_indexes:
                    idx_num = int(i)
                    if idx_num < len(children):
                        order_cols.append(children[idx_num])
                if order_cols:
                    pieces.append("ORDER BY " + ", ".join(order_cols))
            return " ".join(pieces), next_idx

        if class_name == "SpecifiedWindowFrame":
            return "", next_idx

        if class_name == "ArrayTransform":
            argument = children[0] if len(children) > 0 else "NULL"
            function = children[1] if len(children) > 1 else "x -> x"
            return "TRANSFORM(" + argument + ", " + function + ")", next_idx
        if class_name == "LambdaFunction":
            function = children[0] if children else "x"
            arg_count = len(item.get("arguments", []))
            args = ", ".join(children[1:1 + arg_count]) if arg_count else "x"
            return "(" + args + ") -> " + function, next_idx
        if class_name == "NamedLambdaVariable":
            return str(item.get("name", "x")), next_idx
        if class_name == "CreateNamedStruct":
            val_exprs = item.get("valExprs", [])
            pairs = []
            for i in range(0, len(val_exprs), 2):
                key_idx = int(val_exprs[i])
                value_idx = int(val_exprs[i + 1]) if i + 1 < len(val_exprs) else -1
                key = children[key_idx] if key_idx < len(children) else "'k'"
                value = children[value_idx] if value_idx >= 0 and value_idx < len(children) else "NULL"
                pairs.append(key + ", " + value)
            return "NAMED_STRUCT(" + ", ".join(pairs) + ")", next_idx
        if class_name in ("MapFromArrays", "MapKeys", "MapValues", "JsonToStructs", "Explode"):
            return class_name.upper() + "(" + ", ".join(children) + ")", next_idx
        if class_name == "IsNotNull":
            child = children[0] if children else "NULL"
            return "(" + child + " IS NOT NULL)", next_idx

        return "/* unsupported expr: " + class_name + " */", next_idx

    def _expr(self, expr_chain: List[Any]) -> str:
        sql, _ = self._consume_expr(expr_chain, 0)
        return sql

    def _expr_list(self, expr_chains: List[List[Any]]) -> List[str]:
        return [self._expr(chunk) for chunk in expr_chains]

    # --- Plan handlers ---
    def _plan_ResolvedNamespace(self, node: Dict[str, Any], _children: List[str]) -> str:
        names = _split_bracket_list(node.get("namespace"))
        return ".".join(names) if names else "default"

    def _plan_SetCatalogAndNamespace(self, _node: Dict[str, Any], children: List[str]) -> str:
        namespace = children[0] if children else "default"
        return "USE " + namespace

    def _plan_CreateNamespace(self, node: Dict[str, Any], children: List[str]) -> str:
        db = children[0] if children else "default"
        clause = "IF NOT EXISTS " if node.get("ifNotExists") else ""
        return "CREATE DATABASE " + clause + db

    def _plan_CreateDataSourceTableCommand(self, node: Dict[str, Any], _children: List[str]) -> str:
        table = node.get("table", {})
        name = self._table_name(table)
        schema = table.get("schema", {}).get("fields", [])
        col_defs = []
        for field in schema:
            col_name = field["name"]
            col_type = self._data_type_to_sql(field["type"])
            col_defs.append("  " + col_name + " " + col_type)
        provider = table.get("provider", "parquet")
        return "CREATE TABLE " + name + " (\n" + ",\n".join(col_defs) + "\n) USING " + provider

    def _data_type_to_sql(self, data_type: Any) -> str:
        if isinstance(data_type, str):
            return data_type.upper().replace("INTEGER", "INT")
        if isinstance(data_type, dict):
            kind = data_type.get("type")
            if kind == "array":
                return "ARRAY<" + self._data_type_to_sql(data_type["elementType"]) + ">"
            if kind == "map":
                return (
                    "MAP<"
                    + self._data_type_to_sql(data_type["keyType"])
                    + ", "
                    + self._data_type_to_sql(data_type["valueType"])
                    + ">"
                )
            if kind == "struct":
                fields = data_type.get("fields", [])
                parts = [f["name"] + ":" + self._data_type_to_sql(f["type"]) for f in fields]
                return "STRUCT<" + ", ".join(parts) + ">"
        return "STRING"

    def _plan_DropTable(self, node: Dict[str, Any], children: List[str]) -> str:
        target = children[0] if children else "unknown_table"
        if_exists = " IF EXISTS" if node.get("ifExists") else ""
        return "DROP TABLE" + if_exists + " " + target

    def _plan_ResolvedIdentifier(self, node: Dict[str, Any], _children: List[str]) -> str:
        ident = node.get("identifier") or {}
        if not isinstance(ident, dict):
            return "unknown"
        namespace = _split_bracket_list(ident.get("namespace"))
        parts = namespace + [ident.get("name", "unknown")]
        return ".".join([p for p in parts if p])

    def _plan_DropTableCommand(self, node: Dict[str, Any], _children: List[str]) -> str:
        table = self._table_name(node.get("tableName"))
        if_exists = " IF EXISTS" if node.get("ifExists") else ""
        return "DROP TABLE" + if_exists + " " + table

    def _plan_SubqueryAlias(self, node: Dict[str, Any], children: List[str]) -> str:
        child = children[0] if children else "SELECT 1"
        identifier = node.get("identifier", {})
        alias = identifier.get("name") if isinstance(identifier, dict) else None
        if not alias:
            return child
        return "SELECT * FROM (" + child + ") " + str(alias)

    def _plan_LogicalRelation(self, node: Dict[str, Any], _children: List[str]) -> str:
        return "SELECT * FROM " + self._table_name(node.get("catalogTable"))

    def _plan_Filter(self, node: Dict[str, Any], children: List[str]) -> str:
        cond = self._expr(node.get("condition", []))
        child = children[0] if children else "SELECT 1"
        return "SELECT * FROM (" + child + ") q WHERE " + cond

    def _plan_Project(self, node: Dict[str, Any], children: List[str]) -> str:
        cols = self._expr_list(node.get("projectList", []))
        child = children[0] if children else "SELECT 1"
        return "SELECT " + ", ".join(cols) + " FROM (" + child + ") q"

    def _plan_Sort(self, node: Dict[str, Any], children: List[str]) -> str:
        order = self._expr_list(node.get("order", []))
        child = children[0] if children else "SELECT 1"
        return "SELECT * FROM (" + child + ") q ORDER BY " + ", ".join(order)

    def _plan_Distinct(self, _node: Dict[str, Any], children: List[str]) -> str:
        child = children[0] if children else "SELECT 1"
        return "SELECT DISTINCT * FROM (" + child + ") q"

    def _plan_Aggregate(self, node: Dict[str, Any], children: List[str]) -> str:
        groups = self._expr_list(node.get("groupingExpressions", []))
        exprs = self._expr_list(node.get("aggregateExpressions", []))
        child = children[0] if children else "SELECT 1"
        group_clause = " GROUP BY " + ", ".join(groups) if groups else ""
        return "SELECT " + ", ".join(exprs) + " FROM (" + child + ") q" + group_clause

    def _plan_Join(self, node: Dict[str, Any], children: List[str]) -> str:
        left = children[0] if len(children) > 0 else "SELECT 1"
        right = children[1] if len(children) > 1 else "SELECT 1"
        join_type = str(node.get("joinType", {}).get("product-class", "Inner")).split(".")[-1].replace("$", "")
        cond = node.get("condition")

        join_sql = "JOIN"
        if "LeftOuter" in join_type:
            join_sql = "LEFT JOIN"
        elif "RightOuter" in join_type:
            join_sql = "RIGHT JOIN"
        elif "FullOuter" in join_type:
            join_sql = "FULL JOIN"
        elif "LeftSemi" in join_type:
            join_sql = "LEFT SEMI JOIN"
        elif "LeftAnti" in join_type:
            join_sql = "LEFT ANTI JOIN"
        elif "Cross" in join_type:
            join_sql = "CROSS JOIN"

        on_clause = " ON " + self._expr(cond) if cond else ""
        if join_sql == "CROSS JOIN":
            on_clause = ""
        return "SELECT * FROM (" + left + ") l " + join_sql + " (" + right + ") r" + on_clause

    def _plan_GlobalLimit(self, node: Dict[str, Any], children: List[str]) -> str:
        child = children[0] if children else "SELECT 1"
        limit = self._expr(node.get("limitExpr", []))
        return "SELECT * FROM (" + child + ") q LIMIT " + limit

    def _plan_LocalLimit(self, node: Dict[str, Any], children: List[str]) -> str:
        return self._plan_GlobalLimit(node, children)

    def _plan_Union(self, _node: Dict[str, Any], children: List[str]) -> str:
        return " UNION ".join("(" + c + ")" for c in children)

    def _plan_Intersect(self, node: Dict[str, Any], children: List[str]) -> str:
        left = children[0] if len(children) > 0 else "SELECT 1"
        right = children[1] if len(children) > 1 else "SELECT 1"
        all_kw = " ALL" if node.get("isAll") else ""
        return "(" + left + ") INTERSECT" + all_kw + " (" + right + ")"

    def _plan_Except(self, node: Dict[str, Any], children: List[str]) -> str:
        left = children[0] if len(children) > 0 else "SELECT 1"
        right = children[1] if len(children) > 1 else "SELECT 1"
        all_kw = " ALL" if node.get("isAll") else ""
        return "(" + left + ") EXCEPT" + all_kw + " (" + right + ")"

    def _plan_Window(self, node: Dict[str, Any], children: List[str]) -> str:
        child = children[0] if children else "SELECT 1"
        exprs = self._expr_list(node.get("windowExpressions", []))
        suffix = ", " + ", ".join(exprs) if exprs else ""
        return "SELECT *" + suffix + " FROM (" + child + ") q"

    def _plan_Generate(self, node: Dict[str, Any], children: List[str]) -> str:
        child = children[0] if children else "SELECT 1"
        generator = self._expr(node.get("generator", []))
        return "SELECT *, " + generator + " FROM (" + child + ") q"

    def _plan_WithCTE(self, node: Dict[str, Any], children: List[str]) -> str:
        ctes: List[str] = []
        for i, cte_id in enumerate(node.get("cteDefs", [])):
            cte_id_num = int(cte_id)
            cte_name = "cte_" + str(cte_id_num)
            self._cte_names[cte_id_num] = cte_name
            cte_sql = children[i] if i < len(children) else "SELECT 1"
            ctes.append(cte_name + " AS (" + cte_sql + ")")
        child = children[-1] if children else "SELECT 1"
        return "WITH " + ", ".join(ctes) + " " + child

    def _plan_CTERelationDef(self, node: Dict[str, Any], children: List[str]) -> str:
        cte_id = int(node.get("id", -1))
        if cte_id >= 0:
            self._cte_names.setdefault(cte_id, "cte_" + str(cte_id))
        return children[0] if children else "SELECT 1"

    def _plan_CTERelationRef(self, node: Dict[str, Any], _children: List[str]) -> str:
        cte_id = int(node.get("cteId", -1))
        name = self._cte_names.get(cte_id, "cte_" + str(cte_id))
        return "SELECT * FROM " + name

    def _plan_InsertIntoHadoopFsRelationCommand(self, node: Dict[str, Any], children: List[str]) -> str:
        table = self._table_name(node.get("catalogTable"))
        query_sql = children[0] if children else "SELECT 1"
        return "INSERT INTO " + table + " " + query_sql

    def _plan_LocalRelation(self, _node: Dict[str, Any], _children: List[str]) -> str:
        return "SELECT /* LocalRelation rows missing in capture */ NULL"

    def _plan_OneRowRelation(self, _node: Dict[str, Any], _children: List[str]) -> str:
        return "SELECT 1"

    def _plan_Expand(self, _node: Dict[str, Any], children: List[str]) -> str:
        # Expand is an internal rewrite for GROUPING SETS / ROLLUP / CUBE.
        return children[0] if children else "SELECT 1"

    def _plan_CreateViewCommand(self, node: Dict[str, Any], children: List[str]) -> str:
        name = self._table_name(node.get("name"))
        original = node.get("originalText")
        if isinstance(original, str) and original.strip():
            query = original.strip()
        else:
            query = children[0] if children else "SELECT 1"
        return "CREATE VIEW " + name + " AS " + query


def restore_sql_from_json_payload(payload: List[Dict[str, Any]]) -> str:
    return SparkLogicalPlanToSql(payload).restore()


def restore_sql_from_json_string(json_string: str) -> str:
    """
    Restore SQL from a JSON string produced from Spark LogicalPlan capture.

    Example:
        sql = restore_sql_from_json_string(json.dumps(plan_nodes))
    """
    payload = json.loads(json_string)
    if not isinstance(payload, list):
        raise ValueError("LogicalPlan JSON must be a JSON array of nodes")
    return restore_sql_from_json_payload(payload)


def restore_sql(payload_or_json: Any) -> str:
    """
    Convenience API for Python usage.

    Accepts:
      - JSON string
      - already parsed payload (list[dict])
    Returns:
      - restored SQL string
    """
    if isinstance(payload_or_json, str):
        return restore_sql_from_json_string(payload_or_json)
    if isinstance(payload_or_json, list):
        return restore_sql_from_json_payload(payload_or_json)
    raise TypeError("Expected JSON string or list payload")


def restore_sql_from_json_file(input_path: Path) -> str:
    with input_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return restore_sql_from_json_payload(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore SQL from Spark Catalyst LogicalPlan JSON")
    parser.add_argument("input", type=Path, help="Input JSON file or directory")
    parser.add_argument("--output", type=Path, help="Output SQL file or directory")
    args = parser.parse_args()

    if args.input.is_file():
        sql = restore_sql_from_json_file(args.input)
        if args.output:
            args.output.write_text(sql, encoding="utf-8")
        else:
            print(sql, end="")
        return

    if not args.input.is_dir():
        raise SystemExit("Input does not exist: " + str(args.input))

    out_dir = args.output or Path("restored_sql")
    out_dir.mkdir(parents=True, exist_ok=True)
    for json_file in sorted(args.input.glob("*.json")):
        sql = restore_sql_from_json_file(json_file)
        target = out_dir / (json_file.stem + ".sql")
        target.write_text(sql, encoding="utf-8")
        print("restored: " + json_file.name + " -> " + str(target))


if __name__ == "__main__":
    main()
