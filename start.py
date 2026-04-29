import sys
from pathlib import Path

from pyspark.sql import SparkSession

base_dir = Path(__file__).resolve().parent
warehouse_dir = base_dir / "spark-warehouse"
metastore_dir = base_dir / "spark-metastore"

spark = (
    SparkSession.builder
    .config("spark.jars", "spark-logical-plan-capture_2.12-0.1.0.jar")
    .config("spark.driver.extraClassPath", "spark-logical-plan-capture_2.12-0.1.0.jar")
    .config("spark.executor.extraClassPath", "spark-logical-plan-capture_2.12-0.1.0.jar")
    .config("spark.sql.extensions", "io.github.mt.logicplan.LogicalPlanCaptureExtension")
    .config("spark.sql.warehouse.dir", str(warehouse_dir))
    .config(
        "javax.jdo.option.ConnectionURL",
        f"jdbc:derby:;databaseName={metastore_dir};create=true",
    )
    .enableHiveSupport()
    .appName("My App")
    .getOrCreate()
)

if len(sys.argv) != 2:
    raise SystemExit("Usage: start.py <sql_file_path>")

sql_file_path = sys.argv[1]

with open(sql_file_path, "r", encoding="utf-8") as sql_file:
    query = sql_file.read().strip()

if not query:
    raise ValueError(f"SQL file is empty: {sql_file_path}")

spark.sql(query).show()
