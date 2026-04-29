INSERT INTO departments
SELECT CAST(col1 AS INT) AS dept_id, CAST(col2 AS STRING) AS dept_name, CAST(col3 AS STRING) AS region
FROM (
  SELECT NULL AS col1, NULL AS col2, NULL AS col3
);
