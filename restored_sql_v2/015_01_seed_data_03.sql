INSERT INTO employees
SELECT CAST(col1 AS INT) AS emp_id, CAST(col2 AS STRING) AS emp_name, CAST(col3 AS INT) AS dept_id, CAST(col4 AS DECIMAL(12,2)) AS salary, CAST(col5 AS DATE) AS hire_date, CAST(col6 AS BOOLEAN) AS active
FROM (
  SELECT NULL AS col1, NULL AS col2, NULL AS col3, NULL AS col4, NULL AS col5, NULL AS col6
);
