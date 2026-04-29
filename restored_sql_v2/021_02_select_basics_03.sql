SELECT employees.emp_name, coalesce(CAST(employees.dept_id AS STRING), 'NO_DEPT') AS dept_label, (CASE WHEN (employees.salary >= CAST(CAST(13000 AS DECIMAL(5,0)) AS DECIMAL(12,2))) THEN 'A' WHEN (employees.salary >= CAST(CAST(10000 AS DECIMAL(5,0)) AS DECIMAL(12,2))) THEN 'B' ELSE 'C' END) AS salary_band
FROM employees
ORDER BY employees.emp_id ASC NULLS FIRST;
