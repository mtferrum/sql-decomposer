SELECT
  emp_name,
  COALESCE(CAST(dept_id AS STRING), 'NO_DEPT') AS dept_label,
  CASE
    WHEN salary >= 13000 THEN 'A'
    WHEN salary >= 10000 THEN 'B'
    ELSE 'C'
  END AS salary_band
FROM employees
ORDER BY emp_id;
