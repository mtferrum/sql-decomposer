SELECT employees.emp_id, employees.emp_name, employees.salary
FROM employees
WHERE ((employees.active = TRUE) AND (employees.salary >= CAST(CAST(9000 AS DECIMAL(4,0)) AS DECIMAL(12,2))))
ORDER BY employees.salary DESC NULLS LAST, employees.emp_id ASC NULLS FIRST
LIMIT 5;
