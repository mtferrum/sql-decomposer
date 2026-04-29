SELECT emp_id, emp_name, salary
FROM employees
WHERE active = true AND salary >= 9000
ORDER BY salary DESC, emp_id
LIMIT 5;
