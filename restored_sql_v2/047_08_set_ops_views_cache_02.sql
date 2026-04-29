SELECT employees.emp_id
FROM employees
WHERE (employees.dept_id = 10)
UNION
SELECT sales.emp_id
FROM sales;
