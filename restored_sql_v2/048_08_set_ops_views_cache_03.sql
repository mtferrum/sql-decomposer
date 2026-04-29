SELECT employees.emp_id
FROM employees
INTERSECT
SELECT sales.emp_id
FROM sales;
