SELECT employees.emp_id
FROM employees
EXCEPT
SELECT sales.emp_id
FROM sales;
