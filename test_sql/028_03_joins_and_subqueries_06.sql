SELECT e.emp_id, e.emp_name, e.salary
FROM employees e
WHERE e.salary > (
  SELECT AVG(e2.salary)
  FROM employees e2
  WHERE e2.dept_id = e.dept_id
)
ORDER BY e.emp_id;
