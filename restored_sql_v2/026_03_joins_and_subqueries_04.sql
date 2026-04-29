SELECT e.emp_id, e.emp_name, e.dept_id, e.salary, e.hire_date, e.active
FROM employees e
  LEFT SEMI JOIN sales s ON (e.emp_id = s.emp_id)
ORDER BY e.emp_id ASC NULLS FIRST;
