SELECT e.*
FROM employees e
LEFT SEMI JOIN sales s ON e.emp_id = s.emp_id
ORDER BY e.emp_id;
