SELECT e.*
FROM employees e
LEFT ANTI JOIN sales s ON e.emp_id = s.emp_id
ORDER BY e.emp_id;
