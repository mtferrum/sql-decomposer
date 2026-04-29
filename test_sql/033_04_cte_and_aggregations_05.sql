SELECT dept_id, active, COUNT(*) AS cnt
FROM employees
GROUP BY CUBE(dept_id, active)
ORDER BY dept_id, active;
