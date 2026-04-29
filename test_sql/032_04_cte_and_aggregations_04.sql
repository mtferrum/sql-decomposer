SELECT dept_id, active, COUNT(*) AS cnt
FROM employees
GROUP BY ROLLUP(dept_id, active)
ORDER BY dept_id, active;
