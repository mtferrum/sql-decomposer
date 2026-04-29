SELECT
  dept_id,
  active,
  COUNT(*) AS cnt,
  SUM(salary) AS total_salary
FROM employees
GROUP BY GROUPING SETS ((dept_id, active), (dept_id), ())
ORDER BY dept_id, active;
