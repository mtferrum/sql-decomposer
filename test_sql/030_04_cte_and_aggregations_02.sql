WITH active_emp AS (
  SELECT *
  FROM employees
  WHERE active = true
),
dept_salary AS (
  SELECT dept_id, SUM(salary) AS total_salary, AVG(salary) AS avg_salary
  FROM active_emp
  GROUP BY dept_id
)
SELECT a.emp_name, a.dept_id, d.total_salary, d.avg_salary
FROM active_emp a
LEFT JOIN dept_salary d ON a.dept_id = d.dept_id
ORDER BY a.emp_id;
