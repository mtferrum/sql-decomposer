WITH active_emp AS (
SELECT employees.emp_id, employees.emp_name, employees.dept_id, employees.salary, employees.hire_date, employees.active
FROM employees
WHERE (employees.active = TRUE)
),
dept_salary AS (
SELECT active_emp.dept_id, sum(active_emp.salary) AS total_salary, avg(active_emp.salary) AS avg_salary
FROM active_emp
GROUP BY active_emp.dept_id
)
SELECT a.emp_name, a.dept_id, d.total_salary, d.avg_salary
FROM active_emp a
  LEFT JOIN dept_salary d ON (a.dept_id = d.dept_id)
ORDER BY a.emp_id ASC NULLS FIRST;
