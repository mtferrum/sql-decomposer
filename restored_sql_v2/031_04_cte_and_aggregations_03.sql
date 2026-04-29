SELECT employees.dept_id, employees.active, count(*) AS cnt, sum(employees.salary) AS total_salary
FROM employees
GROUP BY ROLLUP(employees.dept_id, employees.active)
ORDER BY employees.dept_id ASC NULLS FIRST, employees.active ASC NULLS FIRST;
