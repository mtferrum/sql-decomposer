SELECT employees.dept_id, employees.active, count(*) AS cnt
FROM employees
GROUP BY CUBE(employees.dept_id, employees.active)
ORDER BY employees.dept_id ASC NULLS FIRST, employees.active ASC NULLS FIRST;
