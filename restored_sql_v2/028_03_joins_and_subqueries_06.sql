SELECT avg(e2.salary) AS `avg(salary)`
FROM employees e2
WHERE (e2.dept_id = e.dept_id);
