CREATE OR REPLACE TEMP VIEW v_active_employees AS
SELECT employees.emp_id, employees.emp_name, employees.dept_id, employees.salary, employees.hire_date, employees.active
FROM employees
WHERE (employees.active = TRUE);
