SELECT employees.emp_id, employees.hire_date, add_months(employees.hire_date, 6) AS plus_6_months, date_add(employees.hire_date, 10) AS plus_10_days, months_between(CAST(current_date() AS TIMESTAMP), CAST(employees.hire_date AS TIMESTAMP), TRUE) AS months_worked
FROM employees
ORDER BY employees.emp_id ASC NULLS FIRST;
