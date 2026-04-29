SELECT
  emp_id,
  hire_date,
  add_months(hire_date, 6) AS plus_6_months,
  date_add(hire_date, 10) AS plus_10_days,
  months_between(current_date(), hire_date) AS months_worked
FROM employees
ORDER BY emp_id;
