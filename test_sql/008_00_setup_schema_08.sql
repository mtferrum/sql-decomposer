CREATE TABLE employees (
  emp_id INT,
  emp_name STRING,
  dept_id INT,
  salary DECIMAL(12,2),
  hire_date DATE,
  active BOOLEAN
) USING parquet;
