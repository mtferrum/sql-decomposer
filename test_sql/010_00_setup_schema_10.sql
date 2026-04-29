CREATE TABLE sales (
  sale_id BIGINT,
  emp_id INT,
  sale_ts TIMESTAMP,
  amount DECIMAL(12,2),
  channel STRING
) USING parquet;
