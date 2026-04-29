SELECT
  emp_id,
  sale_ts,
  amount,
  LAG(amount, 1, 0.00) OVER (PARTITION BY emp_id ORDER BY sale_ts) AS prev_amount,
  LEAD(amount, 1, 0.00) OVER (PARTITION BY emp_id ORDER BY sale_ts) AS next_amount
FROM sales
ORDER BY emp_id, sale_ts;
