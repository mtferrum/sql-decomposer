SELECT
  s.emp_id,
  s.sale_ts,
  s.amount,
  ROW_NUMBER() OVER (PARTITION BY s.emp_id ORDER BY s.sale_ts) AS rn,
  RANK() OVER (PARTITION BY s.emp_id ORDER BY s.amount DESC) AS amount_rank,
  SUM(s.amount) OVER (
    PARTITION BY s.emp_id
    ORDER BY s.sale_ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_amount
FROM sales s
ORDER BY s.emp_id, s.sale_ts;
