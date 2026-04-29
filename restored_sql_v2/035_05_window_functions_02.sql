SELECT s.emp_id, s.sale_ts, s.amount, row_number() OVER (PARTITION BY s.emp_id ORDER BY s.sale_ts ASC NULLS FIRST) AS rn, rank() OVER (PARTITION BY s.emp_id ORDER BY s.amount DESC NULLS LAST) AS amount_rank, sum(s.amount) OVER (PARTITION BY s.emp_id ORDER BY s.sale_ts ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
FROM sales s
ORDER BY s.emp_id ASC NULLS FIRST, s.sale_ts ASC NULLS FIRST;
