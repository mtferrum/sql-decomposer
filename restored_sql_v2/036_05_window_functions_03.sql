SELECT sales.emp_id, sales.sale_ts, sales.amount, lag(sales.amount, 1, CAST(0.00 AS DECIMAL(12,2))) OVER (PARTITION BY sales.emp_id ORDER BY sales.sale_ts ASC NULLS FIRST) AS prev_amount, lead(sales.amount, 1, CAST(0.00 AS DECIMAL(12,2))) OVER (PARTITION BY sales.emp_id ORDER BY sales.sale_ts ASC NULLS FIRST) AS next_amount
FROM sales
ORDER BY sales.emp_id ASC NULLS FIRST, sales.sale_ts ASC NULLS FIRST;
