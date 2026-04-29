CREATE OR REPLACE TEMP VIEW v_sales_total AS
SELECT sales.emp_id, sum(sales.amount) AS total_amount
FROM sales
GROUP BY sales.emp_id;
