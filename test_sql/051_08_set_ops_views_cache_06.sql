CREATE OR REPLACE TEMP VIEW v_sales_total AS
SELECT emp_id, SUM(amount) AS total_amount
FROM sales
GROUP BY emp_id;
