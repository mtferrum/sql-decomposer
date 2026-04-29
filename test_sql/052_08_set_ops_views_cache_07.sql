WITH joined AS (
  SELECT v.emp_id, v.emp_name, COALESCE(s.total_amount, 0.00) AS total_amount
  FROM v_active_employees v
  LEFT JOIN v_sales_total s ON v.emp_id = s.emp_id
)
SELECT *
FROM joined
ORDER BY total_amount DESC, emp_id;
