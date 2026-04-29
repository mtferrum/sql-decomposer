SELECT
  sale_id,
  sale_ts,
  date_trunc('DAY', sale_ts) AS sale_day,
  to_date(sale_ts) AS sale_date,
  year(sale_ts) AS y,
  month(sale_ts) AS m,
  day(sale_ts) AS d
FROM sales
ORDER BY sale_id;
