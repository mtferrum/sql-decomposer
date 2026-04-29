SELECT sales.sale_id, sales.sale_ts, date_trunc('DAY', sales.sale_ts) AS sale_day, to_date(sales.sale_ts) AS sale_date, year(CAST(sales.sale_ts AS DATE)) AS y, month(CAST(sales.sale_ts AS DATE)) AS m, day(CAST(sales.sale_ts AS DATE)) AS d
FROM sales
ORDER BY sales.sale_id ASC NULLS FIRST;
