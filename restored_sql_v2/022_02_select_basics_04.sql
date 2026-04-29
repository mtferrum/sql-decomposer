SELECT DISTINCT sales.channel
FROM sales
WHERE (sales.channel IS NOT NULL)
ORDER BY sales.channel ASC NULLS FIRST;
