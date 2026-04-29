SELECT DISTINCT channel
FROM sales
WHERE channel IS NOT NULL
ORDER BY channel;
