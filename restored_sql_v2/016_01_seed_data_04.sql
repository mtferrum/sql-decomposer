INSERT INTO sales
SELECT CAST(col1 AS BIGINT) AS sale_id, CAST(col2 AS INT) AS emp_id, CAST(col3 AS TIMESTAMP) AS sale_ts, CAST(col4 AS DECIMAL(12,2)) AS amount, CAST(col5 AS STRING) AS channel
FROM (
  SELECT NULL AS col1, NULL AS col2, NULL AS col3, NULL AS col4, NULL AS col5
);
