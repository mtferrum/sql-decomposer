INSERT INTO events
SELECT CAST(col1 AS BIGINT) AS event_id, CAST(col2 AS STRING) AS payload, CAST(col3 AS DATE) AS event_date
FROM (
  SELECT NULL AS col1, NULL AS col2, NULL AS col3
);
