SELECT
  event_id,
  get_json_object(payload, '$.type') AS event_type,
  get_json_object(payload, '$.source') AS source,
  CAST(get_json_object(payload, '$.value') AS INT) AS value_int
FROM events
ORDER BY event_id;
