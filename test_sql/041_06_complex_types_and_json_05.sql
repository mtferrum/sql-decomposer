SELECT
  event_id,
  from_json(
    payload,
    'type STRING, source STRING, value INT'
  ) AS parsed_payload
FROM events
ORDER BY event_id;
