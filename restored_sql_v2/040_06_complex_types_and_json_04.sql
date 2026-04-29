SELECT events.event_id, get_json_object(events.payload, '$.type') AS event_type, get_json_object(events.payload, '$.source') AS source, CAST(get_json_object(events.payload, '$.value') AS INT) AS value_int
FROM events
ORDER BY events.event_id ASC NULLS FIRST;
