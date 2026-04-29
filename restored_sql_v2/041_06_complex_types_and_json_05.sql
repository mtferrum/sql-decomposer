SELECT events.event_id, from_json(events.payload, 'STRUCT<type: STRING, source: STRING, value: INT>') AS parsed_payload
FROM events
ORDER BY events.event_id ASC NULLS FIRST;
