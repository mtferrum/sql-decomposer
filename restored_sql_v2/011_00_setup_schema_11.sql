CREATE TABLE events (
  event_id BIGINT,
  payload STRING,
  event_date DATE
) USING parquet;
