CREATE TABLE nested_data (
  id INT,
  tags ARRAY<STRING>,
  attrs MAP<STRING, STRING>,
  profile STRUCT<country: STRING, city: STRING, zip: STRING>
) USING parquet;
