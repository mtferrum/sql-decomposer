INSERT INTO nested_data
SELECT CAST(col1 AS INT) AS id, transform(col2, element -> element) AS tags, map_from_arrays(transform(map_keys(col3), key -> key), transform(map_values(col3), value -> value)) AS attrs, named_struct('country', col4.country AS country, 'city', col4.city AS city, 'zip', col4.zip AS zip) AS profile
FROM (
  SELECT NULL AS col1, NULL AS col2, NULL AS col3, NULL AS col4
);
