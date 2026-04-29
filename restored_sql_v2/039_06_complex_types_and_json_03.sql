SELECT nested_data.id, t.tag
FROM nested_data
  LATERAL VIEW explode(nested_data.tags) t AS tag
ORDER BY nested_data.id ASC NULLS FIRST, t.tag ASC NULLS FIRST;
