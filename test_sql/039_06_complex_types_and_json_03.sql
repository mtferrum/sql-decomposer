SELECT id, tag
FROM nested_data
LATERAL VIEW explode(tags) t AS tag
ORDER BY id, tag;
