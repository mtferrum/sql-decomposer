SELECT
  id,
  tags[0] AS first_tag,
  element_at(attrs, 'tier') AS tier,
  profile.country AS country,
  profile.city AS city
FROM nested_data
ORDER BY id;
