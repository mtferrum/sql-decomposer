SELECT nested_data.id, nested_data.tags[0] AS first_tag, element_at(nested_data.attrs, 'tier') AS tier, nested_data.profile.country AS country, nested_data.profile.city AS city
FROM nested_data
ORDER BY nested_data.id ASC NULLS FIRST;
