INSERT INTO nested_data VALUES
  (1, array('spark', 'sql'), map('tier', 'gold', 'lang', 'ru'), named_struct('country', 'RU', 'city', 'Moscow', 'zip', '101000')),
  (2, array('etl', 'batch'), map('tier', 'silver', 'lang', 'en'), named_struct('country', 'US', 'city', 'Austin', 'zip', '73301')),
  (3, array('streaming'), map('tier', 'bronze'), named_struct('country', 'DE', 'city', 'Berlin', 'zip', '10115'));
