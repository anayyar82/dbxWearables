-- Rows produced by the seed notebook (x-device-id = demo-notebook-seed)
SELECT
  to_date(ingested_at) AS day,
  record_type,
  COUNT(*) AS rows
FROM users.ankur_nayyar.wearables_zerobus
WHERE get_json_object(to_json(headers), '$.x-device-id') = 'demo-notebook-seed'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
