-- Freshness: age of latest bronze row (good SLA / monitoring tile)
SELECT
  MAX(ingested_at) AS last_ingested_at,
  datediff(current_timestamp(), MAX(ingested_at)) AS days_since_last_row,
  (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(ingested_at))) / 3600.0 AS hours_since_last_row
FROM users.ankur_nayyar.wearables_zerobus;
