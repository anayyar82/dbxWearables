-- Databricks SQL — Bronze ingest volume (edit FQN if needed)
SELECT
  to_date(ingested_at) AS ingest_day,
  record_type,
  source_platform,
  COUNT(*) AS row_count
FROM users.ankur_nayyar.wearables_zerobus
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;
