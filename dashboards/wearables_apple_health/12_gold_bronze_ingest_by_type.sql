-- Bronze ingest volume by day / user / record_type (long-form gold)
SELECT ingest_day, user_id, record_type, row_count
FROM users.ankur_nayyar.01_wearable_gold_bronze_ingest_daily
WHERE ingest_day >= current_date() - INTERVAL 90 DAY
ORDER BY ingest_day DESC, row_count DESC;
