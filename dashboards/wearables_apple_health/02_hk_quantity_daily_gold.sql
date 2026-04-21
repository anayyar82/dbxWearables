-- Gold: daily HK quantity rollups (DLT: 01_wearable_hk_quantity_daily_gold)
SELECT
  day,
  user_id,
  regexp_replace(hk_type, 'HKQuantityTypeIdentifier', '') AS metric_short,
  avg_value,
  min_value,
  max_value,
  sample_rows
FROM users.ankur_nayyar.01_wearable_hk_quantity_daily_gold
WHERE day >= current_date() - INTERVAL 30 DAYS
ORDER BY day DESC, sample_rows DESC;
