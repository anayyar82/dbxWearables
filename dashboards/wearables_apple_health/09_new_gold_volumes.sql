-- Row counts for newer gold tables (edit catalog.schema if needed)
SELECT '01_wearable_gold_daily_steps' AS dataset, COUNT(*) AS row_count
FROM users.ankur_nayyar.01_wearable_gold_daily_steps
UNION ALL
SELECT '01_wearable_gold_activity_enriched_daily', COUNT(*)
FROM users.ankur_nayyar.01_wearable_gold_activity_enriched_daily
UNION ALL
SELECT '01_wearable_gold_sleep_nightly', COUNT(*) FROM users.ankur_nayyar.01_wearable_gold_sleep_nightly
UNION ALL
SELECT '01_wearable_gold_weekly_workout_summary', COUNT(*)
FROM users.ankur_nayyar.01_wearable_gold_weekly_workout_summary
UNION ALL
SELECT '01_wearable_gold_cardio_vitals_daily', COUNT(*)
FROM users.ankur_nayyar.01_wearable_gold_cardio_vitals_daily
UNION ALL
SELECT '01_wearable_gold_heart_rate_intraday_daily', COUNT(*)
FROM users.ankur_nayyar.01_wearable_gold_heart_rate_intraday_daily
UNION ALL
SELECT '01_wearable_gold_bronze_ingest_daily', COUNT(*)
FROM users.ankur_nayyar.01_wearable_gold_bronze_ingest_daily
UNION ALL
SELECT '01_wearable_gold_hk_family_weekly', COUNT(*) FROM users.ankur_nayyar.01_wearable_gold_hk_family_weekly
ORDER BY dataset;
