-- Operational: row counts across DLT outputs (edit FQN prefix if needed)
SELECT 'wearable_events_silver' AS dataset, COUNT(*) AS row_count
FROM users.ankur_nayyar.wearable_events_silver
UNION ALL
SELECT 'wearable_hk_quantity_samples_silver', COUNT(*) FROM users.ankur_nayyar.wearable_hk_quantity_samples_silver
UNION ALL
SELECT 'wearable_workouts_silver', COUNT(*) FROM users.ankur_nayyar.wearable_workouts_silver
UNION ALL
SELECT 'wearable_sleep_stages_silver', COUNT(*) FROM users.ankur_nayyar.wearable_sleep_stages_silver
UNION ALL
SELECT 'wearable_activity_ring_daily_silver', COUNT(*) FROM users.ankur_nayyar.wearable_activity_ring_daily_silver
UNION ALL
SELECT 'wearable_hk_quantity_daily_gold', COUNT(*) FROM users.ankur_nayyar.wearable_hk_quantity_daily_gold
UNION ALL
SELECT 'wearable_subject_daily_gold', COUNT(*) FROM users.ankur_nayyar.wearable_subject_daily_gold
ORDER BY dataset;
