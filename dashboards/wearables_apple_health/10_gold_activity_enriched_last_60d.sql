-- Activity rings + steps (gold), last 60 days
SELECT day, user_id, move_ring_ratio, exercise_ring_ratio, stand_ring_ratio, total_steps
FROM users.ankur_nayyar.01_wearable_gold_activity_enriched_daily
WHERE day >= current_date() - INTERVAL 60 DAY
ORDER BY day DESC, user_id;
