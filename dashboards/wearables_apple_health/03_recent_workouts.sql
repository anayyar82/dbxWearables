-- Silver: recent workouts (DLT: wearable_workouts_silver)
SELECT
  start_at,
  user_id,
  activity_type,
  round(duration_seconds / 60, 1) AS duration_min,
  total_energy_burned_kcal,
  round(total_distance_meters / 1000, 2) AS distance_km,
  ingested_at
FROM users.ankur_nayyar.wearable_workouts_silver
WHERE start_at >= current_timestamp() - INTERVAL 14 DAYS
ORDER BY start_at DESC
LIMIT 200;
