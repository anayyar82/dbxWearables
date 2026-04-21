-- Workout volume rolled up per user for weeks whose start falls in the window (top 50).
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  user_id,
  SUM(workout_count) AS workouts,
  ROUND(SUM(total_duration_min), 0) AS total_minutes,
  ROUND(SUM(total_energy_kcal), 0) AS total_kcal
FROM `users`.`ankur_nayyar`.`01_wearable_gold_weekly_workout_summary`
WHERE CAST(week_start AS DATE) BETWEEN :startDate AND :endDate
  AND user_id LIKE :userPattern
GROUP BY user_id
ORDER BY workouts DESC NULLS LAST
LIMIT 50;
