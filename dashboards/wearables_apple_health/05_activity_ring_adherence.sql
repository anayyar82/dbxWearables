-- Silver: Activity ring goals vs actuals (DLT: 01_wearable_activity_ring_daily_silver)
SELECT
  summary_date,
  user_id,
  active_energy_burned_kcal,
  active_energy_burned_goal_kcal,
  round(active_energy_burned_kcal / nullif(active_energy_burned_goal_kcal, 0), 2) AS move_ring_ratio,
  exercise_minutes,
  exercise_minutes_goal,
  stand_hours,
  stand_hours_goal
FROM users.ankur_nayyar.01_wearable_activity_ring_daily_silver
WHERE summary_date >= current_date() - INTERVAL 30 DAYS
ORDER BY summary_date DESC;
