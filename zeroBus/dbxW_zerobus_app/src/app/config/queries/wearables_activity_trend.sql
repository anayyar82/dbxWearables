-- Daily activity cohort — filtered by calendar days + user pattern.
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  day,
  COUNT(DISTINCT user_id) AS active_users,
  ROUND(AVG(move_ring_ratio), 3) AS avg_move_ratio,
  ROUND(AVG(total_steps), 0) AS avg_steps
FROM `users`.`ankur_nayyar`.`01_wearable_gold_activity_enriched_daily`
WHERE day BETWEEN :startDate AND :endDate
  AND user_id LIKE :userPattern
GROUP BY day
ORDER BY day;
