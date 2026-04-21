-- Sleep profile by user — window + user pattern (top 50).
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  user_id,
  ROUND(AVG(deep_sleep_min), 1) AS avg_deep_min,
  ROUND(AVG(rem_sleep_min), 1) AS avg_rem_min,
  ROUND(AVG(total_tracked_min), 0) AS avg_tracked_min,
  COUNT(*) AS nights
FROM `users`.`ankur_nayyar`.`01_wearable_gold_sleep_nightly`
WHERE sleep_night BETWEEN :startDate AND :endDate
  AND user_id LIKE :userPattern
GROUP BY user_id
ORDER BY nights DESC NULLS LAST
LIMIT 50;
