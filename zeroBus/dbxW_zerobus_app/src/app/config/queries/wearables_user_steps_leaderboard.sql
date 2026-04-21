-- Step leaderboard — window + user pattern (top 50; avoids NULL bind during AppKit DESCRIBE).
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  user_id,
  ROUND(AVG(total_steps), 0) AS avg_steps,
  MAX(total_steps) AS peak_steps,
  ROUND(SUM(total_steps), 0) AS sum_steps
FROM `users`.`ankur_nayyar`.`01_wearable_gold_daily_steps`
WHERE day BETWEEN :startDate AND :endDate
  AND user_id LIKE :userPattern
GROUP BY user_id
ORDER BY avg_steps DESC NULLS LAST
LIMIT 50;
