-- Resting HR daily average (when present).
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  day,
  ROUND(AVG(resting_hr_avg), 1) AS avg_resting_hr,
  COUNT(DISTINCT user_id) AS users_reporting
FROM `users`.`ankur_nayyar`.`01_wearable_gold_cardio_vitals_daily`
WHERE day BETWEEN :startDate AND :endDate
  AND user_id LIKE :userPattern
  AND resting_hr_avg IS NOT NULL
GROUP BY day
ORDER BY day;
