-- KPI strip — respects date window + user pattern on lakehouse tables.
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  (SELECT COUNT(*) FROM users.ankur_nayyar.wearables_zerobus b
   WHERE CAST(b.ingested_at AS DATE) BETWEEN :startDate AND :endDate
     AND b.user_id LIKE :userPattern) AS bronze_rows_in_window,
  (SELECT COUNT(*) FROM users.ankur_nayyar.wearables_zerobus) AS bronze_rows_all_time,
  (
    SELECT COUNT(DISTINCT user_id)
    FROM `users`.`ankur_nayyar`.`01_wearable_gold_activity_enriched_daily` a
    WHERE a.day BETWEEN :startDate AND :endDate
      AND a.user_id LIKE :userPattern
  ) AS active_users_in_window,
  (
    SELECT COUNT(DISTINCT user_id)
    FROM `users`.`ankur_nayyar`.`01_wearable_gold_daily_steps` s
    WHERE s.day BETWEEN :startDate AND :endDate
      AND s.user_id LIKE :userPattern
  ) AS users_with_steps_in_window,
  (
    SELECT MAX(b.ingested_at)
    FROM users.ankur_nayyar.wearables_zerobus b
    WHERE CAST(b.ingested_at AS DATE) BETWEEN :startDate AND :endDate
      AND b.user_id LIKE :userPattern
  ) AS last_bronze_ingest_in_window;
