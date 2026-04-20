-- Nightly sleep stage totals (gold)
SELECT
  sleep_night,
  user_id,
  deep_sleep_min,
  rem_sleep_min,
  core_sleep_min,
  awake_min,
  in_bed_min,
  total_tracked_min,
  sleep_sessions
FROM users.ankur_nayyar.wearable_gold_sleep_nightly
WHERE sleep_night >= current_date() - INTERVAL 90 DAY
ORDER BY sleep_night DESC, user_id;
