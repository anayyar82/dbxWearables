-- Silver: sleep stage minutes per night (DLT: wearable_sleep_stages_silver)
SELECT
  user_id,
  to_date(session_start_at) AS sleep_night,
  sleep_stage,
  round(sum((unix_timestamp(stage_end_at) - unix_timestamp(stage_start_at)) / 60.0), 1) AS stage_minutes
FROM users.ankur_nayyar.wearable_sleep_stages_silver
WHERE stage_start_at IS NOT NULL AND stage_end_at IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 2 DESC, 4 DESC;
