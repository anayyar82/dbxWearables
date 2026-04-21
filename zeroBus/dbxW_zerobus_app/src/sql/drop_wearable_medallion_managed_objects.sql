-- Drop UC objects for the **01_**-prefixed medallion (streaming bronze/silver + gold MVs in wearable_medallion.py).
-- Adjust catalog.schema if needed. Use after stopping the pipeline that owns these objects, or to reset a dev schema.
--
-- Legacy unprefixed tables (wearable_* without 01_) are still owned by older pipelines — delete those pipelines
-- or DROP the old names separately.

DROP VIEW IF EXISTS users.ankur_nayyar.01_wearable_vw_gold_activity_last_45d;
DROP VIEW IF EXISTS users.ankur_nayyar.01_wearable_vw_gold_sleep_last_30_sessions;
DROP VIEW IF EXISTS users.ankur_nayyar.01_wearable_vw_gold_workouts_recent;

DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_activity_enriched_daily;

DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_daily_steps;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_hk_quantity_daily_gold;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_subject_daily_gold;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_sleep_nightly;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_weekly_workout_summary;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_cardio_vitals_daily;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_heart_rate_intraday_daily;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_bronze_ingest_daily;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_gold_hk_family_weekly;

DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_events_silver;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_hk_quantity_samples_silver;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_workouts_silver;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_sleep_stages_silver;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_activity_ring_daily_silver;
DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_deletes_silver;

DROP TABLE IF EXISTS users.ankur_nayyar.01_wearable_bronze_stream;
