"""DLT: ZeroBus bronze ``wearables_zerobus`` -> Apple Health–shaped silver + gold.

Configure pipeline key ``wearables_bronze_table`` to the fully qualified bronze
Delta table (e.g. ``users.some_schema.wearables_zerobus``).

Silver uses **batch** reads from bronze and ``row_number`` deduplication on the
normalized ``event_id`` (streaming + ``row_number`` is not supported on this path).
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

import dlt

spark = SparkSession.builder.getOrCreate()

BRONZE_CONF = "wearables_bronze_table"


def _bronze_fqn() -> str:
    return spark.conf.get(BRONZE_CONF, "users.ankur_nayyar.wearables_zerobus")


def _bronze_df():
    return spark.read.format("delta").table(_bronze_fqn())


def _json_body_col():
    return F.to_json(F.col("body"))


def _silver_event_frame(df):
    j = _json_body_col()
    event_id = F.coalesce(F.get_json_object(j, "$.uuid"), F.col("record_id"))
    metric_type = F.coalesce(
        F.get_json_object(j, "$.type"),
        F.get_json_object(j, "$.sample_type"),
        F.get_json_object(j, "$.activity_type"),
        F.when(F.col("record_type") == F.lit("sleep"), F.lit("sleep"))
        .when(F.col("record_type") == F.lit("activity_summaries"), F.lit("activity_summary"))
        .otherwise(F.col("record_type")),
    )
    start_ts = F.coalesce(
        F.to_timestamp(F.get_json_object(j, "$.start_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(F.get_json_object(j, "$.start_date")),
    )
    end_ts = F.coalesce(
        F.to_timestamp(F.get_json_object(j, "$.end_date"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(F.get_json_object(j, "$.end_date")),
    )
    effective_time = F.coalesce(start_ts, end_ts, F.col("ingested_at"))
    sample_value = F.get_json_object(j, "$.value").cast("double")
    workout_duration = F.get_json_object(j, "$.duration_seconds").cast("double")
    workout_energy = F.get_json_object(j, "$.total_energy_burned_kcal").cast("double")
    workout_distance = F.get_json_object(j, "$.total_distance_meters").cast("double")
    activity_active_energy = F.get_json_object(j, "$.active_energy_burned_kcal").cast("double")
    activity_exercise_minutes = F.get_json_object(j, "$.exercise_minutes").cast("double")
    activity_stand_hours = F.get_json_object(j, "$.stand_hours").cast("double")
    value_number = (
        F.when(sample_value.isNotNull(), sample_value)
        .when(workout_duration.isNotNull(), workout_duration)
        .when(workout_energy.isNotNull(), workout_energy)
        .when(workout_distance.isNotNull(), workout_distance)
        .when(activity_active_energy.isNotNull(), activity_active_energy)
        .when(activity_exercise_minutes.isNotNull(), activity_exercise_minutes)
        .when(activity_stand_hours.isNotNull(), activity_stand_hours)
    )
    return df.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("record_type"),
        F.col("source_platform"),
        F.col("user_id"),
        event_id.alias("event_id"),
        F.col("user_id").alias("subject_id"),
        metric_type.alias("metric_type"),
        effective_time.alias("effective_time"),
        value_number.alias("value_number"),
    )


@dlt.table(
    name="wearable_events_silver",
    comment="All record types normalized for dedupe and downstream joins.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("event_id_present", "event_id IS NOT NULL")
@dlt.expect_or_drop("metric_present", "metric_type IS NOT NULL")
def wearable_events_silver():
    base = _silver_event_frame(_bronze_df())
    w = Window.partitionBy("event_id").orderBy(
        F.col("effective_time").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    return (
        base.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == F.lit(1))
        .drop("_rn")
    )


@dlt.table(
    name="wearable_hk_quantity_samples_silver",
    comment="Apple HealthKit quantity samples (record_type = samples).",
    table_properties={"quality": "silver"},
)
def wearable_hk_quantity_samples_silver():
    j = _json_body_col()
    df = _bronze_df().filter(F.col("record_type") == F.lit("samples"))
    return df.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.get_json_object(j, "$.uuid").alias("hk_uuid"),
        F.get_json_object(j, "$.type").alias("hk_type"),
        F.get_json_object(j, "$.unit").alias("hk_unit"),
        F.get_json_object(j, "$.value").cast("double").alias("value"),
        F.to_timestamp(F.get_json_object(j, "$.start_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("start_at"),
        F.to_timestamp(F.get_json_object(j, "$.end_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("end_at"),
        F.get_json_object(j, "$.source_name").alias("source_name"),
    ).where(F.col("hk_type").isNotNull())


@dlt.table(
    name="wearable_workouts_silver",
    comment="Apple HealthKit workouts (record_type = workouts).",
    table_properties={"quality": "silver"},
)
def wearable_workouts_silver():
    j = _json_body_col()
    df = _bronze_df().filter(F.col("record_type") == F.lit("workouts"))
    return df.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.get_json_object(j, "$.uuid").alias("workout_uuid"),
        F.get_json_object(j, "$.activity_type").alias("activity_type"),
        F.get_json_object(j, "$.activity_type_raw").cast("long").alias("activity_type_raw"),
        F.to_timestamp(F.get_json_object(j, "$.start_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("start_at"),
        F.to_timestamp(F.get_json_object(j, "$.end_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("end_at"),
        F.get_json_object(j, "$.duration_seconds").cast("double").alias("duration_seconds"),
        F.get_json_object(j, "$.total_energy_burned_kcal").cast("double").alias("total_energy_burned_kcal"),
        F.get_json_object(j, "$.total_distance_meters").cast("double").alias("total_distance_meters"),
        F.get_json_object(j, "$.source_name").alias("source_name"),
    )


@dlt.table(
    name="wearable_sleep_stages_silver",
    comment="Exploded per-stage rows from Apple HealthKit sleep JSON.",
    table_properties={"quality": "silver"},
)
def wearable_sleep_stages_silver():
    j = _json_body_col()
    stage_schema = ArrayType(
        StructType(
            [
                StructField("uuid", StringType(), True),
                StructField("stage", StringType(), True),
                StructField("start_date", StringType(), True),
                StructField("end_date", StringType(), True),
            ]
        )
    )
    df = _bronze_df().filter(F.col("record_type") == F.lit("sleep"))
    parsed = df.select(
        F.col("record_id").alias("sleep_record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.to_timestamp(F.get_json_object(j, "$.start_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("session_start_at"),
        F.to_timestamp(F.get_json_object(j, "$.end_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("session_end_at"),
        F.from_json(F.get_json_object(j, "$.stages"), stage_schema).alias("stages"),
    )
    exploded = parsed.filter(F.size(F.col("stages")) > 0).select(
        "sleep_record_id",
        "ingested_at",
        "user_id",
        "session_start_at",
        "session_end_at",
        F.explode_outer("stages").alias("st"),
    )
    return exploded.select(
        F.col("sleep_record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.col("session_start_at"),
        F.col("session_end_at"),
        F.col("st.uuid").alias("stage_uuid"),
        F.col("st.stage").alias("sleep_stage"),
        F.to_timestamp(F.col("st.start_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("stage_start_at"),
        F.to_timestamp(F.col("st.end_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("stage_end_at"),
    )


@dlt.table(
    name="wearable_activity_ring_daily_silver",
    comment="Daily Activity ring summaries (Move / Exercise / Stand).",
    table_properties={"quality": "silver"},
)
def wearable_activity_ring_daily_silver():
    j = _json_body_col()
    df = _bronze_df().filter(F.col("record_type") == F.lit("activity_summaries"))
    return df.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.to_date(F.get_json_object(j, "$.date")).alias("summary_date"),
        F.get_json_object(j, "$.active_energy_burned_kcal").cast("double").alias("active_energy_burned_kcal"),
        F.get_json_object(j, "$.active_energy_burned_goal_kcal").cast("double").alias("active_energy_burned_goal_kcal"),
        F.get_json_object(j, "$.exercise_minutes").cast("double").alias("exercise_minutes"),
        F.get_json_object(j, "$.exercise_minutes_goal").cast("double").alias("exercise_minutes_goal"),
        F.get_json_object(j, "$.stand_hours").cast("long").alias("stand_hours"),
        F.get_json_object(j, "$.stand_hours_goal").cast("long").alias("stand_hours_goal"),
    ).where(F.col("summary_date").isNotNull())


@dlt.table(
    name="wearable_deletes_silver",
    comment="Deletion rows (UUID + sample_type) for soft-delete joins.",
    table_properties={"quality": "silver"},
)
def wearable_deletes_silver():
    j = _json_body_col()
    df = _bronze_df().filter(F.col("record_type") == F.lit("deletes"))
    return df.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.get_json_object(j, "$.uuid").alias("deleted_uuid"),
        F.get_json_object(j, "$.sample_type").alias("deleted_sample_type"),
    )


@dlt.table(
    name="wearable_hk_quantity_daily_gold",
    comment="Daily aggregates for HK quantity samples (dashboard-friendly).",
    table_properties={"quality": "gold"},
)
def wearable_hk_quantity_daily_gold():
    q = dlt.read("wearable_hk_quantity_samples_silver")
    return (
        q.filter(F.col("hk_type").isNotNull() & F.col("value").isNotNull())
        .groupBy(
            F.col("user_id"),
            F.col("hk_type"),
            F.to_date(F.col("start_at")).alias("day"),
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count(F.lit(1)).alias("sample_rows"),
        )
    )


@dlt.table(
    name="wearable_subject_daily_gold",
    comment="Daily aggregates across all normalized event types (numeric values only).",
    table_properties={"quality": "gold"},
)
def wearable_subject_daily_gold():
    silver = dlt.read("wearable_events_silver")
    return (
        silver.filter(F.col("value_number").isNotNull())
        .groupBy(
            "subject_id",
            "metric_type",
            F.to_date("effective_time").alias("day"),
        )
        .agg(
            F.avg("value_number").alias("avg_value_number"),
            F.min("value_number").alias("min_value_number"),
            F.max("value_number").alias("max_value_number"),
            F.count(F.lit(1)).alias("event_count"),
        )
    )


# ---------------------------------------------------------------------------
# Gold — curated business / analytics tables (for Lakeview & SQL)
# ---------------------------------------------------------------------------


@dlt.table(
    name="wearable_gold_daily_steps",
    comment="Per-user daily step totals from HKQuantityTypeIdentifierStepCount samples.",
    table_properties={"quality": "gold"},
)
def wearable_gold_daily_steps():
    q = dlt.read("wearable_hk_quantity_samples_silver")
    day = F.to_date(F.col("start_at"))
    return (
        q.filter(F.col("hk_type").contains("StepCount"))
        .filter(F.col("value").isNotNull())
        .groupBy("user_id", day.alias("day"))
        .agg(
            F.sum("value").alias("total_steps"),
            F.count(F.lit(1)).alias("sample_intervals"),
        )
    )


@dlt.table(
    name="wearable_gold_activity_enriched_daily",
    comment="Activity rings merged with daily step totals and goal ratios.",
    table_properties={"quality": "gold"},
)
def wearable_gold_activity_enriched_daily():
    ring = dlt.read("wearable_activity_ring_daily_silver").withColumnRenamed("summary_date", "day")
    steps = dlt.read("wearable_gold_daily_steps")
    joined = ring.join(steps, on=["user_id", "day"], how="left")
    return joined.select(
        F.col("user_id"),
        F.col("day"),
        F.col("active_energy_burned_kcal"),
        F.col("active_energy_burned_goal_kcal"),
        F.round(
            F.col("active_energy_burned_kcal") / F.nullif(F.col("active_energy_burned_goal_kcal"), F.lit(0.0)),
            3,
        ).alias("move_ring_ratio"),
        F.col("exercise_minutes"),
        F.col("exercise_minutes_goal"),
        F.round(
            F.col("exercise_minutes") / F.nullif(F.col("exercise_minutes_goal"), F.lit(0.0)),
            3,
        ).alias("exercise_ring_ratio"),
        F.col("stand_hours").cast("double").alias("stand_hours"),
        F.col("stand_hours_goal").cast("double").alias("stand_hours_goal"),
        F.round(
            F.col("stand_hours") / F.nullif(F.col("stand_hours_goal"), F.lit(0.0)),
            3,
        ).alias("stand_ring_ratio"),
        F.col("total_steps"),
        F.col("sample_intervals"),
    )


@dlt.table(
    name="wearable_gold_sleep_nightly",
    comment="Per-night sleep minutes by stage (Apple sleep stage strings).",
    table_properties={"quality": "gold"},
)
def wearable_gold_sleep_nightly():
    st = dlt.read("wearable_sleep_stages_silver").filter(
        F.col("stage_start_at").isNotNull() & F.col("stage_end_at").isNotNull()
    )
    mins = (F.unix_timestamp("stage_end_at") - F.unix_timestamp("stage_start_at")) / F.lit(60.0)
    st = st.withColumn("stage_minutes", mins)
    night = F.to_date("session_start_at")
    return (
        st.groupBy("user_id", night.alias("sleep_night"))
        .agg(
            F.sum(F.when(F.col("sleep_stage") == "asleepDeep", F.col("stage_minutes")).otherwise(F.lit(0.0))).alias(
                "deep_sleep_min"
            ),
            F.sum(F.when(F.col("sleep_stage") == "asleepREM", F.col("stage_minutes")).otherwise(F.lit(0.0))).alias(
                "rem_sleep_min"
            ),
            F.sum(F.when(F.col("sleep_stage") == "asleepCore", F.col("stage_minutes")).otherwise(F.lit(0.0))).alias(
                "core_sleep_min"
            ),
            F.sum(F.when(F.col("sleep_stage") == "awake", F.col("stage_minutes")).otherwise(F.lit(0.0))).alias(
                "awake_min"
            ),
            F.sum(F.when(F.col("sleep_stage") == "inBed", F.col("stage_minutes")).otherwise(F.lit(0.0))).alias(
                "in_bed_min"
            ),
            F.sum("stage_minutes").alias("total_tracked_min"),
            F.countDistinct("sleep_record_id").alias("sleep_sessions"),
        )
    )


@dlt.table(
    name="wearable_gold_weekly_workout_summary",
    comment="Weekly workout volume and intensity proxies per user.",
    table_properties={"quality": "gold"},
)
def wearable_gold_weekly_workout_summary():
    w = dlt.read("wearable_workouts_silver").filter(F.col("start_at").isNotNull())
    week = F.date_trunc("week", F.col("start_at"))
    return (
        w.groupBy("user_id", week.alias("week_start"))
        .agg(
            F.count(F.lit(1)).alias("workout_count"),
            F.sum(F.col("duration_seconds") / F.lit(60.0)).alias("total_duration_min"),
            F.sum("total_energy_burned_kcal").alias("total_energy_kcal"),
            F.sum("total_distance_meters").alias("total_distance_m"),
            (
                F.sum("total_distance_meters")
                / F.nullif(F.sum("duration_seconds"), F.lit(0.0))
            ).alias("avg_speed_m_per_s"),
            F.countDistinct("activity_type").alias("distinct_activity_types"),
        )
    )


@dlt.table(
    name="wearable_gold_cardio_vitals_daily",
    comment="Daily resting HR, HRV (SDNN), and SpO2 averages when present.",
    table_properties={"quality": "gold"},
)
def wearable_gold_cardio_vitals_daily():
    hq = dlt.read("wearable_hk_quantity_samples_silver").filter(F.col("start_at").isNotNull())
    day = F.to_date(F.col("start_at"))
    rhr = (
        hq.filter(F.col("hk_type").contains("RestingHeartRate"))
        .groupBy("user_id", day.alias("day"))
        .agg(F.avg("value").alias("resting_hr_avg"))
    )
    hrv = (
        hq.filter(F.col("hk_type").contains("HeartRateVariability"))
        .groupBy("user_id", day.alias("day"))
        .agg(F.avg("value").alias("hrv_sdnn_avg"))
    )
    spo = (
        hq.filter(F.col("hk_type").contains("OxygenSaturation"))
        .groupBy("user_id", day.alias("day"))
        .agg(F.avg("value").alias("spo2_avg"))
    )
    hr = (
        hq.filter(F.col("hk_type").contains("HeartRate"))
        .filter(~F.col("hk_type").contains("Resting"))
        .groupBy("user_id", day.alias("day"))
        .agg(F.avg("value").alias("heart_rate_avg"), F.max("value").alias("heart_rate_max"))
    )
    base = rhr.join(hrv, on=["user_id", "day"], how="outer").join(spo, on=["user_id", "day"], how="outer")
    return base.join(hr, on=["user_id", "day"], how="outer")


@dlt.table(
    name="wearable_gold_heart_rate_intraday_daily",
    comment="All non-resting heart-rate samples aggregated per calendar day.",
    table_properties={"quality": "gold"},
)
def wearable_gold_heart_rate_intraday_daily():
    hq = (
        dlt.read("wearable_hk_quantity_samples_silver")
        .filter(F.col("hk_type").contains("HeartRate"))
        .filter(~F.col("hk_type").contains("Resting"))
        .filter(F.col("value").isNotNull())
    )
    day = F.to_date(F.col("start_at"))
    return hq.groupBy("user_id", day.alias("day")).agg(
        F.avg("value").alias("avg_hr"),
        F.min("value").alias("min_hr"),
        F.max("value").alias("max_hr"),
        F.count(F.lit(1)).alias("reading_count"),
    )


@dlt.table(
    name="wearable_gold_bronze_ingest_daily",
    comment="Bronze ingest volume by day, user, and record_type (long form for charts).",
    table_properties={"quality": "gold"},
)
def wearable_gold_bronze_ingest_daily():
    b = _bronze_df()
    return b.groupBy(F.to_date("ingested_at").alias("ingest_day"), "user_id", "record_type").agg(
        F.count(F.lit(1)).alias("row_count")
    )


@dlt.table(
    name="wearable_gold_hk_family_weekly",
    comment="Weekly rollups of HK metric family (steps, energy, distance, etc.) for trend charts.",
    table_properties={"quality": "gold"},
)
def wearable_gold_hk_family_weekly():
    hq = dlt.read("wearable_hk_quantity_samples_silver").filter(F.col("value").isNotNull())
    week = F.date_trunc("week", F.col("start_at"))
    family = (
        F.when(F.col("hk_type").contains("StepCount"), F.lit("steps"))
        .when(F.col("hk_type").contains("ActiveEnergy"), F.lit("active_energy"))
        .when(F.col("hk_type").contains("DistanceWalking"), F.lit("distance"))
        .when(F.col("hk_type").contains("HeartRate"), F.lit("heart_rate"))
        .when(F.col("hk_type").contains("OxygenSaturation"), F.lit("spo2"))
        .when(F.col("hk_type").contains("FlightsClimbed"), F.lit("flights"))
        .otherwise(F.lit("other"))
    )
    return (
        hq.withColumn("metric_family", family)
        .groupBy("user_id", week.alias("week_start"), "metric_family")
        .agg(
            F.avg("value").alias("avg_value"),
            F.sum("value").alias("sum_value"),
            F.count(F.lit(1)).alias("readings"),
        )
    )


# ---------------------------------------------------------------------------
# Views — thin slices for dashboards (filter heavy tables here, not in UI)
# ---------------------------------------------------------------------------


@dlt.view(
    name="wearable_vw_gold_activity_last_45d",
    comment="Last 45 days of enriched daily activity KPIs.",
)
def wearable_vw_gold_activity_last_45d():
    g = dlt.read("wearable_gold_activity_enriched_daily")
    cutoff = F.date_sub(F.current_date(), 45)
    return g.filter(F.col("day") >= cutoff)


@dlt.view(
    name="wearable_vw_gold_sleep_last_30_sessions",
    comment="Sleep nightly totals for the most recent 30 calendar nights per user.",
)
def wearable_vw_gold_sleep_last_30_sessions():
    s = dlt.read("wearable_gold_sleep_nightly")
    w = Window.partitionBy("user_id").orderBy(F.col("sleep_night").desc())
    ranked = s.withColumn("_rn", F.row_number().over(w))
    return ranked.filter(F.col("_rn") <= 30).drop("_rn")


@dlt.view(
    name="wearable_vw_gold_workouts_recent",
    comment="Weekly workout summary for the last 12 weeks.",
)
def wearable_vw_gold_workouts_recent():
    w = dlt.read("wearable_gold_weekly_workout_summary")
    cutoff = F.date_sub(F.current_timestamp(), 84)
    return w.filter(F.col("week_start") >= cutoff)
