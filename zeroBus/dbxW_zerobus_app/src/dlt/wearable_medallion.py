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
