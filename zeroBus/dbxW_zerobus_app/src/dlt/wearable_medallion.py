"""DLT: single pipeline — ZeroBus bronze Delta → **streaming bronze ST** → **append-only silver STs** → **gold MVs**.

**Flow**

1. **ZeroBus** writes to the configured UC Delta table (``wearables_bronze_table`` — e.g. ``wearables_zerobus``).
2. **``01_wearable_bronze_stream``** — ``create_streaming_table`` + ``@append_flow`` from ``readStream`` on that
   table (append-only micro-batches; watermark on ``ingested_at``).
3. **Silver** — one ``create_streaming_table`` + ``@append_flow`` per domain, each reading
   ``_read_stream("01_wearable_bronze_stream")`` with ``record_type`` filters. Watermark + ``dropDuplicates`` on
   natural keys for idempotency (append semantics, not ``row_number`` batch dedupe).
4. **Gold** — ``@dlt.table`` aggregations over ``dlt.read(...)`` of silver streaming tables; Lakeflow surfaces
   these as **materialized views** / incremental sinks (not streaming gold append_flows).

**One bundle pipeline** (``wearable_medallion.pipeline.yml``): ``continuous: true``, single Python module.

**Configuration** (pipeline ``configuration`` keys):

- ``wearables_bronze_table`` — FQN of the UC bronze Delta table.
- ``wearables_ingest_channel_filter`` — ``all`` (default), ``notebook_simulator``, or ``rest_app``.
  Rows are classified from ``headers``:

  - Explicit ``x-ingest-channel`` (set by the seed notebook as ``notebook_simulator`` and by the
    app ingest route as ``rest_app``).
  - Legacy notebook seeds: ``x-device-id = demo-notebook-seed`` → treated as ``notebook_simulator``.

  Use ``all`` in production so simulator + app + device data share one medallion; set a filter for
  demos that should only materialize one path.

**UC table names** use the ``01_`` prefix (for example ``01_wearable_deletes_silver``) so this pipeline
does not collide with legacy pipelines that still own unprefixed ``wearable_*`` tables.
"""

from __future__ import annotations

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window

import dlt

try:
    from pyspark import pipelines as _ldp  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    import dlt as _ldp

create_streaming_table = _ldp.create_streaming_table
append_flow = _ldp.append_flow

_read_stream = _ldp.read_stream if hasattr(_ldp, "read_stream") else dlt.read_stream

spark = SparkSession.builder.getOrCreate()

BRONZE_CONF = "wearables_bronze_table"
CHANNEL_FILTER_CONF = "wearables_ingest_channel_filter"


def _bronze_fqn() -> str:
    return spark.conf.get(BRONZE_CONF, "users.ankur_nayyar.wearables_zerobus")


def _ingest_channel_value() -> Column:
    """Logical source: notebook_simulator | rest_app (see module docstring)."""
    jh = F.to_json(F.col("headers"))
    explicit = F.lower(F.trim(F.get_json_object(jh, "$.x-ingest-channel")))
    device = F.lower(F.trim(F.get_json_object(jh, "$.x-device-id")))
    return (
        F.when((explicit.isNotNull()) & (F.length(explicit) > 0), explicit)
        .when(device == F.lit("demo-notebook-seed"), F.lit("notebook_simulator"))
        .otherwise(F.lit("rest_app"))
    )


def _ingest_channel_filter() -> Column:
    mode = spark.conf.get(CHANNEL_FILTER_CONF, "all").strip().lower()
    if mode in ("", "all"):
        return F.lit(True)
    if mode == "notebook_simulator":
        return _ingest_channel_value() == F.lit("notebook_simulator")
    if mode == "rest_app":
        return _ingest_channel_value() == F.lit("rest_app")
    return F.lit(True)


def _json_body_col():
    return F.to_json(F.col("body"))


def _parse_hk_iso_timestamp(col: Column) -> Column:
    """
    Parse HealthKit JSON ISO-8601 timestamps. Payloads commonly end with ``Z``;
    Spark's ``XXX`` zone pattern matches numeric offsets (+00:00), not ``Z``,
    so include explicit Z patterns then fall back to default parsing.
    """
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(col),
    )


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
    start_ts = _parse_hk_iso_timestamp(F.get_json_object(j, "$.start_date"))
    end_ts = _parse_hk_iso_timestamp(F.get_json_object(j, "$.end_date"))
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


# ---------------------------------------------------------------------------
# Streaming bronze — append-only mirror of ZeroBus Delta table
# ---------------------------------------------------------------------------

create_streaming_table(
    name="01_wearable_bronze_stream",
    comment="Append-only streaming bronze: micro-batches from configured wearables_zerobus (ZeroBus).",
    table_properties={"quality": "bronze"},
)


@append_flow(
    target="01_wearable_bronze_stream",
    name="zerobus_bronze_delta_stream",
    comment="Lakeflow reads ZeroBus UC Delta; append-only rows into pipeline bronze streaming table.",
)
def zerobus_bronze_delta_stream():
    fqn = _bronze_fqn()
    return (
        spark.readStream.format("delta")
        .table(fqn)
        .filter(_ingest_channel_filter())
        .withWatermark("ingested_at", "72 hours")
        .select(
            "record_id",
            "ingested_at",
            "body",
            "headers",
            "record_type",
            "source_platform",
            "user_id",
        )
    )


# ---------------------------------------------------------------------------
# Streaming silver — append-only (watermark + dropDuplicates on keys)
# ---------------------------------------------------------------------------

create_streaming_table(
    name="01_wearable_events_silver",
    comment="Append-only streaming silver: normalized events; dropDuplicates(event_id) within watermark.",
    table_properties={"quality": "silver"},
)


@append_flow(
    target="01_wearable_events_silver",
    name="bronze_stream_into_events_silver",
    comment="Bronze stream → normalized event rows.",
)
def bronze_stream_into_events_silver():
    b = _read_stream("01_wearable_bronze_stream")
    base = _silver_event_frame(b)
    return (
        base.filter(F.col("event_id").isNotNull() & F.col("metric_type").isNotNull())
        .withWatermark("ingested_at", "48 hours")
        .dropDuplicates(["event_id"])
    )


create_streaming_table(
    name="01_wearable_hk_quantity_samples_silver",
    comment="Append-only HK quantity samples; dropDuplicates(hk_uuid) within watermark.",
    table_properties={"quality": "silver"},
)


@append_flow(
    target="01_wearable_hk_quantity_samples_silver",
    name="bronze_stream_into_hk_samples_silver",
    comment="Bronze stream → HK quantity sample rows.",
)
def bronze_stream_into_hk_samples_silver():
    j = _json_body_col()
    b = _read_stream("01_wearable_bronze_stream").filter(F.col("record_type") == F.lit("samples"))
    selected = b.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.coalesce(F.get_json_object(j, "$.uuid"), F.col("record_id").cast("string")).alias("hk_uuid"),
        F.get_json_object(j, "$.type").alias("hk_type"),
        F.get_json_object(j, "$.unit").alias("hk_unit"),
        F.get_json_object(j, "$.value").cast("double").alias("value"),
        _parse_hk_iso_timestamp(F.get_json_object(j, "$.start_date")).alias("start_at"),
        _parse_hk_iso_timestamp(F.get_json_object(j, "$.end_date")).alias("end_at"),
        F.get_json_object(j, "$.source_name").alias("source_name"),
    ).where(F.col("hk_type").isNotNull())
    return selected.withWatermark("ingested_at", "36 hours").dropDuplicates(["hk_uuid"])


create_streaming_table(
    name="01_wearable_workouts_silver",
    comment="Append-only workouts; dropDuplicates(workout_uuid) within watermark.",
    table_properties={"quality": "silver"},
)


@append_flow(
    target="01_wearable_workouts_silver",
    name="bronze_stream_into_workouts_silver",
    comment="Bronze stream → workout rows.",
)
def bronze_stream_into_workouts_silver():
    j = _json_body_col()
    b = _read_stream("01_wearable_bronze_stream").filter(F.col("record_type") == F.lit("workouts"))
    sel = b.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.coalesce(F.get_json_object(j, "$.uuid"), F.col("record_id").cast("string")).alias("workout_uuid"),
        F.get_json_object(j, "$.activity_type").alias("activity_type"),
        F.get_json_object(j, "$.activity_type_raw").cast("long").alias("activity_type_raw"),
        _parse_hk_iso_timestamp(F.get_json_object(j, "$.start_date")).alias("start_at"),
        _parse_hk_iso_timestamp(F.get_json_object(j, "$.end_date")).alias("end_at"),
        F.get_json_object(j, "$.duration_seconds").cast("double").alias("duration_seconds"),
        F.get_json_object(j, "$.total_energy_burned_kcal").cast("double").alias("total_energy_burned_kcal"),
        F.get_json_object(j, "$.total_distance_meters").cast("double").alias("total_distance_meters"),
        F.get_json_object(j, "$.source_name").alias("source_name"),
    )
    return sel.withWatermark("ingested_at", "48 hours").dropDuplicates(["workout_uuid"])


create_streaming_table(
    name="01_wearable_sleep_stages_silver",
    comment="Append-only exploded sleep stages; dropDuplicates(stage_uuid) within watermark.",
    table_properties={"quality": "silver"},
)


@append_flow(
    target="01_wearable_sleep_stages_silver",
    name="bronze_stream_into_sleep_stages_silver",
    comment="Bronze stream → exploded sleep stage rows.",
)
def bronze_stream_into_sleep_stages_silver():
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
    b = _read_stream("01_wearable_bronze_stream").filter(F.col("record_type") == F.lit("sleep"))
    parsed = b.select(
        F.col("record_id").alias("sleep_record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        _parse_hk_iso_timestamp(F.get_json_object(j, "$.start_date")).alias("session_start_at"),
        _parse_hk_iso_timestamp(F.get_json_object(j, "$.end_date")).alias("session_end_at"),
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
    out = exploded.select(
        F.col("sleep_record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.col("session_start_at"),
        F.col("session_end_at"),
        F.coalesce(
            F.col("st.uuid").cast("string"),
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("sleep_record_id").cast("string"),
                    F.col("st.stage").cast("string"),
                    F.col("st.start_date").cast("string"),
                ),
                256,
            ),
        ).alias("stage_uuid"),
        F.col("st.stage").alias("sleep_stage"),
        _parse_hk_iso_timestamp(F.col("st.start_date")).alias("stage_start_at"),
        _parse_hk_iso_timestamp(F.col("st.end_date")).alias("stage_end_at"),
    )
    return out.withWatermark("ingested_at", "48 hours").dropDuplicates(["stage_uuid"])


create_streaming_table(
    name="01_wearable_activity_ring_daily_silver",
    comment="Append-only activity ring summaries; dropDuplicates(record_id) within watermark.",
    table_properties={"quality": "silver"},
)


@append_flow(
    target="01_wearable_activity_ring_daily_silver",
    name="bronze_stream_into_activity_ring_silver",
    comment="Bronze stream → daily activity ring rows.",
)
def bronze_stream_into_activity_ring_silver():
    j = _json_body_col()
    b = _read_stream("01_wearable_bronze_stream").filter(F.col("record_type") == F.lit("activity_summaries"))
    sel = b.select(
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
    return sel.withWatermark("ingested_at", "48 hours").dropDuplicates(["record_id"])


create_streaming_table(
    name="01_wearable_deletes_silver",
    comment="Append-only delete rows; dropDuplicates(record_id) within watermark.",
    table_properties={"quality": "silver"},
)


@append_flow(
    target="01_wearable_deletes_silver",
    name="bronze_stream_into_deletes_silver",
    comment="Bronze stream → delete rows.",
)
def bronze_stream_into_deletes_silver():
    j = _json_body_col()
    b = _read_stream("01_wearable_bronze_stream").filter(F.col("record_type") == F.lit("deletes"))
    sel = b.select(
        F.col("record_id"),
        F.col("ingested_at"),
        F.col("user_id"),
        F.get_json_object(j, "$.uuid").alias("deleted_uuid"),
        F.get_json_object(j, "$.sample_type").alias("deleted_sample_type"),
    )
    return sel.withWatermark("ingested_at", "48 hours").dropDuplicates(["record_id"])


@dlt.table(
    name="01_wearable_hk_quantity_daily_gold",
    comment="Daily aggregates for HK quantity samples (dashboard-friendly).",
    table_properties={"quality": "gold"},
)
def wearable_hk_quantity_daily_gold():
    q = dlt.read("01_wearable_hk_quantity_samples_silver")
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
    name="01_wearable_subject_daily_gold",
    comment="Daily aggregates across all normalized event types (numeric values only).",
    table_properties={"quality": "gold"},
)
def wearable_subject_daily_gold():
    silver = dlt.read("01_wearable_events_silver")
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
    name="01_wearable_gold_daily_steps",
    comment="Per-user daily step totals from HKQuantityTypeIdentifierStepCount samples.",
    table_properties={"quality": "gold"},
)
def wearable_gold_daily_steps():
    q = dlt.read("01_wearable_hk_quantity_samples_silver")
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
    name="01_wearable_gold_activity_enriched_daily",
    comment="Activity rings merged with daily step totals and goal ratios.",
    table_properties={"quality": "gold"},
)
def wearable_gold_activity_enriched_daily():
    ring = dlt.read("01_wearable_activity_ring_daily_silver").withColumnRenamed("summary_date", "day")
    steps = dlt.read("01_wearable_gold_daily_steps")
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
    name="01_wearable_gold_sleep_nightly",
    comment="Per-night sleep minutes by stage (Apple sleep stage strings).",
    table_properties={"quality": "gold"},
)
def wearable_gold_sleep_nightly():
    st = dlt.read("01_wearable_sleep_stages_silver").filter(
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
    name="01_wearable_gold_weekly_workout_summary",
    comment="Weekly workout volume and intensity proxies per user.",
    table_properties={"quality": "gold"},
)
def wearable_gold_weekly_workout_summary():
    w = dlt.read("01_wearable_workouts_silver").filter(F.col("start_at").isNotNull())
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
    name="01_wearable_gold_cardio_vitals_daily",
    comment="Daily resting HR, HRV (SDNN), and SpO2 averages when present.",
    table_properties={"quality": "gold"},
)
def wearable_gold_cardio_vitals_daily():
    hq = dlt.read("01_wearable_hk_quantity_samples_silver").filter(F.col("start_at").isNotNull())
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
    name="01_wearable_gold_heart_rate_intraday_daily",
    comment="All non-resting heart-rate samples aggregated per calendar day.",
    table_properties={"quality": "gold"},
)
def wearable_gold_heart_rate_intraday_daily():
    hq = (
        dlt.read("01_wearable_hk_quantity_samples_silver")
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
    name="01_wearable_gold_bronze_ingest_daily",
    comment="Bronze ingest volume by day, user, and record_type (long form for charts).",
    table_properties={"quality": "gold"},
)
def wearable_gold_bronze_ingest_daily():
    b = dlt.read("01_wearable_bronze_stream")
    return b.groupBy(F.to_date("ingested_at").alias("ingest_day"), "user_id", "record_type").agg(
        F.count(F.lit(1)).alias("row_count")
    )


@dlt.table(
    name="01_wearable_gold_hk_family_weekly",
    comment="Weekly rollups of HK metric family (steps, energy, distance, etc.) for trend charts.",
    table_properties={"quality": "gold"},
)
def wearable_gold_hk_family_weekly():
    hq = dlt.read("01_wearable_hk_quantity_samples_silver").filter(F.col("value").isNotNull())
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
    name="01_wearable_vw_gold_activity_last_45d",
    comment="Last 45 days of enriched daily activity KPIs.",
)
def wearable_vw_gold_activity_last_45d():
    g = dlt.read("01_wearable_gold_activity_enriched_daily")
    cutoff = F.date_sub(F.current_date(), 45)
    return g.filter(F.col("day") >= cutoff)


@dlt.view(
    name="01_wearable_vw_gold_sleep_last_30_sessions",
    comment="Sleep nightly totals for the most recent 30 calendar nights per user.",
)
def wearable_vw_gold_sleep_last_30_sessions():
    s = dlt.read("01_wearable_gold_sleep_nightly")
    w = Window.partitionBy("user_id").orderBy(F.col("sleep_night").desc())
    ranked = s.withColumn("_rn", F.row_number().over(w))
    return ranked.filter(F.col("_rn") <= 30).drop("_rn")


@dlt.view(
    name="01_wearable_vw_gold_workouts_recent",
    comment="Weekly workout summary for the last 12 weeks.",
)
def wearable_vw_gold_workouts_recent():
    w = dlt.read("01_wearable_gold_weekly_workout_summary")
    cutoff = F.date_sub(F.current_timestamp(), 84)
    return w.filter(F.col("week_start") >= cutoff)


