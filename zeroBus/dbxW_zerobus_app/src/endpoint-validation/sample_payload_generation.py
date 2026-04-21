"""
Apple HealthKit–shaped synthetic payloads for ZeroBus ingest demos.

Used by ``validate-zerobus-ingest`` notebook. Keep field names aligned with
``healthKit/healthKit/Models/*.swift`` (snake_case dates, HK type strings).
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4


def _uuid() -> str:
    return str(uuid4()).upper()


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _rng(rng: Optional[random.Random]) -> random.Random:
    return rng if rng is not None else random


def _heart_rate(rng: Optional[random.Random] = None) -> float:
    g = _rng(rng)
    return round(g.gauss(72, 12), 1) if g.random() > 0.2 else round(g.uniform(90, 140), 1)


def _step_count(duration_hours: float = 1.0, rng: Optional[random.Random] = None) -> float:
    g = _rng(rng)
    base = g.gauss(5500, 1500) * duration_hours
    return round(max(100, base), 0)


def _workout_duration_min(rng: Optional[random.Random] = None) -> float:
    return round(_rng(rng).triangular(15, 90, 35), 0)


def _calories_from_duration(duration_min: float, intensity: float = 1.0, rng: Optional[random.Random] = None) -> float:
    return round(duration_min * _rng(rng).uniform(8, 12) * intensity, 1)


def _distance_from_duration(duration_min: float, activity: str, rng: Optional[random.Random] = None) -> float:
    g = _rng(rng)
    pace_m_per_min = {"running": 180, "walking": 85, "cycling": 350, "swimming": 50}.get(activity, 100)
    return round(duration_min * pace_m_per_min * g.uniform(0.8, 1.2), 0)


def _sleep_hours(rng: Optional[random.Random] = None) -> float:
    return round(_rng(rng).triangular(5.5, 9.5, 7.5), 2)


def _hk_profiles_for_rng(rng: random.Random) -> List[Tuple[str, str, Callable[[], float]]]:
    """Quantity profiles with value generators closed over ``rng``."""
    g = rng

    return [
        ("HKQuantityTypeIdentifierActiveEnergyBurned", "kcal", lambda: round(g.uniform(3, 45), 2)),
        ("HKQuantityTypeIdentifierAppleExerciseTime", "min", lambda: round(g.uniform(1, 40), 1)),
        ("HKQuantityTypeIdentifierAppleStandTime", "min", lambda: round(g.uniform(1, 12), 1)),
        ("HKQuantityTypeIdentifierDistanceWalkingRunning", "m", lambda: round(g.uniform(200, 4500), 0)),
        ("HKQuantityTypeIdentifierOxygenSaturation", "%", lambda: round(g.uniform(0.94, 0.99), 3)),
        ("HKQuantityTypeIdentifierRestingHeartRate", "count/min", lambda: round(g.gauss(58, 6), 1)),
        ("HKQuantityTypeIdentifierHeartRateVariabilitySDNN", "ms", lambda: round(g.gauss(45, 18), 1)),
        ("HKQuantityTypeIdentifierBodyMass", "kg", lambda: round(g.uniform(58, 92), 1)),
        ("HKQuantityTypeIdentifierFlightsClimbed", "count", lambda: float(g.randint(0, 18))),
        ("HKQuantityTypeIdentifierWalkingSpeed", "km/hr", lambda: round(g.uniform(3.5, 5.8), 2)),
        ("HKQuantityTypeIdentifierStepCount", "count", lambda: _step_count(g.uniform(0.2, 1.2), rng)),
        ("HKQuantityTypeIdentifierHeartRate", "count/min", lambda: _heart_rate(rng)),
    ]


def build_payloads(
    payload_size: str,
    *,
    history_days: Optional[int] = None,
    user_id: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    ``payload_size``: ``standard`` (small) or ``demo`` (more volume for dashboards / DLT).

    ``history_days`` (optional): calendar span for activity summaries, sleep nights, and
    sample timestamps. Defaults to **60** for ``demo`` and **14** for ``standard`` (clamped 1–730).

    ``user_id`` (optional): stable per-user RNG so multi-user seeds are not identical.
    """
    demo = payload_size.strip().lower() == "demo"
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    if history_days is None:
        history_days = 60 if demo else 14
    history_days = max(1, min(int(history_days), 730))

    seed = None
    if user_id and user_id.strip():
        seed = int(hashlib.sha256(user_id.strip().encode("utf-8")).hexdigest()[:12], 16)
    rng = random.Random(seed)

    # Volume scales with history so Lakeview / gold have enough points for trends.
    if demo:
        extra_lines = min(720, max(180, history_days * 12))
        summary_days = history_days
        num_workouts = max(18, history_days // 3)
        num_sleep_sessions = history_days
    else:
        extra_lines = min(120, max(24, history_days * 4))
        summary_days = min(history_days, 28)
        num_workouts = max(3, min(summary_days // 4, 12))
        num_sleep_sessions = min(summary_days, 14)

    samples: List[Dict[str, Any]] = []
    hk_profiles = _hk_profiles_for_rng(rng)

    hr1_time = now - timedelta(hours=rng.uniform(1, 4))
    hr2_time = now - timedelta(hours=rng.uniform(0.1, 1))
    steps_start = now - timedelta(hours=rng.uniform(2, 5))
    steps_duration_hr = rng.uniform(0.5, 2)

    samples.extend(
        [
            {
                "uuid": _uuid(),
                "type": "HKQuantityTypeIdentifierHeartRate",
                "value": _heart_rate(rng),
                "unit": "count/min",
                "start_date": _iso(hr1_time),
                "end_date": _iso(hr1_time),
                "source_name": "Apple Watch",
                "source_bundle_id": "com.apple.health",
                "metadata": {"HKMetadataKeyHeartRateMotionContext": str(rng.choice([0, 1, 2]))},
            },
            {
                "uuid": _uuid(),
                "type": "HKQuantityTypeIdentifierHeartRate",
                "value": _heart_rate(rng),
                "unit": "count/min",
                "start_date": _iso(hr2_time),
                "end_date": _iso(hr2_time),
                "source_name": "Apple Watch",
                "source_bundle_id": "com.apple.health",
                "metadata": None,
            },
            {
                "uuid": _uuid(),
                "type": "HKQuantityTypeIdentifierStepCount",
                "value": _step_count(steps_duration_hr, rng),
                "unit": "count",
                "start_date": _iso(steps_start),
                "end_date": _iso(steps_start + timedelta(hours=steps_duration_hr)),
                "source_name": rng.choice(["iPhone", "Apple Watch"]),
                "source_bundle_id": "com.apple.health",
                "metadata": None,
            },
        ]
    )

    day_span = max(1, history_days)
    for _ in range(extra_lines):
        hk_type, unit, gen = rng.choice(hk_profiles)
        start = now - timedelta(days=rng.randint(0, day_span - 1), hours=rng.uniform(0, 23))
        window_min = rng.choice([0, 2, 5, 15, 30, 60])
        end = start + timedelta(minutes=window_min) if window_min else start
        samples.append(
            {
                "uuid": _uuid(),
                "type": hk_type,
                "value": gen(),
                "unit": unit,
                "start_date": _iso(start),
                "end_date": _iso(end),
                "source_name": rng.choice(["Apple Watch", "iPhone"]),
                "source_bundle_id": "com.apple.health",
                "metadata": None,
            }
        )

    workouts: List[Dict[str, Any]] = []
    for _ in range(num_workouts):
        workout_start = now - timedelta(
            days=rng.randint(0, max(0, day_span - 1)),
            hours=rng.uniform(0, 23),
            minutes=rng.randint(0, 59),
        )
        workout_dur_min = _workout_duration_min(rng)
        workout_activity = rng.choice(["running", "walking", "cycling", "swimming"])
        workouts.append(
            {
                "uuid": _uuid(),
                "activity_type": workout_activity,
                "activity_type_raw": {"running": 37, "walking": 52, "cycling": 13, "swimming": 46}.get(
                    workout_activity, 0
                ),
                "start_date": _iso(workout_start),
                "end_date": _iso(workout_start + timedelta(minutes=workout_dur_min)),
                "duration_seconds": workout_dur_min * 60,
                "total_energy_burned_kcal": _calories_from_duration(workout_dur_min, rng=rng),
                "total_distance_meters": _distance_from_duration(workout_dur_min, workout_activity, rng),
                "source_name": "Apple Watch",
                "metadata": None,
            }
        )

    sleep: List[Dict[str, Any]] = []
    for s in range(num_sleep_sessions):
        sleep_hr = _sleep_hours(rng)
        sleep_end = now.replace(hour=rng.randint(5, 9), minute=rng.randint(0, 55)) - timedelta(days=s)
        sleep_start = sleep_end - timedelta(hours=sleep_hr)
        stage_fracs = {"awake": 0.05, "asleepCore": 0.50, "asleepDeep": 0.20, "asleepREM": 0.25}
        stages = []
        cursor = sleep_start
        for stage_name, frac in stage_fracs.items():
            stage_dur = timedelta(hours=sleep_hr * frac)
            stages.append(
                {
                    "uuid": _uuid(),
                    "stage": stage_name,
                    "start_date": _iso(cursor),
                    "end_date": _iso(cursor + stage_dur),
                }
            )
            cursor += stage_dur
        sleep.append({"start_date": _iso(sleep_start), "end_date": _iso(sleep_end), "stages": stages})

    activity_summaries: List[Dict[str, Any]] = []
    for d in range(summary_days):
        day = (now - timedelta(days=d + 1)).date().isoformat()
        active_cal = round(rng.triangular(300, 900, 520), 1)
        exercise_min = round(rng.triangular(10, 90, 35), 0)
        stand_hr = rng.randint(6, 14)
        activity_summaries.append(
            {
                "date": day,
                "active_energy_burned_kcal": active_cal,
                "active_energy_burned_goal_kcal": rng.choice([400.0, 500.0, 600.0, 750.0]),
                "exercise_minutes": exercise_min,
                "exercise_minutes_goal": 30.0,
                "stand_hours": stand_hr,
                "stand_hours_goal": 12,
            }
        )

    deletes = [
        {
            "uuid": samples[0]["uuid"],
            "sample_type": "HKQuantityTypeIdentifierHeartRate",
        }
    ]

    return {
        "samples": samples,
        "workouts": workouts,
        "sleep": sleep,
        "activity_summaries": activity_summaries,
        "deletes": deletes,
    }


def summarize(payloads: Dict[str, List[Any]]) -> str:
    lines = [f"Generated {sum(len(v) for v in payloads.values())} test records:"]
    for rt, recs in payloads.items():
        lines.append(f"  {rt:20s} {len(recs)} record(s)")
    return "\n".join(lines)
