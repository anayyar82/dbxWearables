"""
Apple HealthKit–shaped synthetic payloads for ZeroBus ingest demos.

Used by ``validate-zerobus-ingest`` notebook. Keep field names aligned with
``healthKit/healthKit/Models/*.swift`` (snake_case dates, HK type strings).
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Tuple
from uuid import uuid4


def _uuid() -> str:
    return str(uuid4()).upper()


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _heart_rate() -> float:
    return round(random.gauss(72, 12), 1) if random.random() > 0.2 else round(random.uniform(90, 140), 1)


def _step_count(duration_hours: float = 1.0) -> float:
    base = random.gauss(5500, 1500) * duration_hours
    return round(max(100, base), 0)


def _workout_duration_min() -> float:
    return round(random.triangular(15, 90, 35), 0)


def _calories_from_duration(duration_min: float, intensity: float = 1.0) -> float:
    return round(duration_min * random.uniform(8, 12) * intensity, 1)


def _distance_from_duration(duration_min: float, activity: str) -> float:
    pace_m_per_min = {"running": 180, "walking": 85, "cycling": 350, "swimming": 50}.get(activity, 100)
    return round(duration_min * pace_m_per_min * random.uniform(0.8, 1.2), 0)


def _sleep_hours() -> float:
    return round(random.triangular(5.5, 9.5, 7.5), 2)


# (HKQuantityTypeIdentifier…, unit, value generator)
_HK_QUANTITY_PROFILES: List[Tuple[str, str, Callable[[], float]]] = [
    ("HKQuantityTypeIdentifierActiveEnergyBurned", "kcal", lambda: round(random.uniform(3, 45), 2)),
    ("HKQuantityTypeIdentifierAppleExerciseTime", "min", lambda: round(random.uniform(1, 40), 1)),
    ("HKQuantityTypeIdentifierAppleStandTime", "min", lambda: round(random.uniform(1, 12), 1)),
    ("HKQuantityTypeIdentifierDistanceWalkingRunning", "m", lambda: round(random.uniform(200, 4500), 0)),
    ("HKQuantityTypeIdentifierOxygenSaturation", "%", lambda: round(random.uniform(0.94, 0.99), 3)),
    ("HKQuantityTypeIdentifierRestingHeartRate", "count/min", lambda: round(random.gauss(58, 6), 1)),
    ("HKQuantityTypeIdentifierHeartRateVariabilitySDNN", "ms", lambda: round(random.gauss(45, 18), 1)),
    ("HKQuantityTypeIdentifierBodyMass", "kg", lambda: round(random.uniform(58, 92), 1)),
    ("HKQuantityTypeIdentifierFlightsClimbed", "count", lambda: float(random.randint(0, 18))),
    ("HKQuantityTypeIdentifierWalkingSpeed", "km/hr", lambda: round(random.uniform(3.5, 5.8), 2)),
    ("HKQuantityTypeIdentifierStepCount", "count", lambda: _step_count(random.uniform(0.2, 1.2))),
    ("HKQuantityTypeIdentifierHeartRate", "count/min", _heart_rate),
]


def build_payloads(payload_size: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    ``payload_size``: ``standard`` (small) or ``demo`` (more volume for dashboards / DLT).
    """
    demo = payload_size.strip().lower() == "demo"
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    extra_lines = 72 if demo else 18
    summary_days = 21 if demo else 4
    num_workouts = 4 if demo else 1
    num_sleep_sessions = 3 if demo else 1

    samples: List[Dict[str, Any]] = []

    hr1_time = now - timedelta(hours=random.uniform(1, 4))
    hr2_time = now - timedelta(hours=random.uniform(0.1, 1))
    steps_start = now - timedelta(hours=random.uniform(2, 5))
    steps_duration_hr = random.uniform(0.5, 2)

    samples.extend(
        [
            {
                "uuid": _uuid(),
                "type": "HKQuantityTypeIdentifierHeartRate",
                "value": _heart_rate(),
                "unit": "count/min",
                "start_date": _iso(hr1_time),
                "end_date": _iso(hr1_time),
                "source_name": "Apple Watch",
                "source_bundle_id": "com.apple.health",
                "metadata": {"HKMetadataKeyHeartRateMotionContext": str(random.choice([0, 1, 2]))},
            },
            {
                "uuid": _uuid(),
                "type": "HKQuantityTypeIdentifierHeartRate",
                "value": _heart_rate(),
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
                "value": _step_count(steps_duration_hr),
                "unit": "count",
                "start_date": _iso(steps_start),
                "end_date": _iso(steps_start + timedelta(hours=steps_duration_hr)),
                "source_name": random.choice(["iPhone", "Apple Watch"]),
                "source_bundle_id": "com.apple.health",
                "metadata": None,
            },
        ]
    )

    for _ in range(extra_lines):
        hk_type, unit, gen = random.choice(_HK_QUANTITY_PROFILES)
        start = now - timedelta(days=random.randint(0, 14), hours=random.uniform(0, 23))
        window_min = random.choice([0, 2, 5, 15, 30, 60])
        end = start + timedelta(minutes=window_min) if window_min else start
        samples.append(
            {
                "uuid": _uuid(),
                "type": hk_type,
                "value": gen(),
                "unit": unit,
                "start_date": _iso(start),
                "end_date": _iso(end),
                "source_name": random.choice(["Apple Watch", "iPhone"]),
                "source_bundle_id": "com.apple.health",
                "metadata": None,
            }
        )

    workouts: List[Dict[str, Any]] = []
    for _ in range(num_workouts):
        workout_start = now - timedelta(hours=random.uniform(2, 72))
        workout_dur_min = _workout_duration_min()
        workout_activity = random.choice(["running", "walking", "cycling", "swimming"])
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
                "total_energy_burned_kcal": _calories_from_duration(workout_dur_min),
                "total_distance_meters": _distance_from_duration(workout_dur_min, workout_activity),
                "source_name": "Apple Watch",
                "metadata": None,
            }
        )

    sleep: List[Dict[str, Any]] = []
    for s in range(num_sleep_sessions):
        sleep_hr = _sleep_hours()
        sleep_end = now.replace(hour=random.randint(5, 9), minute=random.randint(0, 55)) - timedelta(days=s)
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
        active_cal = round(random.triangular(300, 900, 520), 1)
        exercise_min = round(random.triangular(10, 90, 35), 0)
        stand_hr = random.randint(6, 14)
        activity_summaries.append(
            {
                "date": day,
                "active_energy_burned_kcal": active_cal,
                "active_energy_burned_goal_kcal": random.choice([400.0, 500.0, 600.0, 750.0]),
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
