"""
Helpers to build rows for ``wearables_zerobus`` (bronze) demo seeding.

Reuses ``sample_payload_generation.build_payloads`` when importable. Falls back
to a minimal inline generator if the notebook runs outside the repo layout.
"""

from __future__ import annotations

import importlib.util
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _load_sample_payload_module():
    try:
        import sample_payload_generation as m  # type: ignore

        return m
    except ImportError:
        pass
    here = Path(__file__).resolve().parent
    candidates = [
        here.parent / "endpoint-validation" / "sample_payload_generation.py",
        here / "sample_payload_generation.py",
    ]
    for py in candidates:
        if not py.is_file():
            continue
        spec = importlib.util.spec_from_file_location("sample_payload_generation", str(py))
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[attr-defined]
        return mod
    raise ImportError(
        "Could not import sample_payload_generation. "
        "Place this file under src/notebooks next to src/endpoint-validation in the repo."
    )


def synthetic_headers(record_type: str) -> Dict[str, str]:
    return {
        "content-type": "application/x-ndjson",
        "x-record-type": record_type,
        "x-platform": "apple_healthkit",
        "x-device-id": "demo-notebook-seed",
        "x-ingest-channel": "notebook_simulator",
        "x-app-version": "1.0.0-demo",
    }


def bronze_row_dict(
    *,
    record_type: str,
    body: Dict[str, Any],
    user_id: str,
    ingested_at: datetime | None = None,
    record_id: str | None = None,
) -> Dict[str, Any]:
    ts = ingested_at or datetime.now(timezone.utc)
    rid = record_id or str(uuid.uuid4())
    return {
        "record_id": rid,
        "ingested_at": ts,
        "body_json": json.dumps(body, separators=(",", ":")),
        "headers_json": json.dumps(synthetic_headers(record_type)),
        "record_type": record_type,
        "source_platform": "apple_healthkit",
        "user_id": user_id,
    }


def build_seed_rows(
    user_id: str,
    payload_size: str,
    *,
    history_days: int | None = None,
) -> List[Dict[str, Any]]:
    mod = _load_sample_payload_module()
    payloads: Dict[str, List[Dict[str, Any]]] = mod.build_payloads(
        payload_size,
        history_days=history_days,
        user_id=user_id,
    )
    rows: List[Dict[str, Any]] = []
    for rt, recs in payloads.items():
        for body in recs:
            rows.append(bronze_row_dict(record_type=rt, body=body, user_id=user_id))
    return rows


# Stable synthetic identities so Lakeview / DLT show multiple subjects (not real PII).
DEMO_COHORT_USER_IDS: List[str] = [
    "demo.wearables+aurora@dbx.demo",
    "demo.wearables+blake@dbx.demo",
    "demo.wearables+chen@dbx.demo",
    "demo.wearables+dana@dbx.demo",
    "demo.wearables+eli@dbx.demo",
    "demo.wearables+noa@dbx.demo",
]


def seed_target_user_ids(primary_user_id: str, multi_user_mode: str) -> List[str]:
    """
    Resolve which ``user_id`` values receive demo rows for this notebook run.

    ``multi_user_mode`` (widget / job parameter):
      - ``single``: only ``primary_user_id``
      - ``demo_cohort_only``: the six ``DEMO_COHORT_USER_IDS`` only
      - ``primary_plus_demo``: primary plus cohort (deduped, up to 7 users)
    """
    mode = (multi_user_mode or "single").strip().lower().replace("-", "_")
    primary = (primary_user_id or "").strip()
    cohort = list(DEMO_COHORT_USER_IDS)

    if mode in ("single", ""):
        return [primary] if primary else cohort[:1]

    if mode == "demo_cohort_only":
        return cohort

    if mode in ("primary_plus_demo", "primary_and_cohort", "multi"):
        out: List[str] = []
        for u in [primary] + cohort:
            u = u.strip()
            if u and u not in out:
                out.append(u)
        return out

    return [primary] if primary else cohort[:1]


def sql_in_list(user_ids: List[str]) -> str:
    """Comma-separated quoted literals for SQL ``IN (...)``."""
    parts: List[str] = []
    for u in user_ids:
        parts.append("'" + str(u).replace("'", "''") + "'")
    return ", ".join(parts)
