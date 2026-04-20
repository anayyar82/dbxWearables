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


def build_seed_rows(user_id: str, payload_size: str) -> List[Dict[str, Any]]:
    mod = _load_sample_payload_module()
    payloads: Dict[str, List[Dict[str, Any]]] = mod.build_payloads(payload_size)
    rows: List[Dict[str, Any]] = []
    for rt, recs in payloads.items():
        for body in recs:
            rows.append(bronze_row_dict(record_type=rt, body=body, user_id=user_id))
    return rows
