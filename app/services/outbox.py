from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import OutboxEvent


def record_outbox_event(
    session: Session,
    *,
    event_id: str,
    aggregate_id: str,
    event_type: str,
    shop_id: str,
    payload: dict[str, Any],
    event_time: datetime,
    schema_version: int = 1,
) -> OutboxEvent:
    """Stage an event in the caller's transaction; this function never commits."""
    event = OutboxEvent(
        event_id=event_id,
        aggregate_type="booking",
        aggregate_id=aggregate_id,
        event_type=event_type,
        partition_key=shop_id,
        payload=payload,
        event_time=event_time,
        schema_version=schema_version,
    )
    session.add(event)
    return event
