"""Write ten deterministic booking events with transactional outbox records."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pymysql


SEED_NAMESPACE = uuid.UUID("7a6f8a42-b6d8-4dd7-91e5-3aa5e65f8e51")
KST = ZoneInfo("Asia/Seoul")
BOOKED_PRICE_KRW = 55_000


def deterministic_id(label: str) -> str:
    return str(uuid.uuid5(SEED_NAMESPACE, label))


def shop_id_for(index: int) -> str:
    return f"shop_{index % 3 + 1:03d}"


def service_id_for(shop_id: str) -> str:
    return deterministic_id(f"p0-service-{shop_id}")


def event_payload(index: int, booking_id: str, event_id: str, occurred_at: datetime) -> dict[str, object]:
    shop_id = shop_id_for(index)
    return {
        "event_id": event_id,
        "event_type": "booking_created",
        "event_time": occurred_at.isoformat(),
        "ingest_time": occurred_at.isoformat(),
        "schema_version": 1,
        "booking_id": booking_id,
        "shop_id": shop_id,
        "staff_id": f"staff_{index % 5 + 1:03d}",
        "customer_id": f"customer_{index:03d}",
        "service_id": service_id_for(shop_id),
        "start_at": (occurred_at + timedelta(days=1)).replace(minute=0, second=0, microsecond=0).isoformat(),
        "booked_price_krw": BOOKED_PRICE_KRW,
        "status": "scheduled",
    }


def seed_reference_data(cursor: pymysql.cursors.Cursor) -> None:
    """Insert P0's non-PII catalog once; business events start below."""
    for index in range(1, 4):
        shop_id = f"shop_{index:03d}"
        cursor.execute(
            """
            INSERT INTO shops (shop_id, shop_name, region)
            VALUES (%s, %s, 'seoul')
            ON DUPLICATE KEY UPDATE shop_name = VALUES(shop_name)
            """,
            (shop_id, f"ShopSlot {index:03d}"),
        )
        cursor.execute(
            """
            INSERT INTO services
              (service_id, shop_id, service_name, list_price_krw, duration_minutes)
            VALUES (%s, %s, 'standard_service', %s, 60)
            ON DUPLICATE KEY UPDATE
              service_name = VALUES(service_name),
              list_price_krw = VALUES(list_price_krw),
              duration_minutes = VALUES(duration_minutes)
            """,
            (service_id_for(shop_id), shop_id, BOOKED_PRICE_KRW),
        )

    for index in range(1, 11):
        cursor.execute(
            """
            INSERT INTO customers (customer_id, segment)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE segment = VALUES(segment)
            """,
            (f"customer_{index:03d}", "returning" if index % 2 else "new"),
        )


def main() -> None:
    connection = pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "shopslot"),
        password=os.environ.get("MYSQL_PASSWORD", "shopslot"),
        database=os.environ.get("MYSQL_DATABASE", "shopslot"),
        charset="utf8mb4",
        autocommit=False,
    )
    base_time = datetime(2026, 9, 4, 9, 0, tzinfo=KST)

    try:
        with connection.cursor() as cursor:
            try:
                seed_reference_data(cursor)
                connection.commit()
            except Exception:
                connection.rollback()
                raise

            for index in range(1, 11):
                booking_id = deterministic_id(f"p0-booking-{index}")
                event_id = deterministic_id(f"p0-event-{index}")
                occurred_at = base_time + timedelta(minutes=index)
                payload = event_payload(index, booking_id, event_id, occurred_at)
                try:
                    cursor.execute(
                        """
                        INSERT INTO bookings
                          (booking_id, shop_id, customer_id, service_id, staff_id, start_at,
                           status, booked_price_krw)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            booking_id,
                            payload["shop_id"],
                            payload["customer_id"],
                            payload["service_id"],
                            payload["staff_id"],
                            datetime.fromisoformat(str(payload["start_at"])).replace(tzinfo=None),
                            payload["status"],
                            payload["booked_price_krw"],
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO outbox_events
                          (event_id, aggregate_type, aggregate_id, event_type, partition_key,
                           payload, schema_version, event_time)
                        VALUES (%s, 'booking', %s, 'booking_created', %s, %s, 1, %s)
                        """,
                        (
                            event_id,
                            booking_id,
                            payload["shop_id"],
                            json.dumps(payload),
                            occurred_at.replace(tzinfo=None),
                        ),
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                print(f"committed event_id={event_id} booking_id={booking_id}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
