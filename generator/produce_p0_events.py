"""Generate a deterministic P0 booking, payment, and refund lifecycle."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pymysql


SEED_NAMESPACE = uuid.UUID("7a6f8a42-b6d8-4dd7-91e5-3aa5e65f8e51")
KST = ZoneInfo("Asia/Seoul")
BOOKED_PRICE_KRW = 55_000
BOOKING_COUNT = 10

RESCHEDULED_INDEX = 9
CANCELLED_INDEX = 7
NO_SHOW_INDEX = 8
CHECKED_IN_INDICES = (1, 2, 3, 4, 5, 6, 9)
PAID_INDICES = CHECKED_IN_INDICES
REFUND_AMOUNTS = {
    3: 20_000,  # partial refund
    6: BOOKED_PRICE_KRW,  # full refund
}


def deterministic_id(label: str) -> str:
    return str(uuid.uuid5(SEED_NAMESPACE, label))


def shop_id_for(index: int) -> int:
    return (index - 1) % 3 + 1


def service_id_for(shop_id: int) -> int:
    return shop_id


def staff_id_for(shop_id: int, booking_index: int) -> int:
    return (shop_id - 1) * 2 + (booking_index - 1) % 2 + 1


def booking_id_for(index: int) -> int:
    return index


def payment_id_for(index: int) -> int:
    return index


def payment_transaction_id_for(index: int) -> int:
    return index


def refund_transaction_id_for(index: int) -> int:
    return BOOKING_COUNT + index


def event_id_for(event_type: str, index: int) -> str:
    return deterministic_id(f"p0-{event_type}-event-{index}")


def created_at_for(base_time: datetime, index: int) -> datetime:
    return base_time + timedelta(minutes=index)


def start_at_for(base_time: datetime, index: int) -> datetime:
    day_offset = 1 + (index - 1) // 8
    hour = 9 + (index - 1) % 8
    return (base_time + timedelta(days=day_offset)).replace(
        hour=hour,
        minute=0,
        second=0,
        microsecond=0,
    )


def mysql_datetime(value: datetime) -> datetime:
    """Store the fixed Asia/Seoul wall clock in MySQL's timezone-naive DATETIME."""
    return value.replace(tzinfo=None)


def base_event_payload(
    *,
    event_id: str,
    event_type: str,
    event_time: datetime,
    booking_id: int,
    shop_id: int,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": event_time.isoformat(),
        "ingest_time": event_time.isoformat(),
        "schema_version": 1,
        "booking_id": booking_id,
        "shop_id": shop_id,
    }


@contextmanager
def transaction(connection: pymysql.connections.Connection) -> Iterator[pymysql.cursors.Cursor]:
    """Commit all writes in the block together and roll back the whole block on error."""
    try:
        with connection.cursor() as cursor:
            yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def insert_outbox_event(
    cursor: pymysql.cursors.Cursor,
    *,
    event_id: str,
    event_type: str,
    booking_id: int,
    shop_id: int,
    payload: dict[str, Any],
    event_time: datetime,
) -> None:
    cursor.execute(
        """
        INSERT INTO outbox_events
          (event_id, aggregate_type, aggregate_id, event_type, partition_key,
           payload, schema_version, event_time)
        VALUES (%s, 'booking', %s, %s, %s, %s, 1, %s)
        """,
        (
            event_id,
            booking_id,
            event_type,
            shop_id,
            json.dumps(payload),
            mysql_datetime(event_time),
        ),
    )


def require_one_updated(cursor: pymysql.cursors.Cursor, *, operation: str) -> None:
    if cursor.rowcount != 1:
        raise RuntimeError(f"{operation} expected to update one row, updated {cursor.rowcount}")


def seed_reference_data(connection: pymysql.connections.Connection) -> None:
    """Insert P0's non-PII catalog once; business events are committed separately."""
    with transaction(connection) as cursor:
        for index in range(1, 4):
            shop_id = index
            cursor.execute(
                """
                INSERT INTO shops (id, shop_name, region)
                VALUES (%s, %s, 'seoul')
                ON DUPLICATE KEY UPDATE shop_name = VALUES(shop_name)
                """,
                (shop_id, f"ShopSlot {index:03d}"),
            )
            cursor.execute(
                """
                INSERT INTO services
                  (id, shop_id, service_name, list_price_krw, duration_minutes)
                VALUES (%s, %s, 'standard_service', %s, 60)
                ON DUPLICATE KEY UPDATE
                  service_name = VALUES(service_name),
                  list_price_krw = VALUES(list_price_krw),
                  duration_minutes = VALUES(duration_minutes)
                """,
                (service_id_for(shop_id), shop_id, BOOKED_PRICE_KRW),
            )
            for staff_offset in range(2):
                staff_id = (shop_id - 1) * 2 + staff_offset + 1
                cursor.execute(
                    """
                    INSERT INTO staffs (id, shop_id, staff_name)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE staff_name = VALUES(staff_name)
                    """,
                    (staff_id, shop_id, f"Staff {staff_id:03d}"),
                )

        for index in range(1, BOOKING_COUNT + 1):
            cursor.execute(
                """
                INSERT INTO customers (id, segment)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE segment = VALUES(segment)
                """,
                (index, "returning" if index % 2 else "new"),
            )


def create_bookings(connection: pymysql.connections.Connection, base_time: datetime) -> None:
    for index in range(1, BOOKING_COUNT + 1):
        booking_id = booking_id_for(index)
        shop_id = shop_id_for(index)
        event_time = created_at_for(base_time, index)
        start_at = start_at_for(base_time, index)
        event_id = event_id_for("booking_created", index)
        payload = {
            **base_event_payload(
                event_id=event_id,
                event_type="booking_created",
                event_time=event_time,
                booking_id=booking_id,
                shop_id=shop_id,
            ),
            "staff_id": staff_id_for(shop_id, index),
            "customer_id": index,
            "service_id": service_id_for(shop_id),
            "start_at": start_at.isoformat(),
            "booked_price_krw": BOOKED_PRICE_KRW,
            "status": "scheduled",
        }

        with transaction(connection) as cursor:
            cursor.execute(
                """
                INSERT INTO bookings
                  (id, shop_id, customer_id, service_id, staff_id, start_at,
                   status, booked_price_krw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    booking_id,
                    shop_id,
                    payload["customer_id"],
                    payload["service_id"],
                    payload["staff_id"],
                    mysql_datetime(start_at),
                    payload["status"],
                    payload["booked_price_krw"],
                ),
            )
            insert_outbox_event(
                cursor,
                event_id=event_id,
                event_type="booking_created",
                booking_id=booking_id,
                shop_id=shop_id,
                payload=payload,
                event_time=event_time,
            )
        print(f"committed event_type=booking_created booking_id={booking_id}")


def reschedule_booking(connection: pymysql.connections.Connection, base_time: datetime) -> None:
    index = RESCHEDULED_INDEX
    booking_id = booking_id_for(index)
    shop_id = shop_id_for(index)
    old_start_at = start_at_for(base_time, index)
    new_start_at = old_start_at + timedelta(days=1)
    event_time = created_at_for(base_time, index) + timedelta(hours=2)
    event_id = event_id_for("booking_rescheduled", index)
    payload = {
        **base_event_payload(
            event_id=event_id,
            event_type="booking_rescheduled",
            event_time=event_time,
            booking_id=booking_id,
            shop_id=shop_id,
        ),
        "old_start_at": old_start_at.isoformat(),
        "start_at": new_start_at.isoformat(),
        "status": "rescheduled",
    }

    with transaction(connection) as cursor:
        cursor.execute(
            """
            UPDATE bookings
            SET start_at = %s, status = 'rescheduled'
            WHERE id = %s AND status = 'scheduled'
            """,
            (mysql_datetime(new_start_at), booking_id),
        )
        require_one_updated(cursor, operation="booking_rescheduled")
        insert_outbox_event(
            cursor,
            event_id=event_id,
            event_type="booking_rescheduled",
            booking_id=booking_id,
            shop_id=shop_id,
            payload=payload,
            event_time=event_time,
        )
    print(f"committed event_type=booking_rescheduled booking_id={booking_id}")


def transition_booking_status(
    connection: pymysql.connections.Connection,
    *,
    index: int,
    event_type: str,
    target_status: str,
    event_time: datetime,
    expected_statuses: tuple[str, ...],
) -> None:
    booking_id = booking_id_for(index)
    shop_id = shop_id_for(index)
    event_id = event_id_for(event_type, index)
    placeholders = ", ".join(["%s"] * len(expected_statuses))
    payload = {
        **base_event_payload(
            event_id=event_id,
            event_type=event_type,
            event_time=event_time,
            booking_id=booking_id,
            shop_id=shop_id,
        ),
        "status": target_status,
    }

    with transaction(connection) as cursor:
        if target_status == "cancelled":
            cursor.execute(
                f"""
                UPDATE bookings
                SET status = %s, cancelled_at = %s
                WHERE id = %s AND status IN ({placeholders})
                """,
                (target_status, mysql_datetime(event_time), booking_id, *expected_statuses),
            )
            payload["cancelled_at"] = event_time.isoformat()
        else:
            cursor.execute(
                f"""
                UPDATE bookings
                SET status = %s
                WHERE id = %s AND status IN ({placeholders})
                """,
                (target_status, booking_id, *expected_statuses),
            )
        require_one_updated(cursor, operation=event_type)
        insert_outbox_event(
            cursor,
            event_id=event_id,
            event_type=event_type,
            booking_id=booking_id,
            shop_id=shop_id,
            payload=payload,
            event_time=event_time,
        )
    print(f"committed event_type={event_type} booking_id={booking_id}")


def apply_booking_lifecycle(
    connection: pymysql.connections.Connection,
    base_time: datetime,
) -> None:
    reschedule_booking(connection, base_time)

    cancel_time = created_at_for(base_time, CANCELLED_INDEX) + timedelta(hours=3)
    transition_booking_status(
        connection,
        index=CANCELLED_INDEX,
        event_type="booking_cancelled",
        target_status="cancelled",
        event_time=cancel_time,
        expected_statuses=("scheduled",),
    )

    no_show_time = start_at_for(base_time, NO_SHOW_INDEX) + timedelta(minutes=15)
    transition_booking_status(
        connection,
        index=NO_SHOW_INDEX,
        event_type="no_show_marked",
        target_status="no_show",
        event_time=no_show_time,
        expected_statuses=("scheduled",),
    )

    for index in CHECKED_IN_INDICES:
        effective_start_at = start_at_for(base_time, index)
        if index == RESCHEDULED_INDEX:
            effective_start_at += timedelta(days=1)
        transition_booking_status(
            connection,
            index=index,
            event_type="checked_in",
            target_status="checked_in",
            event_time=effective_start_at - timedelta(minutes=5),
            expected_statuses=("scheduled", "rescheduled"),
        )


def complete_payments(connection: pymysql.connections.Connection, base_time: datetime) -> None:
    for index in PAID_INDICES:
        booking_id = booking_id_for(index)
        shop_id = shop_id_for(index)
        payment_id = payment_id_for(index)
        transaction_id = payment_transaction_id_for(index)
        effective_start_at = start_at_for(base_time, index)
        if index == RESCHEDULED_INDEX:
            effective_start_at += timedelta(days=1)
        event_time = effective_start_at + timedelta(minutes=60)
        event_id = event_id_for("payment_completed", index)
        payload = {
            **base_event_payload(
                event_id=event_id,
                event_type="payment_completed",
                event_time=event_time,
                booking_id=booking_id,
                shop_id=shop_id,
            ),
            "payment_id": payment_id,
            "payment_transaction_id": transaction_id,
            "amount_krw": BOOKED_PRICE_KRW,
            "payment_status": "paid",
        }

        with transaction(connection) as cursor:
            cursor.execute(
                """
                INSERT INTO payments
                  (id, booking_id, charged_amount_krw, paid_amount_krw,
                   refunded_amount_krw, payment_status, paid_at)
                VALUES (%s, %s, %s, %s, 0, 'paid', %s)
                """,
                (
                    payment_id,
                    booking_id,
                    BOOKED_PRICE_KRW,
                    BOOKED_PRICE_KRW,
                    mysql_datetime(event_time),
                ),
            )
            cursor.execute(
                """
                INSERT INTO payment_transactions
                  (id, payment_id, transaction_type, amount_krw, occurred_at)
                VALUES (%s, %s, 'payment', %s, %s)
                """,
                (transaction_id, payment_id, BOOKED_PRICE_KRW, mysql_datetime(event_time)),
            )
            insert_outbox_event(
                cursor,
                event_id=event_id,
                event_type="payment_completed",
                booking_id=booking_id,
                shop_id=shop_id,
                payload=payload,
                event_time=event_time,
            )
        print(f"committed event_type=payment_completed payment_id={payment_id}")


def refund_payments(connection: pymysql.connections.Connection, base_time: datetime) -> None:
    for index, refund_amount in REFUND_AMOUNTS.items():
        booking_id = booking_id_for(index)
        shop_id = shop_id_for(index)
        payment_id = payment_id_for(index)
        transaction_id = refund_transaction_id_for(index)
        event_time = start_at_for(base_time, index) + timedelta(days=1, hours=2)
        payment_status = "refunded" if refund_amount == BOOKED_PRICE_KRW else "partially_refunded"
        event_id = event_id_for("payment_refunded", index)
        payload = {
            **base_event_payload(
                event_id=event_id,
                event_type="payment_refunded",
                event_time=event_time,
                booking_id=booking_id,
                shop_id=shop_id,
            ),
            "payment_id": payment_id,
            "payment_transaction_id": transaction_id,
            "amount_krw": refund_amount,
            "payment_status": payment_status,
            "refund_type": "full" if refund_amount == BOOKED_PRICE_KRW else "partial",
        }

        with transaction(connection) as cursor:
            cursor.execute(
                """
                UPDATE payments
                SET refunded_amount_krw = refunded_amount_krw + %s,
                    payment_status = %s,
                    refunded_at = %s
                WHERE id = %s
                  AND refunded_amount_krw + %s <= paid_amount_krw
                """,
                (
                    refund_amount,
                    payment_status,
                    mysql_datetime(event_time),
                    payment_id,
                    refund_amount,
                ),
            )
            require_one_updated(cursor, operation="payment_refunded")
            cursor.execute(
                """
                INSERT INTO payment_transactions
                  (id, payment_id, transaction_type, amount_krw, occurred_at)
                VALUES (%s, %s, 'refund', %s, %s)
                """,
                (transaction_id, payment_id, refund_amount, mysql_datetime(event_time)),
            )
            insert_outbox_event(
                cursor,
                event_id=event_id,
                event_type="payment_refunded",
                booking_id=booking_id,
                shop_id=shop_id,
                payload=payload,
                event_time=event_time,
            )
        print(f"committed event_type=payment_refunded payment_id={payment_id}")


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
        seed_reference_data(connection)
        create_bookings(connection, base_time)
        apply_booking_lifecycle(connection, base_time)
        complete_payments(connection, base_time)
        refund_payments(connection, base_time)
    finally:
        connection.close()

    print(
        "P0 dataset generated: "
        "10 bookings, 7 payments, 9 payment transactions, 29 outbox events"
    )


if __name__ == "__main__":
    main()
