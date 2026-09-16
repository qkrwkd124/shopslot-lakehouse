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
PAYMENT_INDICES = CHECKED_IN_INDICES
INITIAL_PAYMENT_AMOUNTS = {
    1: BOOKED_PRICE_KRW,
    2: 30_000,  # initial partial payment: unpaid 25,000
    3: BOOKED_PRICE_KRW,
    4: BOOKED_PRICE_KRW,
    5: BOOKED_PRICE_KRW,
    6: BOOKED_PRICE_KRW,
    9: BOOKED_PRICE_KRW,
}
REFUND_SCENARIOS = {
    3: (20_000, False),  # accepted partial refund: no receivable
    4: (20_000, True),  # temporary refund: recollection required
    5: (BOOKED_PRICE_KRW, False),  # accepted full refund
    6: (BOOKED_PRICE_KRW, True),  # full refund followed by partial recollection
}
REPAYMENT_AMOUNTS = {
    4: 20_000,  # fully recollected
    6: 30_000,  # still unpaid 25,000
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


def payment_transaction_id_for(index: int, occurrence: int = 1) -> int:
    return (occurrence - 1) * BOOKING_COUNT + index


def event_id_for(event_type: str, index: int, occurrence: int = 1) -> str:
    suffix = "" if occurrence == 1 else f"-{occurrence}"
    return deterministic_id(f"p0-{event_type}-event-{index}{suffix}")


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


def create_payment_requests(
    connection: pymysql.connections.Connection, base_time: datetime
) -> None:
    """Create the payment aggregate before any money movement."""
    for index in PAYMENT_INDICES:
        booking_id = booking_id_for(index)
        shop_id = shop_id_for(index)
        payment_id = payment_id_for(index)
        effective_start_at = start_at_for(base_time, index)
        if index == RESCHEDULED_INDEX:
            effective_start_at += timedelta(days=1)
        event_time = effective_start_at + timedelta(minutes=45)
        event_id = event_id_for("payment_requested", index)
        payload = {
            **base_event_payload(
                event_id=event_id,
                event_type="payment_requested",
                event_time=event_time,
                booking_id=booking_id,
                shop_id=shop_id,
            ),
            "payment_id": payment_id,
            "request_amount_krw": BOOKED_PRICE_KRW,
            "payout_amount_krw": 0,
            "refund_amount_krw": 0,
            "paid_amount_krw": 0,
            "unpaid_amount_krw": BOOKED_PRICE_KRW,
            "payment_status": "unpaid",
            "needs_repayment": False,
        }

        with transaction(connection) as cursor:
            cursor.execute(
                """
                INSERT INTO payments
                  (id, booking_id, request_amount_krw, payout_amount_krw,
                   refund_amount_krw, paid_amount_krw, unpaid_amount_krw,
                   needs_repayment, payment_status)
                VALUES (%s, %s, %s, 0, 0, 0, %s, 0, 'unpaid')
                """,
                (
                    payment_id,
                    booking_id,
                    BOOKED_PRICE_KRW,
                    BOOKED_PRICE_KRW,
                ),
            )
            insert_outbox_event(
                cursor,
                event_id=event_id,
                event_type="payment_requested",
                booking_id=booking_id,
                shop_id=shop_id,
                payload=payload,
                event_time=event_time,
            )
        print(f"committed event_type=payment_requested payment_id={payment_id}")


def payment_state_for(
    *,
    request_amount: int,
    paid_amount: int,
    refund_amount: int,
    needs_repayment: bool,
) -> tuple[str, int]:
    """Return the business status and actual receivable after a movement."""
    if not 0 <= paid_amount <= request_amount:
        raise RuntimeError(
            f"paid amount must be between zero and request amount: "
            f"paid={paid_amount}, request={request_amount}"
        )

    if needs_repayment or refund_amount == 0:
        if paid_amount == request_amount:
            return "paid", 0
        return "unpaid", request_amount - paid_amount

    if paid_amount == request_amount:
        return "paid", 0
    if paid_amount == 0:
        return "refunded", 0
    return "partially_refunded", 0


def apply_payment_movement(
    connection: pymysql.connections.Connection,
    *,
    index: int,
    transaction_type: str,
    amount_krw: int,
    occurrence: int,
    event_time: datetime,
    needs_repayment: bool = False,
) -> None:
    """Append one movement and update the materialized payment summary atomically."""
    if transaction_type not in {"payment", "refund"}:
        raise ValueError(f"unsupported transaction_type={transaction_type}")
    if amount_krw <= 0:
        raise ValueError("amount_krw must be positive")

    booking_id = booking_id_for(index)
    shop_id = shop_id_for(index)
    payment_id = payment_id_for(index)
    transaction_id = payment_transaction_id_for(index, occurrence)
    event_type = "payment_completed" if transaction_type == "payment" else "payment_refunded"
    event_id = event_id_for(event_type, index, occurrence)

    with transaction(connection) as cursor:
        cursor.execute(
            """
            SELECT request_amount_krw, payout_amount_krw, refund_amount_krw,
                   paid_amount_krw, needs_repayment
            FROM payments
            WHERE id = %s
            FOR UPDATE
            """,
            (payment_id,),
        )
        current = cursor.fetchone()
        if current is None:
            raise RuntimeError(f"payment_id={payment_id} does not exist")
        request_amount, payout_amount, refund_amount, paid_amount = map(int, current[:4])
        current_needs_repayment = bool(current[4])

        if transaction_type == "payment":
            new_payout_amount = payout_amount + amount_krw
            new_refund_amount = refund_amount
            new_paid_amount = paid_amount + amount_krw
            if new_paid_amount > request_amount:
                raise RuntimeError(
                    f"payment_id={payment_id} cannot collect more than its request: "
                    f"paid={paid_amount}, amount={amount_krw}, request={request_amount}"
                )
            new_needs_repayment = current_needs_repayment and (
                new_paid_amount < request_amount
            )
        else:
            if amount_krw > paid_amount:
                raise RuntimeError(
                    f"payment_id={payment_id} cannot refund more than its current "
                    f"paid amount: amount={amount_krw}, paid={paid_amount}"
                )
            new_payout_amount = payout_amount
            new_paid_amount = paid_amount - amount_krw
            new_refund_amount = refund_amount + amount_krw
            new_needs_repayment = current_needs_repayment or needs_repayment

        payment_status, unpaid_amount = payment_state_for(
            request_amount=request_amount,
            paid_amount=new_paid_amount,
            refund_amount=new_refund_amount,
            needs_repayment=new_needs_repayment,
        )
        if new_payout_amount != new_paid_amount + new_refund_amount:
            raise RuntimeError("net paid amount does not match payment movements")

        if transaction_type == "payment":
            cursor.execute(
                """
                UPDATE payments
                SET payout_amount_krw = %s,
                    refund_amount_krw = %s,
                    paid_amount_krw = %s,
                    unpaid_amount_krw = %s,
                    needs_repayment = %s,
                    payment_status = %s,
                    paid_at = %s
                WHERE id = %s
                """,
                (
                    new_payout_amount,
                    new_refund_amount,
                    new_paid_amount,
                    unpaid_amount,
                    new_needs_repayment,
                    payment_status,
                    mysql_datetime(event_time),
                    payment_id,
                ),
            )
        else:
            cursor.execute(
                """
                UPDATE payments
                SET payout_amount_krw = %s,
                    refund_amount_krw = %s,
                    paid_amount_krw = %s,
                    unpaid_amount_krw = %s,
                    needs_repayment = %s,
                    payment_status = %s,
                    refunded_at = %s
                WHERE id = %s
                """,
                (
                    new_payout_amount,
                    new_refund_amount,
                    new_paid_amount,
                    unpaid_amount,
                    new_needs_repayment,
                    payment_status,
                    mysql_datetime(event_time),
                    payment_id,
                ),
            )
        require_one_updated(cursor, operation=event_type)

        cursor.execute(
            """
            INSERT INTO payment_transactions
              (id, payment_id, transaction_type, amount_krw, occurred_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                transaction_id,
                payment_id,
                transaction_type,
                amount_krw,
                mysql_datetime(event_time),
            ),
        )

        payload = {
            **base_event_payload(
                event_id=event_id,
                event_type=event_type,
                event_time=event_time,
                booking_id=booking_id,
                shop_id=shop_id,
            ),
            "payment_id": payment_id,
            "payment_transaction_id": transaction_id,
            "request_amount_krw": request_amount,
            "amount_krw": amount_krw,
            "payout_amount_krw": new_payout_amount,
            "refund_amount_krw": new_refund_amount,
            "paid_amount_krw": new_paid_amount,
            "unpaid_amount_krw": unpaid_amount,
            "payment_status": payment_status,
            "needs_repayment": new_needs_repayment,
        }
        if transaction_type == "refund":
            payload["refund_type"] = "full" if new_paid_amount == 0 else "partial"
        insert_outbox_event(
            cursor,
            event_id=event_id,
            event_type=event_type,
            booking_id=booking_id,
            shop_id=shop_id,
            payload=payload,
            event_time=event_time,
        )
    print(
        f"committed event_type={event_type} payment_id={payment_id} "
        f"payout={new_payout_amount} refund={new_refund_amount} "
        f"paid={new_paid_amount} unpaid={unpaid_amount} status={payment_status}"
    )


def apply_initial_payments(
    connection: pymysql.connections.Connection, base_time: datetime
) -> None:
    for index, amount_krw in INITIAL_PAYMENT_AMOUNTS.items():
        effective_start_at = start_at_for(base_time, index)
        if index == RESCHEDULED_INDEX:
            effective_start_at += timedelta(days=1)
        apply_payment_movement(
            connection,
            index=index,
            transaction_type="payment",
            amount_krw=amount_krw,
            occurrence=1,
            event_time=effective_start_at + timedelta(minutes=60),
        )


def refund_payments(connection: pymysql.connections.Connection, base_time: datetime) -> None:
    for index, (refund_amount, requires_repayment) in REFUND_SCENARIOS.items():
        apply_payment_movement(
            connection,
            index=index,
            transaction_type="refund",
            amount_krw=refund_amount,
            occurrence=2,
            event_time=start_at_for(base_time, index) + timedelta(days=1, hours=2),
            needs_repayment=requires_repayment,
        )


def apply_recollection_scenarios(
    connection: pymysql.connections.Connection, base_time: datetime
) -> None:
    for index, amount_krw in REPAYMENT_AMOUNTS.items():
        first_refund_time = start_at_for(base_time, index) + timedelta(days=1, hours=2)
        apply_payment_movement(
            connection,
            index=index,
            transaction_type="payment",
            amount_krw=amount_krw,
            occurrence=3,
            event_time=first_refund_time + timedelta(hours=1),
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
        seed_reference_data(connection)
        create_bookings(connection, base_time)
        apply_booking_lifecycle(connection, base_time)
        create_payment_requests(connection, base_time)
        apply_initial_payments(connection, base_time)
        refund_payments(connection, base_time)
        apply_recollection_scenarios(connection, base_time)
    finally:
        connection.close()

    print(
        "P0 dataset generated: "
        "10 bookings, 7 payments, 13 payment transactions, 40 outbox events"
    )


if __name__ == "__main__":
    main()
