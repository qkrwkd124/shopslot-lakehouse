"""Publish deterministic invalid booking and payment events for the DQ walkthrough."""

import os
from datetime import datetime

import pymysql

from produce_p0_events import (
    KST,
    base_event_payload,
    deterministic_id,
    insert_outbox_event,
    transaction,
)

BOOKING_EVENT_ID = deterministic_id("dq-invalid-booking-price")
PAYMENT_EVENT_ID = deterministic_id("dq-invalid-payment-amount")
PAYMENT_TRANSACTION_ID = 9_000_000_001


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
    try:
        inserted = []
        with transaction(connection) as cursor:
            cursor.execute(
                "SELECT event_id FROM outbox_events WHERE event_id IN (%s, %s)",
                (BOOKING_EVENT_ID, PAYMENT_EVENT_ID),
            )
            existing = {row[0] for row in cursor.fetchall()}

            if BOOKING_EVENT_ID not in existing:
                cursor.execute("""
                    SELECT id, shop_id, customer_id, service_id, staff_id,
                           start_at, booked_price_krw
                    FROM bookings
                    ORDER BY id
                    LIMIT 1
                """)
                booking = cursor.fetchone()
                if booking is None:
                    raise SystemExit(
                        "No booking found. Run the P0 generator before adding the DQ fixture."
                    )
                (
                    booking_id,
                    shop_id,
                    customer_id,
                    service_id,
                    staff_id,
                    start_at,
                    _booked_price_krw,
                ) = booking
                booking_event_time = datetime.now(KST)
                booking_payload = {
                    **base_event_payload(
                        event_id=BOOKING_EVENT_ID,
                        event_type="booking_created",
                        event_time=booking_event_time,
                        booking_id=int(booking_id),
                        shop_id=int(shop_id),
                    ),
                    "customer_id": int(customer_id),
                    "service_id": int(service_id),
                    "staff_id": int(staff_id),
                    "start_at": start_at.isoformat(),
                    # Deliberate contract violation: a booking price cannot be negative.
                    "booked_price_krw": -1,
                    "status": "scheduled",
                }
                insert_outbox_event(
                    cursor,
                    event_id=BOOKING_EVENT_ID,
                    event_type="booking_created",
                    booking_id=int(booking_id),
                    shop_id=int(shop_id),
                    payload=booking_payload,
                    event_time=booking_event_time,
                )
                inserted.append(
                    (BOOKING_EVENT_ID, "invalid_booking_created_fields")
                )

            if PAYMENT_EVENT_ID not in existing:
                cursor.execute("""
                    SELECT p.id, p.booking_id, b.shop_id,
                           p.request_amount_krw, p.payout_amount_krw,
                           p.refund_amount_krw, p.paid_amount_krw,
                           p.unpaid_amount_krw, p.payment_status,
                           p.needs_repayment
                    FROM payments p
                    JOIN bookings b ON b.id = p.booking_id
                    WHERE p.payment_status = 'paid'
                    ORDER BY p.id
                    LIMIT 1
                """)
                payment = cursor.fetchone()
                if payment is None:
                    raise SystemExit(
                        "No paid payment found. Run the P0 generator before adding the DQ fixture."
                    )

                (
                    payment_id,
                    booking_id,
                    shop_id,
                    request_amount_krw,
                    payout_amount_krw,
                    refund_amount_krw,
                    paid_amount_krw,
                    unpaid_amount_krw,
                    payment_status,
                    needs_repayment,
                ) = payment
                payment_event_time = datetime.now(KST)
                payment_payload = {
                    **base_event_payload(
                        event_id=PAYMENT_EVENT_ID,
                        event_type="payment_completed",
                        event_time=payment_event_time,
                        booking_id=int(booking_id),
                        shop_id=int(shop_id),
                    ),
                    "payment_id": int(payment_id),
                    "payment_transaction_id": PAYMENT_TRANSACTION_ID,
                    "request_amount_krw": int(request_amount_krw),
                    # Deliberate contract violation: one movement exceeds the request.
                    "amount_krw": int(request_amount_krw) + 1,
                    "payout_amount_krw": int(payout_amount_krw),
                    "refund_amount_krw": int(refund_amount_krw),
                    "paid_amount_krw": int(paid_amount_krw),
                    "unpaid_amount_krw": int(unpaid_amount_krw),
                    "payment_status": str(payment_status),
                    "needs_repayment": bool(needs_repayment),
                }
                insert_outbox_event(
                    cursor,
                    event_id=PAYMENT_EVENT_ID,
                    event_type="payment_completed",
                    booking_id=int(booking_id),
                    shop_id=int(shop_id),
                    payload=payment_payload,
                    event_time=payment_event_time,
                )
                inserted.append(
                    (PAYMENT_EVENT_ID, "invalid_payment_transaction_fields")
                )

        if inserted:
            for event_id, expected_error in inserted:
                print(
                    f"Committed DQ fixture event_id={event_id} "
                    f"expected_error={expected_error}"
                )
            print("MySQL commit complete; Kafka/Bronze delivery is asynchronous.")
        else:
            print("All DQ fixtures already exist; no event was added.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
