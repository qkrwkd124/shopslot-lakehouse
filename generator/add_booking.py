"""Append one new booking and its outbox event; never rerun or reset P0."""

import os
import uuid
from datetime import datetime, timedelta

import pymysql

from produce_p0_events import (
    KST,
    base_event_payload,
    insert_outbox_event,
    mysql_datetime,
    transaction,
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
    try:
        # The existing helper commits both inserts together, or rolls both back.
        with transaction(connection) as cursor:
            cursor.execute("""
                SELECT s.id, v.id, t.id, v.list_price_krw
                FROM shops s
                JOIN services v ON v.shop_id = s.id AND v.is_active = 1
                JOIN staffs t ON t.shop_id = s.id AND t.is_active = 1
                WHERE s.is_active = 1
                ORDER BY s.id, v.id, t.id
                LIMIT 1
            """)
            reference = cursor.fetchone()
            cursor.execute("SELECT id FROM customers ORDER BY id LIMIT 1")
            customer = cursor.fetchone()
            if reference is None or customer is None:
                raise SystemExit(
                    "No usable shop/service/staff/customer found. Prepare reference data first; "
                    "generate-p0 is only for an environment without existing P0 bookings."
                )

            shop_id, service_id, staff_id, price = reference
            customer_id = customer[0]
            event_time = datetime.now(KST)
            start_at = event_time + timedelta(days=1)
            event_id = str(uuid.uuid4())

            # Omit id: MySQL assigns it, avoiding collisions with fixed P0 IDs.
            cursor.execute("""
                INSERT INTO bookings
                  (shop_id, customer_id, service_id, staff_id, start_at,
                   status, booked_price_krw)
                VALUES (%s, %s, %s, %s, %s, 'scheduled', %s)
            """, (shop_id, customer_id, service_id, staff_id,
                  mysql_datetime(start_at), price))
            booking_id = cursor.lastrowid
            payload = {
                **base_event_payload(
                    event_id=event_id, event_type="booking_created",
                    event_time=event_time, booking_id=booking_id, shop_id=shop_id,
                ),
                "customer_id": customer_id,
                "service_id": service_id,
                "staff_id": staff_id,
                "start_at": start_at.isoformat(),
                "booked_price_krw": price,
                "status": "scheduled",
            }
            insert_outbox_event(
                cursor, event_id=event_id, event_type="booking_created",
                booking_id=booking_id, shop_id=shop_id,
                payload=payload, event_time=event_time,
            )
        # Success is printed only after the transaction has committed.
        print(f"Committed booking_id={booking_id} event_id={event_id} "
              f"event_type=booking_created shop_id={shop_id}")
        print("MySQL commit complete; Kafka/Bronze delivery is asynchronous.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
