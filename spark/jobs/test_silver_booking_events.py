"""Small synthetic cases; no writes to Kafka, Bronze or another Iceberg table."""
import json
from datetime import datetime

from silver_booking_events import candidates, clean_events


def check_fixtures(spark):
    base = dict(event_id="e1", event_type="booking_created", schema_version=1,
                event_time="2026-09-10T10:00:00+09:00", ingest_time="2026-09-10T10:01:00+09:00",
                booking_id=10, shop_id=1)
    payloads = [json.dumps(base), json.dumps(base), "{broken", None,
                json.dumps(dict(base, event_id="e2", shop_id=None)),
                json.dumps(dict(base, event_id="e3", event_time="not-a-time")),
                json.dumps(dict(base, event_id="e4", schema_version=2)),
                json.dumps(dict(base, event_id="e5", event_type="payment_completed")),
                json.dumps(dict(base, event_id="e6", event_type="payment_refunded",
                                payment_id=1, payment_transaction_id=2, amount_krw=20000)),
                json.dumps(dict(base, event_id="e7", booking_id="bad")),
                json.dumps(dict(base, event_id="e8", start_at="bad"))]
    now = datetime(2026, 9, 10, 12)
    schema = ("payload STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, "
              "kafka_timestamp TIMESTAMP, bronze_ingested_at TIMESTAMP")
    def frame(values):
        return spark.createDataFrame([(p, "fixture", 0, i, now, now) for i, p in enumerate(values)], schema)
    parsed = candidates(spark, frame(payloads)).cache()
    try:
        assert parsed.filter("validation_error IS NOT NULL").count() == 8
        clean = clean_events(spark, parsed)
        rows = {r.event_id: r for r in clean.collect()}
        assert set(rows) == {"e1", "e6"}
        assert rows["e1"].kafka_offset == 0
        assert rows["e6"].amount_krw == 20000
        assert dict(clean.dtypes)["booking_id"] == "bigint"
        assert dict(clean.dtypes)["event_time"] == "timestamp"
        assert clean.exceptAll(clean_events(spark, parsed)).count() == 0
    finally:
        parsed.unpersist()
    conflict = candidates(spark, frame([json.dumps(base), json.dumps(dict(base, shop_id=2))]))
    try:
        clean_events(spark, conflict)
    except RuntimeError as error:
        assert "Conflicting payloads" in str(error)
    else:
        raise AssertionError("Conflicting event_id payloads were silently accepted")
    print("Silver SQL fixtures passed: parsing, types, validation, deduplication, conflict guard.", flush=True)
