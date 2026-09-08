"""create P0 operational source model

Revision ID: 20260908_0001
Revises:
Create Date: 2026-09-08
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


revision: str = "20260908_0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


TIMESTAMP = mysql.DATETIME(fsp=6)
CREATED_AT = sa.text("CURRENT_TIMESTAMP(6)")
UPDATED_AT = sa.text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)")


def upgrade() -> None:
    op.create_table(
        "shops",
        sa.Column("shop_id", sa.String(length=32), primary_key=True),
        sa.Column("shop_name", sa.String(length=100), nullable=False),
        sa.Column("region", sa.String(length=64), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.Column("updated_at", TIMESTAMP, nullable=False, server_default=UPDATED_AT),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "customers",
        sa.Column("customer_id", sa.String(length=32), primary_key=True),
        sa.Column("segment", sa.String(length=32), nullable=False),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.Column("updated_at", TIMESTAMP, nullable=False, server_default=UPDATED_AT),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "services",
        sa.Column("service_id", mysql.CHAR(length=36), primary_key=True),
        sa.Column("shop_id", sa.String(length=32), nullable=False),
        sa.Column("service_name", sa.String(length=100), nullable=False),
        sa.Column("list_price_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("duration_minutes", mysql.SMALLINT(unsigned=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.Column("updated_at", TIMESTAMP, nullable=False, server_default=UPDATED_AT),
        sa.CheckConstraint("list_price_krw >= 0", name="chk_services_price_nonnegative"),
        sa.CheckConstraint("duration_minutes > 0", name="chk_services_duration_positive"),
        sa.ForeignKeyConstraint(["shop_id"], ["shops.shop_id"], name="fk_services_shop"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("idx_services_shop_active", "services", ["shop_id", "is_active"])
    op.create_table(
        "bookings",
        sa.Column("booking_id", mysql.CHAR(length=36), primary_key=True),
        sa.Column("shop_id", sa.String(length=32), nullable=False),
        sa.Column("customer_id", sa.String(length=32), nullable=False),
        sa.Column("service_id", mysql.CHAR(length=36), nullable=False),
        sa.Column("staff_id", sa.String(length=32), nullable=False),
        sa.Column("start_at", TIMESTAMP, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("booked_price_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("cancelled_at", TIMESTAMP),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.Column("updated_at", TIMESTAMP, nullable=False, server_default=UPDATED_AT),
        sa.CheckConstraint("booked_price_krw >= 0", name="chk_bookings_price_nonnegative"),
        sa.ForeignKeyConstraint(["shop_id"], ["shops.shop_id"], name="fk_bookings_shop"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.customer_id"], name="fk_bookings_customer"),
        sa.ForeignKeyConstraint(["service_id"], ["services.service_id"], name="fk_bookings_service"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("idx_bookings_shop_start", "bookings", ["shop_id", "start_at"])
    op.create_index("idx_bookings_customer_start", "bookings", ["customer_id", "start_at"])
    op.create_index("idx_bookings_service_start", "bookings", ["service_id", "start_at"])
    op.create_table(
        "payments",
        sa.Column("payment_id", mysql.CHAR(length=36), primary_key=True),
        sa.Column("booking_id", mysql.CHAR(length=36), nullable=False),
        sa.Column("charged_amount_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("paid_amount_krw", mysql.INTEGER(unsigned=True), nullable=False, server_default=sa.text("0")),
        sa.Column("refunded_amount_krw", mysql.INTEGER(unsigned=True), nullable=False, server_default=sa.text("0")),
        sa.Column("payment_status", sa.String(length=32), nullable=False),
        sa.Column("paid_at", TIMESTAMP),
        sa.Column("refunded_at", TIMESTAMP),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.Column("updated_at", TIMESTAMP, nullable=False, server_default=UPDATED_AT),
        sa.CheckConstraint("paid_amount_krw >= 0", name="chk_payments_paid_nonnegative"),
        sa.CheckConstraint("refunded_amount_krw >= 0", name="chk_payments_refunded_nonnegative"),
        sa.ForeignKeyConstraint(["booking_id"], ["bookings.booking_id"], name="fk_payments_booking"),
        sa.UniqueConstraint("booking_id", name="uq_payments_booking"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "payment_transactions",
        sa.Column("payment_transaction_id", mysql.CHAR(length=36), primary_key=True),
        sa.Column("payment_id", mysql.CHAR(length=36), nullable=False),
        sa.Column("transaction_type", sa.String(length=32), nullable=False),
        sa.Column("amount_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("occurred_at", TIMESTAMP, nullable=False),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.CheckConstraint("amount_krw > 0", name="chk_payment_transactions_amount_positive"),
        sa.ForeignKeyConstraint(["payment_id"], ["payments.payment_id"], name="fk_payment_transactions_payment"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index(
        "idx_payment_transactions_payment_occurred",
        "payment_transactions",
        ["payment_id", "occurred_at"],
    )
    op.create_table(
        "outbox_events",
        sa.Column("event_id", mysql.CHAR(length=36), primary_key=True),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", mysql.CHAR(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("partition_key", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("schema_version", mysql.SMALLINT(unsigned=True), nullable=False, server_default=sa.text("1")),
        sa.Column("event_time", TIMESTAMP, nullable=False),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.CheckConstraint("schema_version > 0", name="chk_outbox_schema_version_positive"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("idx_outbox_created_at", "outbox_events", ["created_at"])
    op.create_index("idx_outbox_aggregate", "outbox_events", ["aggregate_type", "aggregate_id"])
    op.create_index("idx_outbox_partition_time", "outbox_events", ["partition_key", "event_time"])


def downgrade() -> None:
    op.drop_table("outbox_events")
    op.drop_table("payment_transactions")
    op.drop_table("payments")
    op.drop_table("bookings")
    op.drop_table("services")
    op.drop_table("customers")
    op.drop_table("shops")
