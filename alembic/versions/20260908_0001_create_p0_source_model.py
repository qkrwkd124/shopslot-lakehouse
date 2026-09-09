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


BIGINT = mysql.BIGINT(unsigned=True)
TIMESTAMP = mysql.DATETIME(fsp=6)
CREATED_AT = sa.text("CURRENT_TIMESTAMP(6)")
UPDATED_AT = sa.text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)")


def entity_columns() -> tuple[sa.Column, sa.Column, sa.Column]:
    return (
        sa.Column("id", BIGINT, primary_key=True, autoincrement=True),
        sa.Column("created_at", TIMESTAMP, nullable=False, server_default=CREATED_AT),
        sa.Column("updated_at", TIMESTAMP, nullable=False, server_default=UPDATED_AT),
    )


def upgrade() -> None:
    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "shops",
        id_column,
        sa.Column("shop_name", sa.String(length=100), nullable=False),
        sa.Column("region", sa.String(length=64), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        created_at,
        updated_at,
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "customers",
        id_column,
        sa.Column("segment", sa.String(length=32), nullable=False),
        created_at,
        updated_at,
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "services",
        id_column,
        sa.Column("shop_id", BIGINT, nullable=False),
        sa.Column("service_name", sa.String(length=100), nullable=False),
        sa.Column("list_price_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("duration_minutes", mysql.SMALLINT(unsigned=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        created_at,
        updated_at,
        sa.CheckConstraint("list_price_krw >= 0", name="chk_services_price_nonnegative"),
        sa.CheckConstraint("duration_minutes > 0", name="chk_services_duration_positive"),
        sa.ForeignKeyConstraint(["shop_id"], ["shops.id"], name="fk_services_shop"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("idx_services_shop_active", "services", ["shop_id", "is_active"])

    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "staffs",
        id_column,
        sa.Column("shop_id", BIGINT, nullable=False),
        sa.Column("staff_name", sa.String(length=100), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        created_at,
        updated_at,
        sa.ForeignKeyConstraint(["shop_id"], ["shops.id"], name="fk_staffs_shop"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("idx_staffs_shop_active", "staffs", ["shop_id", "is_active"])

    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "bookings",
        id_column,
        sa.Column("shop_id", BIGINT, nullable=False),
        sa.Column("customer_id", BIGINT, nullable=False),
        sa.Column("service_id", BIGINT, nullable=False),
        sa.Column("staff_id", BIGINT, nullable=False),
        sa.Column("start_at", TIMESTAMP, nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("booked_price_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("cancelled_at", TIMESTAMP),
        created_at,
        updated_at,
        sa.CheckConstraint("booked_price_krw >= 0", name="chk_bookings_price_nonnegative"),
        sa.ForeignKeyConstraint(["shop_id"], ["shops.id"], name="fk_bookings_shop"),
        sa.ForeignKeyConstraint(["customer_id"], ["customers.id"], name="fk_bookings_customer"),
        sa.ForeignKeyConstraint(["service_id"], ["services.id"], name="fk_bookings_service"),
        sa.ForeignKeyConstraint(["staff_id"], ["staffs.id"], name="fk_bookings_staff"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("idx_bookings_shop_start", "bookings", ["shop_id", "start_at"])
    op.create_index("idx_bookings_customer_start", "bookings", ["customer_id", "start_at"])
    op.create_index("idx_bookings_service_start", "bookings", ["service_id", "start_at"])
    op.create_index("idx_bookings_staff_start", "bookings", ["staff_id", "start_at"])

    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "payments",
        id_column,
        sa.Column("booking_id", BIGINT, nullable=False),
        sa.Column("charged_amount_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column(
            "paid_amount_krw",
            mysql.INTEGER(unsigned=True),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "refunded_amount_krw",
            mysql.INTEGER(unsigned=True),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("payment_status", sa.String(length=32), nullable=False),
        sa.Column("paid_at", TIMESTAMP),
        sa.Column("refunded_at", TIMESTAMP),
        created_at,
        updated_at,
        sa.CheckConstraint("paid_amount_krw >= 0", name="chk_payments_paid_nonnegative"),
        sa.CheckConstraint("refunded_amount_krw >= 0", name="chk_payments_refunded_nonnegative"),
        sa.ForeignKeyConstraint(["booking_id"], ["bookings.id"], name="fk_payments_booking"),
        sa.UniqueConstraint("booking_id", name="uq_payments_booking"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    id_column, created_at, updated_at = entity_columns()
    op.create_table(
        "payment_transactions",
        id_column,
        sa.Column("payment_id", BIGINT, nullable=False),
        sa.Column("transaction_type", sa.String(length=32), nullable=False),
        sa.Column("amount_krw", mysql.INTEGER(unsigned=True), nullable=False),
        sa.Column("occurred_at", TIMESTAMP, nullable=False),
        created_at,
        updated_at,
        sa.CheckConstraint("amount_krw > 0", name="chk_payment_transactions_amount_positive"),
        sa.ForeignKeyConstraint(
            ["payment_id"], ["payments.id"], name="fk_payment_transactions_payment"
        ),
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
        sa.Column(
            "schema_version",
            mysql.SMALLINT(unsigned=True),
            nullable=False,
            server_default=sa.text("1"),
        ),
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
    op.drop_table("staffs")
    op.drop_table("services")
    op.drop_table("customers")
    op.drop_table("shops")
