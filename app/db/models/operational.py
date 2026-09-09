from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    JSON,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.mysql import CHAR, DATETIME, INTEGER, SMALLINT
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, BaseModel


class Shop(BaseModel):
    __tablename__ = "shops"

    shop_name: Mapped[str] = mapped_column(String(100))
    region: Mapped[str] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=text("1"))

    services: Mapped[list[Service]] = relationship(back_populates="shop")
    staffs: Mapped[list[Staff]] = relationship(back_populates="shop")
    bookings: Mapped[list[Booking]] = relationship(back_populates="shop")


class Customer(BaseModel):
    __tablename__ = "customers"

    segment: Mapped[str] = mapped_column(String(32))

    bookings: Mapped[list[Booking]] = relationship(back_populates="customer")


class Service(BaseModel):
    __tablename__ = "services"
    __table_args__ = (
        CheckConstraint("list_price_krw >= 0", name="services_price_nonnegative"),
        CheckConstraint("duration_minutes > 0", name="services_duration_positive"),
        Index("idx_services_shop_active", "shop_id", "is_active"),
    )

    shop_id: Mapped[int] = mapped_column(ForeignKey("shops.id", name="fk_services_shop"))
    service_name: Mapped[str] = mapped_column(String(100))
    list_price_krw: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    duration_minutes: Mapped[int] = mapped_column(SMALLINT(unsigned=True))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=text("1"))

    shop: Mapped[Shop] = relationship(back_populates="services")
    bookings: Mapped[list[Booking]] = relationship(back_populates="service")


class Staff(BaseModel):
    __tablename__ = "staffs"
    __table_args__ = (Index("idx_staffs_shop_active", "shop_id", "is_active"),)

    shop_id: Mapped[int] = mapped_column(ForeignKey("shops.id", name="fk_staffs_shop"))
    staff_name: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=text("1"))

    shop: Mapped[Shop] = relationship(back_populates="staffs")
    bookings: Mapped[list[Booking]] = relationship(back_populates="staff")


class Booking(BaseModel):
    __tablename__ = "bookings"
    __table_args__ = (
        CheckConstraint("booked_price_krw >= 0", name="bookings_price_nonnegative"),
        Index("idx_bookings_shop_start", "shop_id", "start_at"),
        Index("idx_bookings_customer_start", "customer_id", "start_at"),
        Index("idx_bookings_service_start", "service_id", "start_at"),
        Index("idx_bookings_staff_start", "staff_id", "start_at"),
    )

    shop_id: Mapped[int] = mapped_column(ForeignKey("shops.id", name="fk_bookings_shop"))
    customer_id: Mapped[int] = mapped_column(
        ForeignKey("customers.id", name="fk_bookings_customer")
    )
    service_id: Mapped[int] = mapped_column(
        ForeignKey("services.id", name="fk_bookings_service")
    )
    staff_id: Mapped[int] = mapped_column(
        ForeignKey("staffs.id", name="fk_bookings_staff")
    )
    start_at: Mapped[datetime] = mapped_column(DATETIME(fsp=6))
    status: Mapped[str] = mapped_column(String(32))
    booked_price_krw: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    cancelled_at: Mapped[datetime | None] = mapped_column(DATETIME(fsp=6))

    shop: Mapped[Shop] = relationship(back_populates="bookings")
    customer: Mapped[Customer] = relationship(back_populates="bookings")
    service: Mapped[Service] = relationship(back_populates="bookings")
    staff: Mapped[Staff] = relationship(back_populates="bookings")
    payment: Mapped[Payment | None] = relationship(back_populates="booking", uselist=False)


class Payment(BaseModel):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("paid_amount_krw >= 0", name="payments_paid_nonnegative"),
        CheckConstraint("refunded_amount_krw >= 0", name="payments_refunded_nonnegative"),
        UniqueConstraint("booking_id", name="uq_payments_booking"),
    )

    booking_id: Mapped[int] = mapped_column(
        ForeignKey("bookings.id", name="fk_payments_booking")
    )
    charged_amount_krw: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    paid_amount_krw: Mapped[int] = mapped_column(INTEGER(unsigned=True), server_default=text("0"))
    refunded_amount_krw: Mapped[int] = mapped_column(INTEGER(unsigned=True), server_default=text("0"))
    payment_status: Mapped[str] = mapped_column(String(32))
    paid_at: Mapped[datetime | None] = mapped_column(DATETIME(fsp=6))
    refunded_at: Mapped[datetime | None] = mapped_column(DATETIME(fsp=6))

    booking: Mapped[Booking] = relationship(back_populates="payment")
    transactions: Mapped[list[PaymentTransaction]] = relationship(back_populates="payment")


class PaymentTransaction(BaseModel):
    __tablename__ = "payment_transactions"
    __table_args__ = (
        CheckConstraint("amount_krw > 0", name="payment_transactions_amount_positive"),
        Index("idx_payment_transactions_payment_occurred", "payment_id", "occurred_at"),
    )

    payment_id: Mapped[int] = mapped_column(
        ForeignKey("payments.id", name="fk_payment_transactions_payment")
    )
    transaction_type: Mapped[str] = mapped_column(String(32))
    amount_krw: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    occurred_at: Mapped[datetime] = mapped_column(DATETIME(fsp=6))
    payment: Mapped[Payment] = relationship(back_populates="transactions")


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("schema_version > 0", name="outbox_schema_version_positive"),
        Index("idx_outbox_created_at", "created_at"),
        Index("idx_outbox_aggregate", "aggregate_type", "aggregate_id"),
        Index("idx_outbox_partition_time", "partition_key", "event_time"),
    )

    event_id: Mapped[str] = mapped_column(CHAR(36), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(64))
    aggregate_id: Mapped[str] = mapped_column(CHAR(36))
    event_type: Mapped[str] = mapped_column(String(64))
    partition_key: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    schema_version: Mapped[int] = mapped_column(SMALLINT(unsigned=True), server_default=text("1"))
    event_time: Mapped[datetime] = mapped_column(DATETIME(fsp=6))
    created_at: Mapped[datetime] = mapped_column(
        DATETIME(fsp=6), server_default=text("CURRENT_TIMESTAMP(6)")
    )
