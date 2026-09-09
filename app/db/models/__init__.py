"""Import every mapped model so Alembic receives one complete metadata graph."""

from app.db.models.operational import (
    Booking,
    Customer,
    OutboxEvent,
    Payment,
    PaymentTransaction,
    Service,
    Shop,
    Staff,
)

__all__ = [
    "Booking",
    "Customer",
    "OutboxEvent",
    "Payment",
    "PaymentTransaction",
    "Service",
    "Shop",
    "Staff",
]
