"""add shop operating dates

Revision ID: 20261119_0002
Revises: 20260908_0001
"""

from alembic import op
import sqlalchemy as sa


revision = "20261119_0002"
down_revision = "20260908_0001"


def upgrade() -> None:
    op.add_column("shops", sa.Column("opened_on", sa.Date(), nullable=True))
    op.add_column("shops", sa.Column("closed_on", sa.Date(), nullable=True))
    op.create_check_constraint(
        op.f("ck_shops_operating_dates"), "shops", "closed_on >= opened_on"
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_shops_operating_dates"), "shops", type_="check")
    op.drop_column("shops", "closed_on")
    op.drop_column("shops", "opened_on")
