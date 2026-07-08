"""add processing_config: generic JSONB column for per-execution processing options (e.g. evidence_types)

Revision ID: 5f30ba3da9ec
Revises: c4e7b1f8a2d9
Create Date: 2026-06-18 00:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '5f30ba3da9ec'
down_revision: Union[str, None] = 'c4e7b1f8a2d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'executions',
        sa.Column('processing_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('executions', 'processing_config')
