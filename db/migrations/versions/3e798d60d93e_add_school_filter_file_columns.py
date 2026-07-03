"""add_school_filter_file_columns

Revision ID: 3e798d60d93e
Revises: c4e7b1f8a2d9
Create Date: 2026-07-01 11:22:00.756063

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3e798d60d93e'
down_revision: Union[str, None] = 'c4e7b1f8a2d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('executions', sa.Column('school_filter_file_url', sa.Text(), nullable=True))
    op.add_column('executions', sa.Column('school_filter_file_size', sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column('executions', 'school_filter_file_size')
    op.drop_column('executions', 'school_filter_file_url')
