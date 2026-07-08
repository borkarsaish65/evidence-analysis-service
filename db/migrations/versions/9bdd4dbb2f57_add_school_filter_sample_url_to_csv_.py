"""add_school_filter_sample_url_to_csv_source_types

Revision ID: 9bdd4dbb2f57
Revises: 3e798d60d93e
Create Date: 2026-07-03 11:19:12.764183

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9bdd4dbb2f57'
down_revision: Union[str, None] = '3e798d60d93e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(table: str) -> set:
    inspector = sa.inspect(op.get_bind())
    return {col['name'] for col in inspector.get_columns(table)}


def upgrade() -> None:
    if 'sample_school_filter_file_url' not in _existing_columns('csv_source_types'):
        op.add_column(
            'csv_source_types',
            sa.Column('sample_school_filter_file_url', sa.Text(), nullable=True),
        )


def downgrade() -> None:
    if 'sample_school_filter_file_url' in _existing_columns('csv_source_types'):
        op.drop_column('csv_source_types', 'sample_school_filter_file_url')
