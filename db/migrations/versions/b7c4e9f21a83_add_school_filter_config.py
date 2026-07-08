"""add school_filter_config: per-tenant JSONB required-column registry on csv_source_types

Revision ID: b7c4e9f21a83
Revises: 9bdd4dbb2f57, a1b2c3d4e5f6
Create Date: 2026-07-08 00:00:00.000000

Merges the two heads left unreconciled after merging evidence-type-filter's migrations
(5f30ba3da9ec, a1b2c3d4e5f6) into this branch, which had already diverged with its own
school-filter migrations (3e798d60d93e, 9bdd4dbb2f57) off the same parent (c4e7b1f8a2d9).

Also moves the school-filter required-column name out of core/constants.py (Python code,
required a deploy to change) and into a DB-driven column, following the same pattern as
evidence_types_config. server_default backfills existing rows with the current hardcoded
value so this is a zero-downtime, non-breaking addition.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b7c4e9f21a83'
down_revision: Union[str, Sequence[str], None] = ('9bdd4dbb2f57', 'a1b2c3d4e5f6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEFAULT_SCHOOL_FILTER_CONFIG_JSON = '{"required_column": "UDISE+ SCHOOL CODE"}'


def upgrade() -> None:
    op.add_column(
        'csv_source_types',
        sa.Column(
            'school_filter_config',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text(f"'{_DEFAULT_SCHOOL_FILTER_CONFIG_JSON}'::jsonb"),
        ),
    )


def downgrade() -> None:
    op.drop_column('csv_source_types', 'school_filter_config')
