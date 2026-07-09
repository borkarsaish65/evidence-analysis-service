"""add evidence_types_config: per-tenant JSONB evidence-type/extension registry on csv_source_types

Revision ID: a1b2c3d4e5f6
Revises: 5f30ba3da9ec
Create Date: 2026-07-06 00:00:00.000000

Moves the evidence-type -> file-extension mapping out of core/constants.py (Python code,
required a deploy to change) and into a DB-driven column, following the same pattern as
default_thresholds. server_default backfills existing rows with the current hardcoded set
so this is a zero-downtime, non-breaking addition.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '5f30ba3da9ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEFAULT_EVIDENCE_TYPES_CONFIG_JSON = (
    '[{"key": "image", "label": "Image", '
    '"extensions": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"]}, '
    '{"key": "pdf", "label": "PDF", "extensions": [".pdf"]}, '
    '{"key": "excel", "label": "Excel", "extensions": [".xlsx"]}]'
)


def upgrade() -> None:
    op.add_column(
        'csv_source_types',
        sa.Column(
            'evidence_types_config',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text(f"'{_DEFAULT_EVIDENCE_TYPES_CONFIG_JSON}'::jsonb"),
        ),
    )


def downgrade() -> None:
    op.drop_column('csv_source_types', 'evidence_types_config')
