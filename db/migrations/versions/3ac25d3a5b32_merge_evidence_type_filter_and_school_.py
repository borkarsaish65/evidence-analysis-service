"""merge evidence_type_filter and school_filter_upload heads

Revision ID: 3ac25d3a5b32
Revises: 5f30ba3da9ec, 9bdd4dbb2f57
Create Date: 2026-07-03 16:35:03.341623

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3ac25d3a5b32'
down_revision: Union[str, None] = ('5f30ba3da9ec', '9bdd4dbb2f57')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
