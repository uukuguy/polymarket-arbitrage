"""Single-authority contract for the control-plane schema revision."""

from alembic.config import Config
from alembic.script import ScriptDirectory

from polyarb.control_plane.db_role_admin import EXPECTED_REVISION
from polyarb.control_plane.schema_contract import CONTROL_PLANE_SCHEMA_REVISION


def test_control_plane_schema_revision_is_the_only_alembic_head() -> None:
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))

    assert scripts.get_heads() == [CONTROL_PLANE_SCHEMA_REVISION]
    assert EXPECTED_REVISION == CONTROL_PLANE_SCHEMA_REVISION
