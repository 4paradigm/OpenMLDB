from sqlalchemy.dialects import registry
import pytest

registry.register("openmldb", "openmldb.sqlalchemy_openmldb.openmldb_dialect", "OpenmldbDialect")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *
