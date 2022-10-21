import logging
import sys
import os
import pytest
from autofe.autofe import OpenMLDBSQLGenerator

class TestSQLTrans:
    def test_mutli_op_trans(self):
        assert OpenMLDBSQLGenerator.multi_op_trans('lag', 'lag3', 'c1') == 'lag(c1,3)'
        assert OpenMLDBSQLGenerator.multi_op_trans('lag', 'lag3-0', 'c1') == 'lag(c1,3)-lag(c1,0)'


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))