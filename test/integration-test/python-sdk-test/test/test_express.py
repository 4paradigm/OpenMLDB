import allure
import pytest

from nb_log import LogManager

from common.fedb_test import FedbTest
from executor import fedb_executor
from util.test_util import getCases

log = LogManager('fedb-sdk-test').get_logger_and_add_handlers()


class TestExpress(FedbTest):

    @pytest.mark.parametrize("testCase", getCases(["/function/expression/"]))
    @allure.feature("Express")
    @allure.story("batch")
    def test_express(self, testCase):
        fedb_executor.build(self.connect, testCase).run()
