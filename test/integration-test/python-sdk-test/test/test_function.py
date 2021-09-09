import allure
import pytest

from nb_log import LogManager

from common.fedb_test import FedbTest
from executor import fedb_executor
from util.test_util import getCases

log = LogManager('python-sdk-test').get_logger_and_add_handlers()


class TestFunction(FedbTest):

    @pytest.mark.parametrize("testCase", getCases(["/function/function/test_calculate.yaml"]))
    @allure.feature("Function")
    @allure.story("batch")
    def test_function(self, testCase):
        fedb_executor.build(self.connect, testCase).run()