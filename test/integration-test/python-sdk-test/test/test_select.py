import allure
import pytest

from nb_log import LogManager

from common.fedb_test import FedbTest
from executor import fedb_executor
from util.test_util import getCases

log = LogManager('python-sdk-test').get_logger_and_add_handlers()


class TestSelect(FedbTest):

    @pytest.mark.parametrize("testCase", getCases(["/function/select/test_select_sample.yaml",
                                                   "/function/select/test_sub_select.yaml"]))
    @allure.feature("SelectTest")
    @allure.story("batch")
    def test_create(self, testCase):
        fedb_executor.build(self.connect, testCase).run()
