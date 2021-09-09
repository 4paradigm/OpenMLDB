import allure
import pytest

from nb_log import LogManager

from common.fedb_test import FedbTest
from executor import fedb_executor
from util.test_util import getCases

log = LogManager('python-sdk-test').get_logger_and_add_handlers()


class TestWindow(FedbTest):

    @pytest.mark.parametrize("testCase", getCases(["/function/cluster/",
                                                   "/function/window/"]))
    @allure.feature("Window")
    @allure.story("batch")
    def test_window(self, testCase):
        fedb_executor.build(self.connect, testCase).run()
