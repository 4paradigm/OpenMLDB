
import allure
import pytest

from nb_log import LogManager

from common.fedb_test import FedbTest
from executor import fedb_executor
from util.test_util import getCases

log = LogManager('python-sdk-test').get_logger_and_add_handlers()

class TestDDL(FedbTest):

    @pytest.mark.parametrize("testCase", getCases(["/function/ddl/test_create.yaml"]))
    @allure.feature("DDL")
    @allure.story("create")
    def test_create(self, testCase):
        print(testCase)
        fedb_executor.build(self.connect, testCase).run()

    @pytest.mark.parametrize("testCase", getCases(["/function/dml/test_insert.yaml"]))
    @allure.feature("DDL")
    @allure.story("insert")
    def test_insert(self, testCase):
        fedb_executor.build(self.connect, testCase).run()

    @pytest.mark.parametrize("testCase", getCases(["/function/ddl/test_ttl.yaml"]))
    @allure.feature("DDL")
    @allure.story("ttl")
    def test_ttl(self, testCase):
        fedb_executor.build(self.connect, testCase).run()
