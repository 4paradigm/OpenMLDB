# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
from libs.clients.ns_cluster import NsCluster
import libs.conf as conf


@ddt.ddt
class TestNameserverMigrate(TestCaseBase):

    def get_base_attr(attr):
        TestCaseBase.setUpClass()
        return TestCaseBase.__getattribute__(TestCaseBase, attr)


    @ddt.data(
        (get_base_attr('leader'), time.time(), '0,1,2', get_base_attr('slave1'), ''),
    )
    @ddt.unpack
    def test_ns_client_migrate_args_check(self, src, tname, pid_group, des):
        pass


if __name__ == "__main__":
    load(TestNameserverMigrate)
