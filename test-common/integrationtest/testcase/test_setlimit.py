# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import libs.ddt as ddt
from libs.test_loader import load



@ddt.ddt
class TestSetLimit(TestCaseBase):
    @ddt.data(
        ('setlimit', 'Put', 10, 'Set Limit ok'),
        ('setlimit', 'Put', 0, 'Set Limit ok'),
        ('setlimit', 'Put', 1, 'Set Limit ok'),
        ('setlimit', 'Put', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Put', -1, 'Fail to set limit'),
        ('setlimit', 'Put', 1.5, 'Bad set limit format'),
        ('setlimit', 'Get', 10, 'Set Limit ok'),
        ('setlimit', 'Get', 0, 'Set Limit ok'),
        ('setlimit', 'Get', 1, 'Set Limit ok'),
        ('setlimit', 'Get', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Get', -1, 'Fail to set limit'),
        ('setlimit', 'Get', 1.5, 'Bad set limit format'),
        ('setlimit', 'Scan', 10, 'Set Limit ok'),
        ('setlimit', 'Scan', 0, 'Set Limit ok'),
        ('setlimit', 'Scan', 1, 'Set Limit ok'),
        ('setlimit', 'Scan', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Scan', -1, 'Fail to set limit'),
        ('setlimit', 'Scan', 1.5, 'Bad set limit format'),
        ('setlimit', 'Server', 10, 'Set Limit ok'),
        ('setlimit', 'Server', 0, 'Set Limit ok'),
        ('setlimit', 'Server', 1, 'Set Limit ok'),
        ('setlimit', 'Server', 2147483647, 'Set Limit ok'),
        ('setlimit', 'Server', -1, 'Fail to set limit'),
        ('setlimit', 'Server', 1.5, 'Bad set limit format'),
        ('setLimit', 'Scan', 1, 'unsupported cmd')
    )
    @ddt.unpack
    def test_set_max_concurrency(self, command, method, max_concurrency_limit, rsp_msg):
        """
        修改并发限制的数值
        :param self:
        :param max_concurrency_limit:
        :return:
        """
        rs1 = self.ns_setlimit(self.leader, command, method, max_concurrency_limit)
        self.assertIn(rsp_msg, rs1)


if __name__ == "__main__":
    load(TestSetLimit)

