import unittest
from testcase.test_create import TestCreateTable
from testcase.test_gettablestatus import TestGetTableStatus
from testcase.test_makesnapshot import TestMakeSnapshot
from testcase.test_loadtable import TestLoadTable
from testcase.test_put import TestPut
from testcase.test_changerole import TestChangeRole
from testcase.test_addreplica import TestAddReplica
from testcase.test_pausesnapshot import TestPauseSnapshot


if __name__ == "__main__":
  suite = unittest.TestSuite([
      unittest.TestLoader().loadTestsFromTestCase(TestCreateTable),
      unittest.TestLoader().loadTestsFromTestCase(TestGetTableStatus),
      unittest.TestLoader().loadTestsFromTestCase(TestMakeSnapshot),
      unittest.TestLoader().loadTestsFromTestCase(TestLoadTable),
      unittest.TestLoader().loadTestsFromTestCase(TestPut),
      unittest.TestLoader().loadTestsFromTestCase(TestChangeRole),
      unittest.TestLoader().loadTestsFromTestCase(TestAddReplica),
      unittest.TestLoader().loadTestsFromTestCase(TestPauseSnapshot),
      ])
  unittest.TextTestRunner(verbosity=2).run(suite)
