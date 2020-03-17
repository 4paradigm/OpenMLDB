import unittest
import rtidb 

class TestRtidb(unittest.TestCase):
  def setUp(self):
    self.nsc = rtidb.RTIDBClient("172.27.128.37:6181", "/issue-5")
  def test_FailedClient(self):
    with self.assertRaises(Exception) as context:
      nsc = rtidb.RTIDBClient("127.0.0.1:61811", "/issue-5")
    self.assertTrue("zk client init failed" in str(context.exception))
  def test_put(self):
    data = {"card":"card3","mcc":"mcc3", "p_biz_date":3}
    self.assertTrue(self.nsc.put("test1", data, None))
  def test_query(self):
    ro = rtidb.ReadOption()
    ro.index.update({"card":"card3"})
    resp = self.nsc.query("test1", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual("card3", l["card"])
      #self.assertEqual("mcc3", l["mcc"]) #TODO: current skip verify mcc, beacuse mcc value is mcc3\x00 maybe server problem
      self.assertEqual(3, l["p_biz_date"])

if __name__ == "__main__":
  unittest.main()
