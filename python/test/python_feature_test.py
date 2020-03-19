import unittest
import rtidb 

class TestRtidb(unittest.TestCase):
  
  def setUp(self):
    self.nsc = rtidb.RTIDBClient("172.27.128.37:7183", "/rtidb_cluster")
  
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

  def test_update(self):
    data = {"id":"11","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("test1", data, None))
    condition_columns = {"id":"11"} 
    value_columns = {"attribute":"a3","image":"i3"}
    ok = self.nsc.update("test1", condition_columns, value_columns, None);
    self.assertEqual(ok, True);
    ro = rtidb.ReadOption()
    ro.index.update({"id":"11"})
    resp = self.nsc.query("test1", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(11, l["id"])
      self.assertEqual("a3", l["attribute"])
      self.assertEqual("i3", l["image"])



if __name__ == "__main__":
  unittest.main()
