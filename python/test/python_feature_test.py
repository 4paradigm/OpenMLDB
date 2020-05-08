import unittest
import rtidb 
from datetime import date

class TestRtidb(unittest.TestCase):
  
  def setUp(self):
    self.nsc = rtidb.RTIDBClient("172.27.128.37:7183", "/rtidb_cluster")
  
  def test_FailedClient(self):
    with self.assertRaises(Exception) as context:
      nsc = rtidb.RTIDBClient("127.0.0.1:61811", "/issue-5")
    self.assertTrue("zk client init failed" in str(context.exception))
  def test_query(self):
    data = {"id":"11","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("test1", data, None))
    ro = rtidb.ReadOption()
    ro.index.update({"id":"11"})
    resp = self.nsc.query("test1", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(11, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
    condition_columns = {"id":"11"} 
    ok = self.nsc.delete("test1", condition_columns);
    self.assertEqual(ok, True);
    try:
      resp = self.nsc.query("test1", ro)
    except:
      self.assertTrue(True);
    else:
      self.assertTrue(False);
    ''' 
    for l in resp:
      self.assertEqual("card3", l["card"])
      #self.assertEqual("mcc3", l["mcc"]) #TODO: current skip verify mcc, beacuse mcc value is mcc3\x00 maybe server problem
      self.assertEqual(3, l["p_biz_date"])
    '''
    # multi index
    data = {"id":"1","name":"n1","mcc":"1","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    data = {"id":"2","name":"n2","mcc":"1","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    ro = rtidb.ReadOption()
    ro.index.update({"id":"1"})
    ro.index.update({"name":"n1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(1, l["id"])
      self.assertEqual("n1", l["name"])
      self.assertEqual(1, l["mcc"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(2, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(1 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      self.assertEqual(1, l["mcc"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      id += 1;
    self.assertEqual(2, id);
    # delete
    condition_columns = {"id":"1", "name":"n1"} 
    ok = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(ok, True);
    ro = rtidb.ReadOption()
    ro.index.update({"id":"1"})
    ro.index.update({"name":"n1"})
    try:
      resp = self.nsc.query("test1", ro)
    except:
      self.assertTrue(True);
    else:
      self.assertTrue(False);
    data = {"id":"1","name":"n1","mcc":"1","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    condition_columns = {"mcc":"1"} 
    ok = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(ok, True);
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"1"})
    try:
      resp = self.nsc.query("test1", ro)
    except:
      self.assertTrue(True);
    else:
      self.assertTrue(False);

  def test_traverse(self):
    for i in range(1000) :
        data = {"id":"{:d}".format(i), "attribute":"a{}".format(i), "image":"i{}".format(i)}
        self.assertTrue(self.nsc.put("test1", data, None))
    ro = rtidb.ReadOption()
    resp = self.nsc.traverse("test1", ro)
    id = 0;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("i{}".format(id), l["image"])
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);
    #TODO(kongquan): current put data use java client, when python put feature is complete, put data before traverse
    # multi index
    for i in range(1000) :
        data = {"id":"{:d}".format(i), "name":"n{}".format(i), "mcc":"{:d}".format(i), 
            "attribute":"a{}".format(i), "image":"i{}".format(i)}
        self.assertTrue(self.nsc.put("rt_ck", data, None))
    ro = rtidb.ReadOption()
    resp = self.nsc.traverse("rt_ck", ro)
    id = 0;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("n{}".format(id), l["name"])
      self.assertEqual(id, l["mcc"])
      self.assertEqual("i{}".format(id), l["image"])
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);


  def test_batchQuery(self):
    for i in range(1000) :
        data = {"id":"{:d}".format(i), "attribute":"a{}".format(i), "image":"i{}".format(i)}
        self.assertTrue(self.nsc.put("test1", data, None))
    ros = list()
    count = 1000;
    for i in range(count):
      ro = rtidb.ReadOption()
      ro.index = {"id": "{:d}".format(i)}
      ros.append(ro)
    resp = self.nsc.batch_query("test1", ros)
    self.assertEqual(1000, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("i{}".format(id), l["image"])
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);
    #TODO(kongquan): current put data use java client, when python put feature is complete, put data before traverse
    # multi index
    data = {"id":"1","name":"n1","mcc":"1","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    data = {"id":"2","name":"n2","mcc":"2","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    data = {"id":"3","name":"n3","mcc":"2","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    ros = list();
    ro = rtidb.ReadOption()
    ro.index.update({"id":"1"})
    ro.index.update({"name":"n1"})
    ros.append(ro);
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"2"})
    ros.append(ro);
    resp = self.nsc.batch_query("rt_ck", ros)
    self.assertEqual(3, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(1 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      if id == 0 : 
        self.assertEqual(1, l["mcc"])
      else :
        self.assertEqual(2, l["mcc"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      id += 1;

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
    # multi index
    data = {"id":"1","name":"n1","mcc":"1","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    data = {"id":"2","name":"n2","mcc":"1","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None))
    condition_columns = {"id":"1", "name":"n1"} 
    value_columns = {"attribute":"a2","image":"i2"}
    ok = self.nsc.update("rt_ck", condition_columns, value_columns, None);
    self.assertEqual(ok, True);
    ro = rtidb.ReadOption()
    ro.index.update({"id":"1"})
    ro.index.update({"name":"n1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(1, l["id"])
      self.assertEqual("n1", l["name"])
      self.assertEqual(1, l["mcc"])
      self.assertEqual("a2", l["attribute"])
      self.assertEqual("i2", l["image"])

    condition_columns = {"mcc":"1"} 
    value_columns = {"attribute":"a3","image":"i3"}
    ok = self.nsc.update("rt_ck", condition_columns, value_columns, None);
    self.assertEqual(ok, True);
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(2, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(1 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      self.assertEqual(1, l["mcc"])
      self.assertEqual("a3", l["attribute"])
      self.assertEqual("i3", l["image"])
      id += 1;
    self.assertEqual(2, id);

  def test_auto_gen(self):
    data = {"attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("auto", data, None))
    ro = rtidb.ReadOption()
    resp = self.nsc.traverse("auto", ro)
    id = 0;
    for l in resp:
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      id += 1;
    self.assertEqual(1, id);
  def test_date_index(self):
    data = {"id":"11","attribute":"a1", "image":"i1", "male":True, "date":date(2020,1,1), "ts":1588756531}
    self.assertTrue(self.nsc.put("date", data, None))
    ro = rtidb.ReadOption()
    ro.index.update({"date":date(2020,1,1)})
    resp = self.nsc.query("date", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(11, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      self.assertEqual(True, l["male"])
      self.assertEqual(date(2020,1,1), l["male"])
      self.assertEqual(1588756531, l["ts"])
    ro = rtidb.ReadOption()
    ro.index.update({"male":True})
    ro.index.update({"ts":1588756531})
    resp = self.nsc.query("date", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(11, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      self.assertEqual(True, l["male"])
      self.assertEqual(date(2020,1,1), l["male"])
      self.assertEqual(1588756531, l["ts"])
    # update
    condition_columns = {"date":date(2020,1,1)} 
    value_columns = {"male":False,"ts":"1588756532"}
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
      self.assertEqual(False, l["male"])
      self.assertEqual(date(2020,1,1), l["male"])
      self.assertEqual(1588756532, l["ts"])
    condition_columns = {"male":False,"ts":"1588756532"} 
    value_columns = {"male":True,"ts":"1588756532"}
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
      self.assertEqual(True, l["male"])
      self.assertEqual(date(2020,1,1), l["male"])
      self.assertEqual(1588756532, l["ts"])
    # delete
    condition_columns = {"date":date(2020,1,1)} 
    ok = self.nsc.delete("date", condition_columns);
    self.assertEqual(ok, True);
    try:
      resp = self.nsc.query("date", ro)
    except:
      self.assertTrue(True);
    else:
      self.assertTrue(False);

if __name__ == "__main__":
  unittest.main()
