import unittest
import rtidb 
from datetime import date

class TestRtidb(unittest.TestCase):
  
  def setUp(self):
    self.nsc = rtidb.RTIDBClient("127.0.0.1:6181", "/onebox")

  def test_query(self):
    data = {"id":"2001","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("test1", data, None).success())
    ro = rtidb.ReadOption()
    ro.index.update({"image":"i1"})
    try:
      resp = self.nsc.query("test1", ro)
    except:
      self.assertTrue(True);
    else:
      self.assertTrue(False);
    ro = rtidb.ReadOption()
    ro.index.update({"id":"2001"})
    resp = self.nsc.query("test1", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(2001, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
    condition_columns = {"id":"2001"} 
    update_result = self.nsc.delete("test1", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    resp = self.nsc.query("test1", ro)
    self.assertTrue(True);
    self.assertEqual(0, resp.count())
    # multi index
    data = {"id":"2001","name":"n1","mcc":"2001","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    data = {"id":"2002","name":"n2","mcc":"2001","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    self.assertEqual(data["image"], b"i1")

    ro = rtidb.ReadOption()
    ro.index.update({"id":"2001"})
    ro.index.update({"name":"n1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(2001, l["id"])
      self.assertEqual("n1", l["name"])
      self.assertEqual(2001, l["mcc"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual(b"i1", l["image"].getData())
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"2001"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(2, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(2001 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      self.assertEqual(2001, l["mcc"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual(b"i1", l["image"].getData())
      id += 1;
    self.assertEqual(2, id);
    # delete
    condition_columns = {"id":"2001", "name":"n1"} 
    update_result = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"id":"2001"})
    ro.index.update({"name":"n1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertTrue(True);
    self.assertEqual(0, resp.count())
    data = {"id":"2001","name":"n1","mcc":"2001","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    condition_columns = {"mcc":"2001"} 
    update_result = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(2, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"2001"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertTrue(True);
    self.assertEqual(0, resp.count())
    
  def test_traverse(self):
    for i in range(1000) :
        data = {"id":"{:d}".format(i), "attribute":"a{}".format(i), "image":"i{}".format(i)}
        self.assertTrue(self.nsc.put("test1", data, None).success())
    ro = rtidb.ReadOption()
    resp = self.nsc.traverse("test1", ro)
    id = 0;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("i{}".format(id), l["image"])
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);

    ro = rtidb.ReadOption()
    ro.index.update({"id":"100"})
    resp = self.nsc.traverse("test1", ro)
    id = 100;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("i{}".format(id), l["image"])
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);

    # batch query
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

    # multi index
    for i in range(1000) :
        data = {"id":"{:d}".format(i), "name":"n{}".format(i), "mcc":"{:d}".format(i), 
            "attribute":"a{}".format(i), "image":"i{}".format(i).encode("UTF-8")}
        self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    ro = rtidb.ReadOption()
    resp = self.nsc.traverse("rt_ck", ro)
    id = 0;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("n{}".format(id), l["name"])
      self.assertEqual(id, l["mcc"])
      self.assertEqual("i{}".format(id).encode("UTF-8"), l["image"].getData())
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);

    ro = rtidb.ReadOption()
    ro.index.update({"id":"100"})
    ro.index.update({"name":"n100"})
    resp = self.nsc.traverse("rt_ck", ro)
    id = 100;
    for l in resp:
      self.assertEqual(id, l["id"])
      self.assertEqual("n{}".format(id), l["name"])
      self.assertEqual(id, l["mcc"])
      self.assertEqual("i{}".format(id).encode("UTF-8"), l["image"].getData())
      self.assertEqual("a{}".format(id), l["attribute"])
      id+=1
    self.assertEqual(1000, id);

  def test_batchQuery(self):
    # multi index
    data = {"id":"1","name":"n1","mcc":"1","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    data = {"id":"2","name":"n2","mcc":"2","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    data = {"id":"3","name":"n3","mcc":"2","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
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
      self.assertEqual("i1".encode("UTF-8"), l["image"].getData())
      id += 1;
    condition_columns = {"id": 1, "name":"n1"} 
    update_result = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    condition_columns = {"id": 2, "name":"n2"} 
    update_result = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    condition_columns = {"id": 3, "name":"n3"} 
    update_result = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())

  def test_update(self):
    data = {"id":"3001","attribute":"a1", "image":"i1"}
    self.assertTrue(self.nsc.put("test1", data, None).success())
    condition_columns = {"id":"3001"} 
    value_columns = {"attribute":"a3","image":"i3"}
    update_result = self.nsc.update("test1", condition_columns, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"id":"3001"})
    resp = self.nsc.query("test1", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(3001, l["id"])
      self.assertEqual("a3", l["attribute"])
      self.assertEqual("i3", l["image"])
    # update empty
    condition_columns_2 = {"id":-1} 
    value_columns = {"attribute":"a3","image":"i3".encode("UTF-8")}
    update_result = self.nsc.update("test1", condition_columns_2, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(0, update_result.affected_count())
    # update error 
    condition_columns = {"image":"i1"} 
    value_columns = {"attribute":"a3","image":"i3"}
    try:
      update_result = self.nsc.update("test1", condition_columns, value_columns, None);
    except:
      self.assertTrue(True)
    else:
      self.assertTrue(False)
    # multi index
    data = {"id":"3001","name":"n1","mcc":"3001","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    data = {"id":"3002","name":"n2","mcc":"3001","attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    condition_columns = {"id":"3001", "name":"n1"} 
    value_columns = {"attribute":"a2","image":b"i2"}
    update_result = self.nsc.update("rt_ck", condition_columns, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"id":"3001"})
    ro.index.update({"name":"n1"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(3001, l["id"])
      self.assertEqual("n1", l["name"])
      self.assertEqual(3001, l["mcc"])
      self.assertEqual("a2", l["attribute"])
      self.assertEqual("i2".encode("UTF-8"), l["image"].getData())

    # update empty
    condition_columns_2 = {"mcc":-1} 
    value_columns = {"attribute":"a3","image":"i3".encode("UTF-8")}
    update_result = self.nsc.update("rt_ck", condition_columns_2, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(0, update_result.affected_count())
    # update error 
    condition_columns = {"image":b"i1"} 
    value_columns = {"attribute":"a3","image":"i3".encode("UTF-8")}
    try:
      update_result = self.nsc.update("rt_ck", condition_columns, value_columns, None);
    except:
      self.assertTrue(True)
    else:
      self.assertTrue(False)

    condition_columns = {"mcc":"3001"} 
    value_columns = {"attribute":"a3","image":"i3".encode("UTF-8")}
    update_result = self.nsc.update("rt_ck", condition_columns, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(2, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":"3001"})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(2, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(3001 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      self.assertEqual(3001, l["mcc"])
      self.assertEqual("a3", l["attribute"])
      self.assertEqual("i3".encode("UTF-8"), l["image"].getData())
      id += 1;
    self.assertEqual(2, id);

  def test_auto_gen(self):
    data = {"id":4001, "attribute":"a1", "image":"i1"}
    try:
      self.nsc.put("auto", data, None);
    except:
      self.assertTrue(True)
    else:
      self.assertTrue(False)
    data = {"attribute":"a1", "image":"i1"}
    put_result = self.nsc.put("auto", data, None);
    self.assertTrue(put_result.success())
    ro = rtidb.ReadOption()
    resp = self.nsc.traverse("auto", ro)
    id = 0;
    for l in resp:
      self.assertEqual(put_result.get_auto_gen_pk(), l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      id += 1;
    self.assertEqual(1, id);

  def test_date_index(self):
    data = {"id":"5001","attribute":"a1", "image":"i1", "male":True, "date":date(2020,1,1), "ts":1588756531}
    self.assertTrue(self.nsc.put("date", data, None).success())
    ro = rtidb.ReadOption()
    ro.index.update({"date":date(2020,1,1)})
    resp = self.nsc.query("date", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(5001, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      self.assertEqual(True, l["male"])
      self.assertEqual(date(2020,1,1), l["date"])
      self.assertEqual(1588756531, l["ts"])
    ro = rtidb.ReadOption()
    ro.index.update({"male":True})
    ro.index.update({"ts":1588756531})
    resp = self.nsc.query("date", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(5001, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      self.assertEqual(True, l["male"])
      self.assertEqual(date(2020,1,1), l["date"])
      self.assertEqual(1588756531, l["ts"])
    # update
    condition_columns = {"date":date(2020,1,1)} 
    value_columns = {"male":False,"ts":"1588756532"}
    update_result = self.nsc.update("date", condition_columns, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"date":date(2020,1,1)})
    resp = self.nsc.query("date", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(5001, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      self.assertEqual(False, l["male"])
      self.assertEqual(date(2020,1,1), l["date"])
      self.assertEqual(1588756532, l["ts"])
    condition_columns = {"male":False,"ts":"1588756532"} 
    value_columns = {"male":True}
    update_result = self.nsc.update("date", condition_columns, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"male":True,"ts":"1588756532"})
    resp = self.nsc.query("date", ro)
    self.assertEqual(1, resp.count())
    for l in resp:
      self.assertEqual(5001, l["id"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual("i1", l["image"])
      self.assertEqual(True, l["male"])
      self.assertEqual(date(2020,1,1), l["date"])
      self.assertEqual(1588756532, l["ts"])
    # delete empty
    condition_columns = {"date":date(2021,1,1)} 
    update_result = self.nsc.delete("date", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(0, update_result.affected_count())
    # delete error 
    condition_columns = {"ts": 123} 
    try:
      update_result = self.nsc.delete("date", condition_columns);
    except:
      self.assertTrue(True)
    else:
      self.assertTrue(False)
    # delete
    condition_columns = {"date":date(2020,1,1)} 
    update_result = self.nsc.delete("date", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    resp = self.nsc.query("date", ro)
    self.assertTrue(True);
    self.assertEqual(0, resp.count())
    
  def test_index_null(self):
    data = {"id": 6001,"name":"n1","mcc": None,"attribute":"a1", "image":b"i1"}
    self.assertTrue(self.nsc.put("rt_ck", data, None).success())
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":None})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(1, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(6001 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      self.assertEqual(None, l["mcc"])
      self.assertEqual("a1", l["attribute"])
      self.assertEqual(b"i1", l["image"].getData())
      id += 1;
    self.assertEqual(1, id);
    # update
    condition_columns = {"mcc":None} 
    value_columns = {"attribute":"a2"}
    update_result = self.nsc.update("rt_ck", condition_columns, value_columns, None);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":None})
    resp = self.nsc.query("rt_ck", ro)
    self.assertEqual(1, resp.count())
    id = 0;
    for l in resp:
      self.assertEqual(6001 + id, l["id"])
      self.assertEqual("n{}".format(id+1), l["name"])
      self.assertEqual(None, l["mcc"])
      self.assertEqual("a2", l["attribute"])
      self.assertEqual(b"i1", l["image"].getData())
      id += 1;
    self.assertEqual(1, id);
    # delete
    condition_columns = {"mcc":None} 
    update_result = self.nsc.delete("rt_ck", condition_columns);
    self.assertEqual(True, update_result.success())
    self.assertEqual(1, update_result.affected_count())
    ro = rtidb.ReadOption()
    ro.index.update({"mcc":None})
    resp = self.nsc.query("rt_ck", ro)
    self.assertTrue(True);
    self.assertEqual(0, resp.count())

if __name__ == "__main__":
  unittest.main()
