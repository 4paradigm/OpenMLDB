kSubkeyLt = 2
kSubkeyLe = 3
kSubKeyEq = 1
kSubKeyGt = 4
kSubKeyGe = 5
def __return_None(x):
  return None
def __return_EmptyStr(x):
  return str()
type_map = {0:str,1:float,2:int,3:int,4:float,5:__return_None,6:int,7:int,8:int,9:str,
10:int,11:int,12:bool,100:str(),200:__return_None}
'''
    kString = 0,
    kFloat = 1,
    kInt32 = 2,
    kInt64 = 3,
    kDouble = 4,
    kNull = 5,
    kUInt32 = 6,
    kUInt64 = 7,
    kTimestamp = 8,
    kDate = 9,
    kInt16 = 10,
    kUInt16 = 11,
    kBool = 12,
    kEmptyString = 100,
    kUnknown = 200
'''

class WriteOption:
  def __init__(self, updateIfExist = True, updateIfEqual = True):
    self.updateIfExist = updateIfExist
    self.updateIfEqual = updateIfEqual
    
class ReadFilter:
  def __init__(self, column, compare: int, value):
    self.name = column
    self.type = compare
    self.value = value

class ReadOption:
  def __init__(self):
    self.index = dict()
    self.read_filter = []
    self.col_set = set()

import interclient

from typing import List
ReadOptions = List[ReadOption]
defaultWriteOption = WriteOption()
class RTIDBClient:
  def __init__(self, zk_cluster: str, zk_path: str):
    self.__client = interclient.RtidbNSClient()
    ok = self.__client.Init(zk_cluster, zk_path)
    if not ok:
      raise Exception("Initial client error!")

  def __del__(self):
    del self.__client

  def put(self, table_name: str, columns: map, write_option: WriteOption = None):
    _wo = interclient.WriteOption();
    if WriteOption != None:
      _wo.updateIfExist = defaultWriteOption.updateIfExist
      _wo.updateIfEqual = defaultWriteOption.updateIfEqual
    value = dict();
    for k in columns:
      value.update({k: str(columns[k])})
    return self.__client.Put(table_name, value, _wo)

  def update(self, table_name: str, condition_columns: map, value_columns: map, write_option: WriteOption = None):
    pass

  def query(self, table_name: str, read_option: ReadOption):
    if (len(read_option.index) < 1):
      raise Exception("must set index")
    mid_map = dict()
    for k in read_option.index:
      mid_map.update({k: str(read_option.index[k])})
    ro = interclient.ReadOption(mid_map)
    for filter in read_option.read_filter:
      mid_rf = interclient.ReadFilter()
      mid_rf.column = filter.name
      mid_rf.type = filter.type
      mid_rf.value = str(filter.value)
      ro.read_filter.append(mid_rf)
    for col in read_option.col_set:
      ro.col_set.append(col)
    resp = self.__client.Get(table_name, ro)
    result = dict()
    for k in resp:
      if k == None:
        continue
      print(type_map[resp[k].type](resp[k].buffer))
      result.update({k: type_map[resp[k].type](resp[k].buffer)})
    return result

  def batch_get(self, table_name: str, read_options: ReadOptions):
    pass

  def delete(self, table_name: str, condition_columns: map):
    if (condition_columns.size < 1):
      raise Exception("empty map")
    v = dict()
    for k in condition_columns:
      v.update({k:str(condition_columns[k])})
    return self.__client.Delete(table_name, v)

  def traverse(self, table_name: str):
    pass
