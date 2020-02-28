kSubkeyLt = 2
kSubkeyLe = 3
kSubKeyEq = 1
kSubKeyGt = 4
kSubKeyGe = 5

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
class RTIDBCliet:
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
      _wo.updateIfExist = write_option.updateIfExist
      _wo.updateIfEqual = write_option.updateIfEqual
    value = dict();
    for k in columns:
      value.update({k, str(columns[k])})
    return self.__client.Put(table_name, value, _wo)

  def update(self, table_name: str, condition_columns: map, value_columns: map, write_option: WriteOption = None):
    pass

  def get(self, table_name: str, read_option: ReadOption):
    if (read_option.index.size() < 1):
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
    return self.__client.Get(table_name, ro)

  def batch_get(self, table_name: str, read_options: ReadOptions):
    pass

  def delete(self, table_name: str, condition_columns: map):
    pass

  def traverse(self, table_name: str):
    pass
