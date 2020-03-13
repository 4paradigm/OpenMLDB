import enum
from . import interclient
from typing import List

class CompareOP(enum.IntEnum):
  EQ = enum.auto()
  LT = enum.auto()
  LE = enum.auto()
  GT = enum.auto()
  GE = enum.auto()

def __return_None(x):
  return None

def __return_EmptyStr(x):
  return str()

type_map = {1:bool,2:int,3:int,4:int,5:float,6:float,7:str,8:int,9:int,100:__return_None};
'''
  kBool = 1,
  kInt16 = 2,
  kInt32 = 3,
  kInt64 = 4,
  kFloat = 5,
  kDouble = 6,
  kVarchar = 7,
  kDate = 8,
  kTimestamp = 9,
  kVoid = 100
'''

NONETOKEN="None#*@!"

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

class RtidbResult:
  def __init__(self, data):
    self.__data = data
  def __iter__(self):
    return self
  def count(self):
    return self.__data.ValueSize()
  def __next__(self):
    if self.__data.next():
      return self.__data.DecodeData()
    else:
      raise StopIteration

ReadOptions = List[ReadOption]
defaultWriteOption = WriteOption()

class RTIDBClient:
  def __init__(self, zk_cluster: str, zk_path: str):
    self.__client = interclient.RtidbClient()
    ok = self.__client.Init(zk_cluster, zk_path)
    if ok.code != 0:
      raise Exception(ok.code, ok.msg)

  def __del__(self):
    del self.__client

  def put(self, table_name: str, columns: map, write_option: WriteOption = None):
    _wo = interclient.WriteOption();
    if WriteOption != None:
      _wo.updateIfExist = defaultWriteOption.updateIfExist
      _wo.updateIfEqual = defaultWriteOption.updateIfEqual
    value = dict();
    for k in columns:
      if columns[k] == None:
        value.update({k: NONETOKEN})
      value.update({k: str(columns[k])})
    ok = self.__client.Put(table_name, value, _wo)
    if ok.code != 0:
      raise Exception(ok.code, ok.msg)
    return True

  def update(self, table_name: str, condition_columns: map, value_columns: map, write_option: WriteOption = None):
    _wo = interclient.WriteOption();
    if write_option != None:
      _wo.updateIfExist = defaultWriteOption.updateIfExist
      _wo.updateIfEqual = defaultWriteOption.updateIfEqual
    cond = dict()
    for k in condition_columns:
      cond.update({k: str(condition_columns[k])})
    v = dict()
    for k in value_columns:
      v.update({k: str(value_columns[k])})
    return self.__client.Update(table_name, cond, v, _wo)
  
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
    resp = self.__client.Query(table_name, ro)
    if resp.code_ != 0:
      raise Exception(resp.code_, resp.msg_)
    return RtidbResult(resp)

  def batch_query(self, table_name: str, read_options: ReadOptions):
    return RtidbResult(interclient.QueryResult())

  def delete(self, table_name: str, condition_columns: map):
    if (len(condition_columns) != 1):
      raise Exception("keys size not 1")
    v = dict()
    for k in condition_columns:
      v.update({k:str(condition_columns[k])})
    resp = self.__client.Delete(table_name, v)
    if resp.code != 0:
      raise Exception(resp.code, resp.msg)
    return true

  def traverse(self, table_name: str):
    return RtidbResult(interclient.QueryResult())
