import enum
from . import interclient
from typing import List

class CompareOP(enum.IntEnum):
  EQ = enum.auto()
  LT = enum.auto()
  LE = enum.auto()
  GT = enum.auto()
  GE = enum.auto()

def return_None(x):
  return None

def return_EmptyStr(x):
  return str()

type_map = {1:bool,2:int,3:int,4:int,5:float,6:float,7:str,8:int,9:int,100:return_None};
# todo: current do not have blob type process function
'''
  kBool = 1,
  kSmallInt = 2,
  kInt = 3,
  kBigInt = 4,
  kFloat = 5,
  kDouble = 6,
  kVarchar = 7,
  kDate = 8,
  kTimestamp = 9,
  kBlob = 10
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
    self.index = {}
    self.read_filter = []
    self.col_set = set()

class RtidbResult:
  def __init__(self, data):
    self.__data = data
    self.__type_to_func = {1:self.__data.GetBool, 
      2:self.__data.GetInt16, 3:self.__data.GetInt32, 
      4:self.__data.GetInt64, 5:self.__data.GetFloat, 
      6:self.__data.GetDouble, 7:self.__data.GetString,
      8:return_None, 9:return_None, 100:return_None}
    names = self.__data.GetColumnsName()
    self.__names = [x for x in names]
  def __iter__(self):
    return self
  def count(self):
    if hasattr(self.__data, "ValueSize"):
      return self.__data.ValueSize()
    else:
      raise Exception(-1, "result not support count")
  def __next__(self):
    if self.__data.Next():
      result = {}
      for idx in range(len(self.__names)):
        type = self.__data.GetColumnType(idx)
        if self.__data.IsNULL(idx):
          result.update({self.__names[idx]: None})
        else:
          result.update({self.__names[idx]: self.__type_to_func[type](idx)})
      return result
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
    value = {}
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
    cond = {}
    for k in condition_columns:
      cond.update({k: str(condition_columns[k])})
    v = {}
    for k in value_columns:
      v.update({k: str(value_columns[k])})
    ok = self.__client.Update(table_name, cond, v, _wo)
    if ok.code != 0:
      raise Exception(ok.code, ok.msg)
    return True
  
  def query(self, table_name: str, read_option: ReadOption):
    if (len(read_option.index) < 1):
      raise Exception("must set index")
    mid_map = {}
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
    if (len(read_options) < 1):
      raise Exception("muse set read_options")
    ros = interclient.VectorReadOption()
    for ro in read_options:
      mid_map = {}
      for k in ro.index:
        mid_map.update({k: str(ro.index[k])})
      interro = interclient.ReadOption(mid_map)
      for filter in ro.read_filter:
        mid_rf = interclient.ReadFilter()
        mid_rf.column = filter.name
        mid_rf.type = filter.type
        mid_rf.value = str(filter.value)
        interro.read_filter.append(mid_rf)
      for col in ro.col_set:
        interro.col_set.append(col)
      ros.append(interro)
    resp = self.__client.BatchQuery(table_name, ros);
    if (resp.code_ != 0):
      raise Exception(resp.code_, resp.msg_);
    return RtidbResult(resp)

  def delete(self, table_name: str, condition_columns: map):
    if (len(condition_columns) != 1):
      raise Exception("keys size not 1")
    v = {}
    for k in condition_columns:
      v.update({k:str(condition_columns[k])})
    resp = self.__client.Delete(table_name, v)
    if resp.code != 0:
      raise Exception(resp.code, resp.msg)
    return True

  def traverse(self, table_name: str, read_option: ReadOption = None):
    mid_map = {}
    ro = interclient.ReadOption(mid_map)
    if read_option != None:
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
    resp = self.__client.Traverse(table_name, ro)
    return RtidbResult(resp)
