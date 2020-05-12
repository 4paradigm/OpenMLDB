import enum
from . import interclient
from typing import List
from datetime import date

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

type_map = {1:bool,2:int,3:int,4:int,5:float,6:float,7:date,8:int,13:str,14:str,100:return_None};
# todo: current do not have blob type process function
'''
kBool = 1;
kSmallInt = 2;
kInt = 3;
kBigInt = 4;
kFloat = 5;
kDouble = 6;
kDate = 7;
kTimestamp = 8;
// reserve 9, 10, 11, 12
kVarchar = 13;
kString = 14;
kBlob = 15;
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

class PutResult:
  def __init__(self, data):
    self.__data = data;
    self.__success = True if data.code == 0 else False;
    self.__auto_gen_pk = data.auto_gen_pk;
  def success(self):
    return self.__success;
  def get_auto_gen_pk(self):
    if not self.__data.has_auto_gen_pk:
      raise Exception(-1, "don't have auto_gen_pk");    
    else:
      return self.__data.auto_gen_pk;

class RtidbResult:
  def __init__(self, data):
    self.__data = data
    self.__type_to_func = {1:self.__data.GetBool, 
      2:self.__data.GetInt16, 3:self.__data.GetInt32, 
      4:self.__data.GetInt64, 5:self.__data.GetFloat, 
      6:self.__data.GetDouble, 7:self.__data.GetDate,
      8:self.__data.GetTimestamp,13:self.__data.GetString,
      14:self.__data.GetString, 100:return_None}
    names = self.__data.GetColumnsName()
    self.__names = [x for x in names]
  def __iter__(self):
    return self
  def count(self):
    if hasattr(self.__data, "Count"):
      return self.__data.Count()
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
          if type == 7: 
            value = self.__type_to_func[type](idx)
            day = value & 0x0000000FF;
            value = value >> 8;
            month = 1 + (value & 0x0000FF);
            year = 1900 + (value >> 8);
            real_date = date(year, month, day)
            result.update({self.__names[idx]: real_date})
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
    general_result = self.__client.Put(table_name, value, _wo)
    if general_result.code != 0:
      raise Exception(general_result.code, general_result.msg)
    return PutResult(general_result); 

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
    ros = interclient.VectorReadOption()
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
    ros.append(ro)
    resp = self.__client.BatchQuery(table_name, ros)
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
    if (resp.code_ != 0):
      raise Exception(resp.code_, resp.msg_);
    return RtidbResult(resp)
