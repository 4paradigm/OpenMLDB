import enum
from . import interclient
from . import interclient_tools
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

type_map = {1:bool,2:int,3:int,4:int,5:float,6:float,7:date,8:int,13:str,14:str,15:int};
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

def buildReadFilter(filter):
  mid_rf = interclient.ReadFilter()
  mid_rf.column = filter.name
  mid_rf.type = filter.type
  mid_rf.value = filter.value
  return mid_rf

def buildStrMap(m: map):
  mid_map = {}
  for k in m:
    if m[k] == None:
      mid_map.update({k: NONETOKEN})
    if isinstance(m[k], str):
      mid_map.update({k: m[k]})
    else:
      mid_map.update({k: str(m[k])})
  return  mid_map

def buildNoNoneStrMap(m: map):
  mid_map = {}
  for k in m:
    if m[k] == None:
        raise Exception("{} value is None, don't allow".format(k))
    if isinstance(m[k], str):
      mid_map.update({k: m[k]})
    else:
      mid_map.update({k: str(m[k])})
  return  mid_map


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
      8:self.__data.GetTimestamp,13: self.__data.GetString,
      14: self.__data.GetString, 15:self.__data.GetBlob}
    names = self.__data.GetColumnsName()
    self.__names = [x for x in names]
    self.__blobInfo = None
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
            day = value & 0x0000000FF
            value = value >> 8
            month = 1 + (value & 0x0000FF)
            year = 1900 + (value >> 8)
            real_date = date(year, month, day)
            result.update({self.__names[idx]: real_date})
          elif type == 15:
            blob_key = self.__type_to_func[type](idx)
            if self.__blobInfo == None:
              blobInfoResult = self.__data.GetBlobInfo()
              if blobInfoResult.code_ != 0:
                raise Exception("erred at get blob server: {}".format(blobInfoResult.msg_))
              self.__blobInfo = blobInfoResult
            self.__blobInfo.key_ = blob_key
            blobOPResult = interclient.BlobOPResult()
            blob_data = interclient_tools.GetBlob(self.__blobInfo, blobOPResult)
            if blobOPResult.code_ != 0:
              raise Exception("erred at get blob data {}".format(blobOPResult.msg_))
            result.update({self.__names[idx]: blob_data})
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

  def putBlob(self, name: str, value: map):
    blobFields = self.__client.GetBlobSchema(name);
    blobInfo = None
    for k in blobFields:
      blobData = value.get(k, None)
      if blobData == None:
        continue
      if not isinstance(blobData, bytes):
        raise Exception("blob data only byte type")
      if blobInfo == None:
        blobInfo = self.__client.GetBlobInfo(name)
        if blobInfo.code_ != 0:
          raise Exception("erred at get blobinfo: {}".format(blobInfo.msg_))
      blobOPResult = interclient.BlobOPResult()
      ok = interclient_tools.PutBlob(blobInfo, blobOPResult, blobData, len(blobData))
      if not ok:
        raise Exception("erred at put blob data: {}".format(blobOPResult.msg_))
      value.update({k: str(blobInfo.key_)})

  def put(self, table_name: str, columns: map, write_option: WriteOption = None):
    _wo = interclient.WriteOption()
    if WriteOption != None:
      _wo.updateIfExist = defaultWriteOption.updateIfExist
      _wo.updateIfEqual = defaultWriteOption.updateIfEqual
    self.putBlob(table_name, columns)
    value = buildStrMap(columns)

    putResult= self.__client.Put(table_name, value, _wo)
    if putResult.code != 0:
      raise Exception(putResult.code, putResult.msg)
    return PutResult(putResult)

  def update(self, table_name: str, condition_columns: map, value_columns: map, write_option: WriteOption = None):
    _wo = interclient.WriteOption()
    if write_option != None:
      _wo.updateIfExist = defaultWriteOption.updateIfExist
      _wo.updateIfEqual = defaultWriteOption.updateIfEqual
    self.putBlob(table_name, value_columns)
    cond = buildNoNoneStrMap(condition_columns)
    v = buildStrMap(value_columns)
    ok = self.__client.Update(table_name, cond, v, _wo)
    if ok.code != 0:
      raise Exception(ok.code, ok.msg)
    return True

  def __buildReadoption(self, read_option: ReadOption):
    mid_map = buildNoNoneStrMap(read_option.index)
    ro = interclient.ReadOption(mid_map)
    for filter in read_option.read_filter:
      mid_rf = buildReadFilter(filter)
      ro.read_filter.append(mid_rf)
    for col in read_option.col_set:
      ro.col_set.append(col)
    return ro

  def query(self, table_name: str, read_option: ReadOption):
    if (len(read_option.index) < 1):
      raise Exception("must set index")
    ros = interclient.VectorReadOption()
    ro = self.__buildReadoption(read_option)
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
      interro = self.__buildReadoption(ro)
      ros.append(interro)
    resp = self.__client.BatchQuery(table_name, ros)
    if (resp.code_ != 0):
      raise Exception(resp.code_, resp.msg_)
    return RtidbResult(resp)

  def delete(self, table_name: str, condition_columns: map):
    v = buildNoNoneStrMap(condition_columns)
    resp = self.__client.Delete(table_name, v)
    if resp.code != 0:
      raise Exception(resp.code, resp.msg)
    return True

  def traverse(self, table_name: str, read_option: ReadOption = None):
    if read_option != None:
      ro = self.__buildReadoption(read_option)
    else:
      ro = interclient.ReadOption({})
    resp = self.__client.Traverse(table_name, ro)
    if (resp.code_ != 0):
      raise Exception(resp.code_, resp.msg_)
    return RtidbResult(resp)
