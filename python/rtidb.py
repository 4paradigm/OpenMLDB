import enum
from . import interclient
from . import interclient_tools
from typing import List
from datetime import date
import io

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

NONETOKEN="!N@U#L$L%"

storageModSet = set(["ssd", "hdd"])
dataTypeSet = set(["bool", "smallint", "int", "bigint", "float", "double", "timestamp", "date", "varchar", "string", "blob"])
IndexTypeSet = set(["unique", "nounique", "primarykey", "autogen", "increment"])
tableTypeSet = set(["Relational", "objectstore"])

class Column:
  def __init__(self, name: str, dataType: str, notNull: bool):
    self.name = name
    self.type = dataType
    self.not_null = notNull
  def SerializeToString(self):
    output = io.StringIO()
    output.write("column_desc {\n")
    output.write("\tname : \"{}\"\n".format(self.name))
    output.write("\ttype : \"{}\"\n".format(self.type))
    output.write("\tnot_null : {}\n".format(str(self.not_null).lower()))
    output.write("}\n")
    return output.getvalue()

class Idx:
  def __init__(self, name: str, cols: List[str], idxType: str):
    self.name = name
    self.cols = []
    colSet = set()
    for col in cols:
      if col in colSet:
        continue
      self.cols.append(col)
      colSet.add(col)
    self.idx_type = idxType
  def SerializeToString(self):
    output = io.StringIO()
    output.write("index {\n")
    output.write("\tindex_name : \"{}\"\n".format(self.name))
    for col in self.cols:
      output.write("\tcol_name : \"{}\"\n".format(col))
    output.write("\tindex_type : \"{}\"\n".format(self.idx_type))
    output.write("}\n")
    return output.getvalue()

class tableBuilder:
  def __init__(self, name: str, storageMode: str, tableType: str, replicaNum: int = 1, partNum: int = 1):
    self.name = name
    if storageMode not in storageModSet:
      raise Exception("unkown storagemode, support mode is {}".format(storageModSet))
    if tableType not in tableTypeSet:
      raise Exception("unkown table type, support table type is {}".format(tableType))
    self.sm = storageMode.lower()
    self.tableType = tableType
    self.replicaNum = replicaNum
    self.partNum = partNum
    self.columns = []
    self.idxs = []
    self.colSet = set()
    self.idxSet = set()
    self.hasPrimary = False

  def addCol(self, name: str, dataType: str, notNull: bool):
    name = name
    if name in self.colSet:
      raise Exception("duplicate col name {}".format(name))
    if not dataType in dataTypeSet:
      raise Exception("unkown data type {}, support data type is {}".format(name, dataTypeSet))
    col = Column(name, dataType, notNull)
    self.columns.append(col)
    self.colSet.add(name)

    return self

  def addIdx(self, name: str, cols: List[str], idxType: str):
    idxType = idxType.lower()
    if self.hasPrimary and idxType == "primarykey":
      raise Exception("already have primary idx")
    if name in self.idxSet:
      raise Exception("duplicate idx name{}".format(name))
    if not idxType in IndexTypeSet:
      raise Exception("unkown idx type {}, support idx type is {}".format(idxType, IndexTypeSet))
    newCols = []
    colSet = set()
    for i in cols:
      if i in colSet:
        continue
      newCols.append(i)
      colSet.add(i)
    idx = Idx(name, newCols, idxType)
    self.idxs.append(idx)
    if idxType == "primarykey":
      self.hasPrimary = True
    self.idxSet.add(name)

    return self
    
  def SerializeToString(self):
    output = io.StringIO()
    output.write("name : \"{}\"\n".format(self.name))
    output.write("table_type : \"{}\"\n".format(self.tableType))
    output.write("storage_mode : \"{}\"\n".format(self.sm))
    for col in self.columns:
      output.write(col.SerializeToString())
    for idx in self.idxs:
      output.write(idx.SerializeToString())
    return output.getvalue()

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
    elif isinstance(m[k], str):
      mid_map.update({k: m[k]})
    elif isinstance(m[k], bytes):
      continue
    else:
      mid_map.update({k: str(m[k])})
  return mid_map

class WriteOption:
  def __init__(self, updateIfExist = False):
    self.updateIfExist = updateIfExist
    
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

class BlobData:
  def __init__(self, name, info, key):
    self._name = name
    self._info = info
    self._key = key

  def getKey(self):
    return self._key

  def getUrl(self):
    blobUrl = "/v1/get/{}/{}".format(self._name, self._key)
    return blobUrl

  def getData(self):
    blobOPResult = interclient.BlobOPResult()
    self._info.key_ = self._key
    data = interclient_tools.GetBlob(self._info, blobOPResult)
    if blobOPResult.code_ != 0:
      raise Exception("erred at get blob data {}".format(blobOPResult.msg_))
    return data

class UpdateResult:
  def __init__(self, data):
    self.__data = data
    self.__success = True if data.code == 0 else False
    self.__affected_count = data.affected_count
  def success(self):
    return self.__success
  def affected_count(self):
    return self.__affected_count

class RtidbResult:
  def __init__(self, table_name, data):
    self.__data = data
    self.__type_to_func = {1:self.__data.GetBool, 
      2:self.__data.GetInt16, 3:self.__data.GetInt32, 
      4:self.__data.GetInt64, 5:self.__data.GetFloat, 
      6:self.__data.GetDouble, 7:self.__data.GetDate,
      8:self.__data.GetTimestamp,13: self.__data.GetString,
      14: self.__data.GetString, 15:self.__data.GetBlob}
    names = self.__data.GetColumnsName()
    self.__names = [x for x in names]
    self._table_name = table_name
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
            blobKey = self.__type_to_func[type](idx)
            blobInfoResult = self.__data.GetBlobInfo()
            if blobInfoResult.code_ != 0:
              msg = blobInfoResult.GetMsg()
              raise Exception("erred at get blob server: {}".format(msg.decode("UTF-8")))
            blobData = BlobData(self._table_name, blobInfoResult, blobKey)
            result.update({self.__names[idx] : blobData})
          else:
            result.update({self.__names[idx]: self.__type_to_func[type](idx)})
      return result
    else:
      raise StopIteration

ReadOptions = List[ReadOption]
defaultWriteOption = WriteOption()

class RTIDBClient:
  def __init__(self, zk_cluster: str, zk_path: str):
    client = interclient.RtidbClient()
    ok = client.Init(zk_cluster, zk_path)
    if ok.code != 0:
      raise Exception(ok.code, ok.msg)
    self.__client = client

  def putBlob(self, name: str, value: map):
    blobFields = self.__client.GetBlobSchema(name);
    blobInfo = None
    blobKeys = {}
    for k in blobFields:
      blobData = value.get(k, None)
      if blobData == None:
        continue
      if not isinstance(blobData, bytes):
        raise Exception("blob data only byte type")
      if blobInfo == None:
        blobInfo = self.__client.GetBlobInfo(name)
        if blobInfo.code_ != 0:
          msg = blobInfo.GetMsg()
          raise Exception("erred at get blobinfo: {}".format(msg.decode("UTF-8")))
      blobOPResult = interclient.BlobOPResult()
      ok = interclient_tools.PutBlob(blobInfo, blobOPResult, blobData, len(blobData))
      if not ok:
        raise Exception("erred at put blob data: {}".format(blobOPResult.msg_))
      blobKeys.update({k: str(blobInfo.key_)})
    return blobKeys

  def deleteBlob(self, name: str, value: map):
    blobInfo = None
    keys = interclient.VectorInt64()
    for k in value:
      if blobInfo == None:
        blobInfo = self.__client.GetBlobInfo(name)
        if blobInfo.code_ != 0:
          msg = blobInfo.GetMsg()
          raise Exception("erred at get blobinfo: {}".format(msg.decode("UTF-8")))
      keys.append(int(value[k]))
    self.__client.DeleteBlobs(name, keys)

  def put(self, table_name: str, columns: map, write_option: WriteOption = None):
    wo = interclient.WriteOption()
    if write_option == None:
      wo.update_if_exist = defaultWriteOption.updateIfExist
    else:
      wo.update_if_exist = write_option.updateIfExist
    blobKeys = self.putBlob(table_name, columns)
    value = buildStrMap(columns)

    value.update(blobKeys)

    putResult= self.__client.Put(table_name, value, wo)
    if putResult.code != 0:
      self.deleteBlob(table_name, blobKeys)
      raise Exception(putResult.code, putResult.msg)
    return PutResult(putResult)

  def update(self, table_name: str, condition_columns: map, value_columns: map, write_option: WriteOption = None):
    _wo = interclient.WriteOption()
    if write_option != None:
      _wo.updateIfExist = defaultWriteOption.updateIfExist
    blobKeys = self.putBlob(table_name, value_columns)
    cond = buildStrMap(condition_columns)
    v = buildStrMap(value_columns)
    v.update(blobKeys)
    update_result = self.__client.Update(table_name, cond, v, _wo)
    if update_result.code != 0:
      self.deleteBlob(table_name, blobKeys)
      raise Exception(update_result.code, update_result.msg)
    return UpdateResult(update_result)

  def __buildReadoption(self, read_option: ReadOption):
    mid_map = buildStrMap(read_option.index)
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
    return RtidbResult(table_name, resp)

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
    return RtidbResult(table_name, resp)

  def delete(self, table_name: str, condition_columns: map):
    v = buildStrMap(condition_columns)
    resp = self.__client.Delete(table_name, v)
    if resp.code != 0:
      raise Exception(resp.code, resp.msg)
    return UpdateResult(resp); 

  def traverse(self, table_name: str, read_option: ReadOption = None):
    if read_option != None:
      ro = self.__buildReadoption(read_option)
    else:
      ro = interclient.ReadOption({})
    resp = self.__client.Traverse(table_name, ro)
    if (resp.code_ != 0):
      raise Exception(resp.code_, resp.msg_)
    return RtidbResult(table_name, resp)

  def createTable(self, table_info: tableBuilder):
    ret = self.__client.CreateTable(table_info.SerializeToString())
    if ret != 0:
        if ret == -2:
            raise Exception(ret, "table is exist")
        else:
            raise Exception(ret, "create table fail")
    return True

  def showTable(self, table_name: str = ""):
    names = self.__client.ShowTable(table_name);
    result = []
    for n in names:
        if len(n) == 0:
            continue
        result.append(n)
    return result

  def dropTable(self, table_name: str):
    if len(table_name) == 0: return
    self.__client.DropTable(table_name)
