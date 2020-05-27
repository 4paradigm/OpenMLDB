import io

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
  def __init__(self, name: str, cols: [str], idxType: str):
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
    self.sm = storageMode
    self.tableType = tableType
    self.replicaNum = replicaNum
    self.partNum = partNum
    self.columns = []
    self.idxs = []
    self.colSet = set()
    self.idxSet = set()
    self.hasPrimary = False

  def addCol(self, name: str, dataType: str, notNull: bool):
    name = name.lower()
    if name in self.colSet:
      raise Exception("duplicate col name {}".format(name))
    dataType = dataType.lower()
    if not dataType in dataTypeSet:
      raise Exception("unkown data type {}, support data type is {}".format(name, dataTypeSet))
    col = Column(name, dataType, notNull)
    self.columns.append(col)
    self.colSet.add(name)

    return self

  def addIdx(self, name: str, cols: [str], idxType: str):
    name = name.lower()
    cols = [ i.lower() for i in cols]
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
    for col in self.columns:
      output.write(col.SerializeToString())
    for idx in self.idxs:
      output.write(idx.SerializeToString())
    return output.getvalue()

def generate_auto():
  table = "auto"
  b = tableBuilder(table, "hdd", "Relational")
  b.addCol("id", "bigint", True).addCol("attribute", "varchar", True).addCol("image", "varchar", False).addIdx("idx1", ["id"], "AutoGen")
  with open("{}.txt".format(table), "w") as f:
    f.write(b.SerializeToString())

def generate_ck():
  table = "ck"
  b = tableBuilder(table, "hdd", "Relational")
  b.addCol("id", "bigint", True).addCol("name", "varchar", True).addCol("mcc", "int", True).addCol("attribute", "varchar", True).addCol("image", "blob", False).addCol("date", "date", True).addCol("ts", "timestamp", True).addIdx("index_1", ["id","name"], "primaryKey").addIdx("index_2", ["mcc"], "nounique").addIdx("index_3",["date","ts"], "unique")
  with open("{}.txt".format(table), "w") as f:
    f.write(b.SerializeToString())

def generate_date():
  table = "date"
  b = tableBuilder(table, "hdd", "Relational")
  b.addCol("id", "bigint", True).addCol("attribute", "varchar", True).addCol("image", "varchar", False).addCol("male", "bool", False).addCol("date", "date", True).addCol("ts", "timestamp", True).addIdx("idx1", ["date"], "primaryKey").addIdx("idx2", ["male", "ts"], "nounique")
  with open("{}.txt".format(table), "w") as f:
    f.write(b.SerializeToString())

def generate_rt_ck():
  table = "rt_ck"
  b = tableBuilder(table, "hdd", "Relational")
  b.addCol("id", "bigint", True).addCol("name", "varchar", True).addCol("mcc", "int", True).addCol("attribute", "varchar", True).addCol("image", "blob", False).addIdx("index_1", ["id","name"], "primaryKey").addIdx("index_2", "mcc", "nounique")
  with open("{}.txt".format(table), "w") as f:
    f.write(b.SerializeToString())

def generate_test1():
  table = "test1"
  b = tableBuilder(table, "hdd", "Relational")
  b.addCol("id", "bigint", True).addCol("attribute", "varchar", True).addCol("image", "varchar", False).addIdx("id", ["id"], "primaryKey")
  with open("{}.txt".format(table), "w") as f:
    f.write(b.SerializeToString())

if __name__ == "__main__":
  generate_auto()
  generate_ck()
  generate_date()
  generate_rt_ck()
  generate_test1()
