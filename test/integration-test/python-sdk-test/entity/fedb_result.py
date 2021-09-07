class FedbResult():
    def __init__(self):
        self.ok = None
        self.count = None
        self.result = None
        self.resultSchema = None
        self.msg = None
        self.rs = None

    def __str__(self):
        resultStr = "FesqlResult{ok=" + str(self.ok) + ", count=" + str(self.count) + ", msg=" + str(self.msg) + "}"
        if self.result is not None:
            resultStr += "result=" + str(len(self.result)) + ":\n"
            columnName = "i\t";
            cols = self.rs._cursor_description()
            for col in cols:
                columnName += "{}\t".format(col[0])
            resultStr += columnName + "\n"
            for index, value in enumerate(self.result):
                lineStr = str(index + 1)
                for v in value:
                    lineStr += '\t' + str(v)
                resultStr += lineStr + "\n"
        return resultStr
