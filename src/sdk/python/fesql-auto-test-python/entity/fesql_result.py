#! /usr/bin/env python
# -*- coding: utf-8 -*-

class FesqlResult():
    def __init__(self):
        self.ok = None
        self.count = None
        self.result = None
        self.resultSchema = None
        self.msg = None
        self.rs = None

    def __str__(self):
        resultStr = "FesqlResult{ok=" + str(self.ok) + ", count=" + str(self.count) + ", msg="+ str(self.msg) +"}"
        if(self.result != None):
            resultStr += "result="+str(len(self.result))+":\n"
            columnCount = self.resultSchema.GetColumnCnt()
            columnName = "i\t";
            for i in range(columnCount):
                columnName+=self.resultSchema.GetColumnName(i)+"\t";
            resultStr += columnName+"\n"
            for index,value in enumerate(self.result):
                lineStr = str(index+1)
                for v in value:
                    lineStr+='\t'+str(v)
                resultStr+=lineStr+"\n"
        return resultStr
