#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class FedbResult():
    def __init__(self):
        self.ok = None
        self.count = None
        self.result = None
        self.resultSchema = None
        self.msg = None
        self.rs = None
        self.deployment = None

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
