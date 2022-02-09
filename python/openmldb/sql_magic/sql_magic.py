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
from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
import re
@magics_class
class SqlMagic(Magics):

    def __init__(self, shell, db):
        super(SqlMagic, self).__init__(shell)
        self.db = db
        self.cursor = db.cursor()
        self.selectRE = re.compile("^select", re.I)
        self.createTableRE = re.compile("^create\s+table", re.I)
        self.createDBRE = re.compile("^create\s+database", re.I)
        self.insertRE = re.compile("^insert", re.I)
        self.dropTable = re.compile("^drop\s+table", re.I)

    @line_cell_magic
    def sql(self, line, cell=None):
        if cell is None:
            sqlText = line
        else:
            sqlText = cell.replace("\n", " ")
            
        if self.selectRE.match(sqlText):
            print(self.cursor.execute(sqlText).fetchone())
        else:
            self.cursor.execute(sqlText)

def register():
    ip = get_ipython()
    magics = SqlMagic(ip,db)
    ip.register_magics(magics)
