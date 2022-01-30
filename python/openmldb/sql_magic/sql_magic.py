from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
import openmldb
import re
@magics_class
class SqlMagic(Magics):

    def __init__(self, shell):
        super(SqlMagic, self).__init__(shell)
        db = openmldb.dbapi.connect('db_test','127.0.0.1:6181','/onebox')
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
            
        if self.createDBRE.match(sqlText):
            DB = sqlText.split()[-1].rstrip(";")
            print(f'Sucessfully create database {DB}.') 
        elif self.createTableRE.match(sqlText):
            print('Sucessfully create table.')
        elif self.insertRE.match(sqlText):
            print('Insert successfully')
        elif self.dropTable.match(sqlText):
            print('Sucessfully drop table')

def register():
    ip = get_ipython()
    magics = SqlMagic(ip)
    ip.register_magics(magics)
