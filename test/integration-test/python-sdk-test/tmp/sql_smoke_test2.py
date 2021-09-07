import sys,os
import util.tools as tool
rootPath = tool.getRootPath()

import sqlalchemy as db
import time

from nb_log import LogManager
log = LogManager('fedb-sdk-test').get_logger_and_add_handlers()

def getEngine():
    engine = db.create_engine('fedb://@/test_zw?zk={}&zkPath={}'.format("172.24.4.55:10000", "/fedb"))
    return engine

def test_smoke():
    print("hello")
    engine = getEngine()
    connection = engine.connect()
    db_name = "pydb_zw"
    table_name = "pytable_zw"
    # connection.execute("create database "+db_name+";")

    # create table
    ddl = "create table "+table_name + "(col1 string, col2 int, col3 float, col4 bigint, index(key=col1, ts=col4));"
    rs = connection.execute(ddl)

    # # insert table normal
    insert_normal = "insert into " + table_name + " values('hello', 123, 3.14, 1000);"

    rs = connection.execute(insert_normal)
    print("DDD:" + str(rs))
    print(type(rs))
    print(rs.__dict__)
    rs = connection.execute("select * from "+table_name+";")

    print("AAA:"+str(rs.rowcount))

    for i in rs:
        j = 0
        # line = data[i[0]]
        print("BBB:"+str(i))
        for d in i:
            print("CCC:"+str(d))
            j += 1

    # # drop not empty db
    # ok, error = sdk.dropDB(db_name)
    # assert ok == False
    #
    # # drop table
    # ok, error = sdk.executeDDL(db_name, "drop table " + table_name + ";")
    # assert ok == True
    #
    # # drop db
    # ok, error = sdk.dropDB(db_name)
    # assert ok == True

    print("end")
    log.info("AAAA")

if __name__ == "__main__":
    test_smoke()


