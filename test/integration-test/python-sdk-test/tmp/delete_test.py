import sys,os

#from fedb import driver
import time
from nb_log import LogManager


log = LogManager('fesql-auto-test').get_logger_and_add_handlers()

def test_smoke():
    print("hello")
    options = driver.DriverOptions("172.27.128.37:16181","/fedb_0903")
    sdk = driver.Driver(options)
    assert sdk.init()
    db_name = "test_zw"
    with open("tables.txt","r") as f:
        for line in f.readlines():
            tableName = line.strip()
            ok, error = sdk.executeDDL(db_name, "drop table " + tableName + ";")
            assert ok == True

    print("end")

if __name__ == "__main__":
    #test_smoke()
    pass


