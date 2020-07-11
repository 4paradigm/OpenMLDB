from fedb import driver
import case_conf
import time

def test_create_db():
    options = driver.DriverOptions(case_conf.FEDB_ZK_CLUSTER,
            case_conf.FEDB_ZK_PATH)
    sdk = driver.Driver(options)
    assert sdk.init()
    db_name = "py_db" + str(time.time())
    cases = [(db_name, True), (db_name, False)]
    def check_case_ret(d, db, ret):
        ok, error = d.createDB(db)
        assert ok == ret
    for case in cases:
        yield check_case_ret, sdk, case[0], case[1]


