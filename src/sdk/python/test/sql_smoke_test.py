from fedb import driver
def test_create_db():
    options = driver.DriverOptions("172.12.128.37:4181", "/onebox")
    driver = driver.Driver(options)
    assert driver.init()
    assert driver.createDB("dx")[0]

