import pytest
from sqlalchemy import engine


def pytest_addoption(parser):
    parser.addoption("--zk_root", action="store", default="openmldb-zk:2181", help="endpoint to zookeeper")
    parser.addoption("--zk_path", action="store", default="/openmldb", help="root path in zookeeper for OpenMLDB")
    parser.addoption("--url",
                     action="store",
                     default="http://openmldb-exporter:8000/metrics",
                     help="openmldb exporter pull url")
    parser.addoption("--api", action="store", default="http://openmldb-api:9527", help="openmldb apiserver url")
    parser.addoption("--prom", action="store", default="http://prometheus:9090", help="prometheus url")


@pytest.fixture(scope="session")
def conn(request):
    zk_root = request.config.getoption("--zk_root")
    zk_path = request.config.getoption("--zk_path")

    eng = engine.create_engine(f"openmldb:///?zk={zk_root}&zkPath={zk_path}")
    conn = eng.connect()
    # default online mode
    conn.execute("set session execute_mode = 'online'")
    # enable deploy response time
    conn.execute("set global deploy_stats = 'on'")
    return conn


@pytest.fixture(scope="session")
def global_url(request):
    return request.config.getoption("--url")


@pytest.fixture(scope="session")
def api_url(request):
    return request.config.getoption("--api")


@pytest.fixture(scope="session")
def prom_url(request):
    return request.config.getoption("--prom")
