from rtidb_sql import sql_router_sdk


def test_create_db():
    options = sql_router_sdk.SQLRouterOptions()
    options.zk_cluster = "172.27.128.37:4181"
    options.zk_path = "/onebox"
    sdk = sql_router_sdk.NewClusterSQLRouter(options)
    status = sql_router_sdk.Status()
    sdk.CreateDB("dx", status)
