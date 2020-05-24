package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.SQLRouterOptions;
import com._4paradigm.sql.Status;
import com._4paradigm.sql.sql_router_sdk;

public class SQLRouterSmoke {
    static {
        String os = System.getProperty("os.name").toLowerCase();
        String path = SQLRouterSmoke.class.getResource("/libsql_jsdk.so").getPath();
        System.load(path);
    }
    final public static void main(String[] args) {
        SQLRouterOptions options = new SQLRouterOptions();
        options.setZk_path("/onebox");
        options.setZk_cluster("172.27.128.37:6181");
        options.setSession_timeout(3000);
        SQLRouter router = sql_router_sdk.NewClusterSQLRouter(options);
        if (router != null) {
            Status status = new Status();
            boolean ok = router.CreateDB("dbxx1", status);
            if (ok) {
                System.out.println("create db ok");
            }else {
                System.out.println("create db failed");
            }
        }

    }
}
