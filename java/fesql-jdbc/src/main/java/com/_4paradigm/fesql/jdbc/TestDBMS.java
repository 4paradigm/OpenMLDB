package com._4paradigm.fesql.jdbc;
import com._4paradigm.fesql.DBMSSdk;
import com._4paradigm.fesql.Status;
import com._4paradigm.fesql.fesql;

public class TestDBMS {
    public static void main(String[] args) {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac")) {
            String path = TestDBMS.class.getResource("/libfesql_jsdk.jnilib").getPath();
            System.load(path);
        }else {
            String path = TestDBMS.class.getResource("/libfesql_jsdk.so").getPath();
            System.load(path);
        }
        String endpoint="172.27.128.37:9211";
        DBMSSdk sdk = fesql.CreateDBMSSdk(endpoint);
        Status status = new Status();
        sdk.CreateDatabase("testxx", status);
        System.out.println(status.getCode());
        System.out.println(status.getMsg());

    }
}
