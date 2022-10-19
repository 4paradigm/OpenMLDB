package com._4paradigm.openmldb.java_sdk_test.cluster.high_availability;

import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
public class HighDiskTableTest {

    @Test
    public void test1() throws Exception {
        SdkOption option = new SdkOption();
        option.setZkPath("/openmldb");
        option.setZkCluster("172.24.4.55:30030");
        SqlExecutor router = null;
        try {
            router = new SqlClusterExecutor(option);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        Statement statement = router.getStatement();
        statement.execute("set @@SESSION.execute_mode='online';");
        statement.execute("use test_zw");
        statement.execute("create table table1 (\n" +
                "    id int,\n" +
                "    c1 string,\n" +
                "    c3 int,\n" +
                "    c4 bigint,\n" +
                "    c5 float,\n" +
                "    c6 double,\n" +
                "    c7 timestamp,\n" +
                "    c8 date,\n" +
                "    index(key=c1,ts=c7,ttl=60m,ttl_type=ABSOLUTE )\n" +
                ")options(partitionnum = 1,replicanum = 1,storage_mode=\"SSD\");");

        insert10000(statement,"table1",1000*60*60*24L);
        ResultSet resultSet = statement.executeQuery("select * from table1");
        SQLResultSet rs = (SQLResultSet)resultSet;
        List<List<Object>> result = convertRestultSetToList(rs);
        System.out.println(result.size());
        result.forEach(s-> System.out.println(s));
        System.out.println(result.size());
        statement.execute("DROP TABLE table1");
    }

    /**
     *
     * @param statement
     * @param tableName
     * @param lastTime  1000*秒*分钟*小时
     * @throws Exception
     */
    public static void insert10000(Statement statement,String tableName,Long lastTime) throws Exception {

        long startTime = new Date().getTime();

        int i = 0;
        while (true){
            long time = new Date().getTime();
//            String sql = String.format("insert into %s values('bb',%d,%d,%d);",tableName,i,i+1,time);
            String sql = String.format("insert into %s values (%d,\"aa\",%d,30,1.1,2.1,%d,\"2020-05-01\");",tableName,i,i+1,time);
            System.out.println(sql);
            statement.execute(sql);
            Thread.sleep(1000);
            if(time<startTime+lastTime+1000 && time>startTime+lastTime-1000){
                break;
            }
            i++;
        }
        log.info("stop stop stop");
    }


    private static List<List<Object>> convertRestultSetToList(SQLResultSet rs) throws SQLException {
        List<List<Object>> result = new ArrayList<>();
        while (rs.next()) {
            List list = new ArrayList();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                list.add(getColumnData(rs, i));
            }
            result.add(list);
        }
        return result;
    }

    public static Object getColumnData(SQLResultSet rs, int index) throws SQLException {
        Object obj = null;
        int columnType = rs.getMetaData().getColumnType(index + 1);
        if (rs.getNString(index + 1) == null) {
            log.info("rs is null");
            return null;
        }
        if (columnType == Types.BOOLEAN) {
            obj = rs.getBoolean(index + 1);
        } else if (columnType == Types.DATE) {
            try {
//                obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                        .parse(rs.getNString(index + 1) + " 00:00:00").getTime());
                obj = rs.getDate(index + 1);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else if (columnType == Types.DOUBLE) {
            obj = rs.getDouble(index + 1);
        } else if (columnType == Types.FLOAT) {
            obj = rs.getFloat(index + 1);
        } else if (columnType == Types.SMALLINT) {
            obj = rs.getShort(index + 1);
        } else if (columnType == Types.INTEGER) {
            obj = rs.getInt(index + 1);
        } else if (columnType == Types.BIGINT) {
            obj = rs.getLong(index + 1);
        } else if (columnType == Types.VARCHAR) {
            obj = rs.getString(index + 1);
            log.info("conver string data {}", obj);
        } else if (columnType == Types.TIMESTAMP) {
            obj = rs.getTimestamp(index + 1);
        }
        return obj;
    }
}
