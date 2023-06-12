package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
public class TestProcedure{
    private String zkCluster = "172.24.4.55:30008";
    private String zkPath = "/openmldb";
    private SqlExecutor executor;
    private String dbName = "test_bank";
    private String spName = "deploy_bank";
    @BeforeClass
    public void init() throws Exception{
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setEnableDebug(true);
        option.setSessionTimeout(1000000);
        option.setRequestTimeout(1000000);
        log.info("zkCluster {}, zkPath {}", option.getZkCluster(), option.getZkPath());
        executor = new SqlClusterExecutor(option);
    }
    @Test
    public void testBank() throws Exception{
        List<String> expectResult = Lists.newArrayList(
            "01a62b57004e934c667c3e9171d0e1cc_2000-11-29|01a62b57004e934c667c3e9171d0e1cc_2000-11-29|2000-11-29 00:00:00.0       |39328                            |01a62b57004e934c667c3e9171d0e1cc     |01a62b57004e934c667c3e9171d0e1cc_2000-11-29|0                                |3                         |1                               |2020-10-23 15:53:26.052      |3                              |2                          |1                          |01a62b57004e934c667c3e9171d0e1cc_2000-11-29|26.457513110645905                 |0.0                                |50.36053315841683                     |25.81888984492716                     |0.0                                |19.84313483298443                  |0.0                                |0.0                                |51.0440789906136                      |0.0                                   |50.09106906425536                       |51.85178299730878                       |0.0                                           |50.803574677378755                            |52.86717696264857                  |52.99235227841844                  |50.94666230480659                       |37.576790183759165                      |25.763380851647117                       |50.13849718529666                        |0.0                                   |19.84313483298443                     |,11,14                                       |4                                           |0,,NULL                                           |2                                                |01a62b57004e934c667c3e9171d0e1cc_2000-11-29|2                                             |4                                             |2                                                |3                                                |01a62b57004e934c667c3e9171d0e1cc_2000-11-29|0.0                           |0.0                           |1                                      |1                                      |1                                      |1",
            "01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30|01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30|2000-11-30 00:00:00.0            |39947                            |01d85e5e9c6cbcf5046707d721f7e8fc     |01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30|0                                |4                         |2                               |2020-10-23 15:53:26.052      |2                              |2                          |1                          |01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30|26.457513110645905                 |0.0                                |50.36053315841683                     |25.60601542302091                     |0.0                                |17.638342073763937                 |0.0                                |0.0                                |51.93387237632103                     |0.0                                   |49.34535135146977                       |51.88535920662013                       |0.0                                           |50.232673629819864                            |51.89189821157056                  |51.89189821157056                  |51.88348484826362                       |31.33363280939899                       |25.226499683963226                       |49.22198594124378                        |0.0                                   |17.638342073763937                    |,11,16                                       |3                                           |0,,NULL                                           |2                                                |01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30|1                                             |2                                             |1                                                |2                                                |01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30|0.0                           |0.0                           |1                                      |1                                      |1                                      |1",
            "02e66caf7336489f55b45ac471295187_2000-10-21|02e66caf7336489f55b45ac471295187_2000-10-21|2000-10-21 00:00:00.0            |20786                            |02e66caf7336489f55b45ac471295187     |02e66caf7336489f55b45ac471295187_2000-10-21|0                                |3                         |2                               |2020-10-23 15:53:26.052      |3                              |2                          |2                          |02e66caf7336489f55b45ac471295187_2000-10-21|26.457513110645905                 |0.0                                |52.77000947507969                     |31.691009034290232                    |0.0                                |26.545392178383434                 |0.0                                |0.0                                |53.82634206408605                     |0.0                                   |53.23327249005081                       |53.33405009935022                       |0.0                                           |52.01805936403241                             |53.35442155997945                  |53.403762039766455                 |53.403762039766455                      |16.46652068974875                       |26.540522679803097                       |53.33007406707776                        |0.0                                   |21.166010488516726                    |7,,14                                        |4                                           |0,,NULL                                           |2                                                |02e66caf7336489f55b45ac471295187_2000-10-21|1                                             |3                                             |1                                                |3                                                |02e66caf7336489f55b45ac471295187_2000-10-21|0.0                           |0.0                           |1                                      |1                                      |1                                      |1"
        );

        List<List<Object>> rows = Lists.newArrayList(
                Lists.newArrayList("01a62b57004e934c667c3e9171d0e1cc_2000-11-29","2000-11-29 00:00:00.0","39328","01a62b57004e934c667c3e9171d0e1cc",5929889487L,1,"2000-11-29"),
                Lists.newArrayList("01d85e5e9c6cbcf5046707d721f7e8fc_2000-11-30","2000-11-30 00:00:00.0","39947","01d85e5e9c6cbcf5046707d721f7e8fc",5929975887L,1,"2000-11-30"),
                Lists.newArrayList("02e66caf7336489f55b45ac471295187_2000-10-21","2000-10-21 00:00:00.0","20786","02e66caf7336489f55b45ac471295187",5926519887L,1,"2000-10-21")
        );
        CallablePreparedStatement rps = executor.getCallablePreparedStmt(dbName, spName);
        List<List<Object>> actualResult = new ArrayList<>();
        for(List<Object> row:rows) {
            setRequestData(rps, row);
            ResultSet resultSet = rps.executeQuery();
            List<Object> list = parseResultSetToList(resultSet);
            actualResult.add(list);
            rps.clearParameters();
        }
        actualResult.forEach(row->System.out.println(row));
        check(actualResult,expectResult);
    }
    private void check(List<List<Object>> actualResult,List<String> expectResult){
        Assert.assertEquals(actualResult.size(),expectResult.size());
        for(int i=0;i<expectResult.size();i++){
            String expectStr = expectResult.get(i);
            List<Object> actualRow = actualResult.get(i);
            String[] expectRow = expectStr.split("\\|");
            Assert.assertEquals(actualRow.size(),expectRow.length);
            for(int j=0;j<expectRow.length;j++){
                Assert.assertEquals(String.valueOf(actualRow.get(j)),expectRow[j].trim());
            }
        }
    }
    // 根据Input参数设置PreparedStatement
    private void setRequestData(PreparedStatement requestPs, List<Object> row) throws SQLException {
        requestPs.setString(1,String.valueOf(row.get(0)));
        requestPs.setTimestamp(2,new Timestamp(parseDateToLong(String.valueOf(row.get(1)))));
        requestPs.setString(3,String.valueOf(row.get(2)));
        requestPs.setString(4,String.valueOf(row.get(3)));
        requestPs.setLong(5,Long.parseLong(String.valueOf(row.get(4))));
        requestPs.setInt(6,Integer.parseInt(String.valueOf(row.get(5))));
        requestPs.setString(7,String.valueOf(row.get(6)));
    }
    private List<Object> parseResultSetToList(ResultSet rs){
        List<Object> list = new ArrayList<>();
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            if(rs.next()){
                for (int i = 0; i < columnCount; i++) {
                    int columnType = metaData.getColumnType(i + 1);
                    list.add(getColumnData(rs,i+1,columnType));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
    private Object getColumnData(ResultSet rs,int index,int columnType) throws Exception {
        String obj = ((SQLResultSet)rs).getNString(index);
        if(null==obj){
            return null;
        }
        switch (columnType){
            case Types.BOOLEAN:
                return rs.getBoolean(index);
            case Types.SMALLINT:
                return rs.getShort(index);
            case Types.INTEGER:
                return rs.getInt(index);
            case Types.BIGINT:
                return rs.getLong(index);
            case Types.FLOAT:
                return rs.getFloat(index);
            case Types.DOUBLE:
                return rs.getDouble(index);
            case Types.TIMESTAMP:
                return rs.getTimestamp(index);
            case Types.DATE:
                return rs.getDate(index);
            case Types.VARCHAR:
                return rs.getString(index);
            default:
                throw new RuntimeException("get column data failed : invalid data type:"+columnType);
        }
    }
    private long parseDateToLong(String str){
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            Date date = df.parse(str);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("please input yyyy-MM-dd HH:mm:ss.SSS,date:"+str);
        }
    }
}
