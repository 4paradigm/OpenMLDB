/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.java_sdk_test.performance;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RequestAggExample extends BaseExample {

    private static final Logger logger = LoggerFactory.getLogger(RequestAggExample.class);
    private String ddl = "create table trans(c_sk_seq string,\n" +
            "                   cust_no string,\n" +
            "                   pay_cust_name string,\n" +
            "                   pay_card_no string,\n" +
            "                   payee_card_no string,\n" +
            "                   card_type string,\n" +
            "                   merch_id string,\n" +
            "                   txn_datetime string,\n" +
            "                   txn_amt double,\n" +
            "                   txn_curr string,\n" +
            "                   card_balance double,\n" +
            "                   day_openbuy double,\n" +
            "                   credit double,\n" +
            "                   remainning_credit double,\n" +
            "                   indi_openbuy double,\n" +
            "                   lgn_ip string,\n" +
            "                   IEMI string,\n" +
            "                   client_mac string,\n" +
            "                   chnl_type int32,\n" +
            "                   cust_idt int32,\n" +
            "                   cust_idt_no string,\n" +
            "                   province string,\n" +
            "                   city string,\n" +
            "                   latitudeandlongitude string,\n" +
            "                   txn_time timestamp,\n" +
            "                   index(key=pay_card_no, ts=txn_time),\n" +
            "                   index(key=merch_id, ts=txn_time));";
    //    private String zkCluster="172.27.128.81:16181";
//    private String zkPath="/fedb_cluster";

    private SqlExecutor sqlExecutor = null;
    private String db = "test_db2";
    private String tname = "trans";
    private boolean needInit = true;
    private String insertTpl = "insert into trans values('c_sk_seq0','cust_no0','pay_cust_name0','%s','payee_card_no0','card_type0','%s','2020-10-20 10:23:50',1.0,'txn_curr'," +
            "2.0,3.0,4.0,5.0,6.0,'lgn_ip0','iemi0','client_mac0',10,20,'cust_idt_no0','province0','city0', 'longitude', %d);";
    private String dropDdl = "drop table trans;";
    private String cardNo = "card1";
    private String merchantId = "merChantId1";

    public void printSQL() {
        logger.info("ddl: \n{}", ddl);
        logger.info("insertTpl: \n{}", insertTpl);
    }

    public void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setZkSessionTimeout(10000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    public void initDDL() throws Exception {
        sqlExecutor.createDB(db);
        sqlExecutor.executeDDL(db, dropDdl);
        sqlExecutor.executeDDL(db, ddl);

    }

    public void initSample() {
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            String sql = String.format(insertTpl, cardNo, merchantId, ts + i);
            sqlExecutor.executeInsert(db, sql);
        }
    }

    public void clearDDL() throws Exception {
        sqlExecutor.executeDDL(db, dropDdl);

    }

    private void updateProcessInfo(boolean isOk) {
        int cnt = threadLocalProcessCnt.get();
        int errorCnt = threadLocalProcessErrorCnt.get();
        if (0 == cnt % 1000) {
            logger.info("thread {} process {} error {} ..........", Thread.currentThread().getId(), cnt, errorCnt);
            System.gc();
        }
        threadLocalProcessCnt.set(cnt + 1);
        if (!isOk) {
            threadLocalProcessErrorCnt.set(errorCnt + 1);
        }
    }

    public void requestInsert() {
        Random random = new Random(System.currentTimeMillis());
        String card = String.valueOf(random.nextInt(1000000));
        String mc = String.valueOf(random.nextInt(1000000));
        String sql = String.format(insertTpl, card, mc, System.currentTimeMillis());
        int cnt = threadLocalProcessCnt.get();
        int errorCnt = threadLocalProcessErrorCnt.get();
        updateProcessInfo(sqlExecutor.executeInsert(db, sql));
    }

//    public void requestAgg() throws Exception {
//        String sql = "select " +
//                " count(c_sk_seq) over w1 as count_include_request, sum(txn_amt) over w3 as sum_include_request," +
//                " count(c_sk_seq) over w2 as count_not_include_request, sum(txn_amt) over w4 as sum_not_include_request from trans window w1 as (PARTITION BY trans.pay_card_no ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)," +
//                " w2 as (PARTITION BY trans.pay_card_no ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND 1 PRECEDING)," +
//                " w3 as (PARTITION BY trans.merch_id ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)," +
//                " w4 as (PARTITION BY trans.merch_id ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND 1 PRECEDING);";
//
//        SQLRequestRow request = sqlExecutor.getRequestRow(db, sql);
//        // 启动字符串当成null
//        request.Init(cardNo.length() + merchantId.length());
//        Schema schema = request.GetSchema();
//        for (int i = 0; i < schema.GetColumnCnt(); i++) {
//            if (schema.GetColumnName(i).equals("pay_card_no")) {
//                request.AppendString(cardNo);
//                continue;
//            } else if (schema.GetColumnName(i).equals("merch_id")) {
//                request.AppendString(merchantId);
//            } else if (schema.GetColumnType(i) == DataType.kTypeString) {
//                request.AppendNULL();
//            } else if (schema.GetColumnType(i) == DataType.kTypeDouble) {
//                request.AppendDouble(10.0);
//            } else if (schema.GetColumnType(i) == DataType.kTypeInt32) {
//                request.AppendInt32(0);
//            } else if (schema.GetColumnType(i) == DataType.kTypeTimestamp) {
//                request.AppendTimestamp(System.currentTimeMillis());
//            }
//        }
//        boolean ok = request.Build();
//        if (!ok) {
//            updateProcessInfo(false);
//            request.delete();
//            schema.delete();
//            return;
//        }
//        ResultSet rs = sqlExecutor.executeSQL(db, sql, request);
//        updateProcessInfo(rs != null && rs.Size() > 0);
//        rs.delete();
//        request.delete();
//        schema.delete();
//    }

    public void requestAgg() throws Exception {
        String sql = "select " +
                " count(c_sk_seq) over w1 as count_include_request, sum(txn_amt) over w3 as sum_include_request," +
                " count(c_sk_seq) over w2 as count_not_include_request, sum(txn_amt) over w4 as sum_not_include_request from trans window w1 as (PARTITION BY trans.pay_card_no ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)," +
                " w2 as (PARTITION BY trans.pay_card_no ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND 1 PRECEDING)," +
                " w3 as (PARTITION BY trans.merch_id ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW)," +
                " w4 as (PARTITION BY trans.merch_id ORDER BY trans.txn_time ROWS_RANGE BETWEEN 30d PRECEDING AND 1 PRECEDING);";

        PreparedStatement pst = sqlExecutor.getRequestPreparedStmt(db, sql);
        // 启动字符串当成null
        ResultSetMetaData metaData = pst.getMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            int columnType = metaData.getColumnType(i + 1);
            if (columnName.equals("pay_card_no")) {
                pst.setString(i + 1, cardNo);
            } else if (columnName.equals("merch_id")) {
                pst.setString(i + 1, merchantId);
            } else if (columnType == Types.VARCHAR) {
                pst.setString(i + 1, null);
            } else if (columnType == Types.DOUBLE) {
                pst.setDouble(i + 1, 10.0);
            } else if (columnType == Types.INTEGER) {
                pst.setInt(i + 1, 0);
            } else if (columnType == Types.TIMESTAMP) {
                pst.setTimestamp(i + 1, new Timestamp(System.currentTimeMillis()));
            }
        }
        java.sql.ResultSet resultSet = pst.executeQuery();
        if (resultSet == null) {
            updateProcessInfo(false);
            pst.close();
            return;
        }
        updateProcessInfo(resultSet != null && resultSet.getFetchSize()> 0);
        pst.close();
        resultSet.close();
    }

    public enum ExecuteType {
        kRequestInsert,
        kRequestAgg,
    }

    public static void run(ExecuteType type, int threadNum) {
        CountDownLatch latch = new CountDownLatch(threadNum);
        final RequestAggExample example = new RequestAggExample();
        try {
            example.init();
            example.initDDL();
            example.initSample();
            int threadPoolSize = threadNum < 8 ? threadNum :
                    8;
            Executor executor = Executors.newFixedThreadPool(threadPoolSize);
            for (int i = 0; i < threadNum; i++) {
                executor.execute(new Runnable() {
                    public void run() {
                        threadLocalProcessCnt.set(0);
                        threadLocalProcessErrorCnt.set(0);
                        while (true) {
                            try {
                                switch (type) {
                                    case kRequestInsert: {
                                        example.requestInsert();
                                        break;
                                    }
                                    case kRequestAgg: {
                                        example.requestAgg();
                                        break;
                                    }
                                    default: {
                                        logger.error("invalid executor type");
                                    }
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                                latch.countDown();
                            }
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            logger.info("wait for executor processing");
            latch.await();
        } catch (InterruptedException e) {
            // handle
            e.printStackTrace();
            try {
                example.clearDDL();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        run(ExecuteType.kRequestAgg, 8);
    }
}
