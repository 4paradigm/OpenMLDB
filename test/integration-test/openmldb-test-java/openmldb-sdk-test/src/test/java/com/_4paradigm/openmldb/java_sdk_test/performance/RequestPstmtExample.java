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

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class RequestPstmtExample extends BaseExample{
    private static final Logger logger = LoggerFactory.getLogger(RequestPstmtExample.class);
    private SqlExecutor sqlExecutor = null;
    private String db = "test_db2";
    private String dropDdl = "drop table main;";

    private String ddl = "create table main(\n" +
            "SK_ID_CURR bigint,\n" +
            "TARGET bigint,\n" +
            "NAME_CONTRACT_TYPE string,\n" +
            "CODE_GENDER string,\n" +
            "FLAG_OWN_CAR string,\n" +
            "FLAG_OWN_REALTY string,\n" +
            "CNT_CHILDREN bigint,\n" +
            "AMT_INCOME_TOTAL double,\n" +
            "AMT_CREDIT double,\n" +
            "AMT_ANNUITY double,\n" +
            "AMT_GOODS_PRICE double,\n" +
            "NAME_TYPE_SUITE string,\n" +
            "NAME_INCOME_TYPE string,\n" +
            "NAME_EDUCATION_TYPE string,\n" +
            "NAME_FAMILY_STATUS string,\n" +
            "NAME_HOUSING_TYPE string,\n" +
            "REGION_POPULATION_RELATIVE double,\n" +
            "DAYS_BIRTH bigint,\n" +
            "DAYS_EMPLOYED bigint,\n" +
            "DAYS_REGISTRATION double,\n" +
            "DAYS_ID_PUBLISH bigint,\n" +
            "OWN_CAR_AGE double,\n" +
            "FLAG_MOBIL bigint,\n" +
            "FLAG_EMP_PHONE bigint,\n" +
            "FLAG_WORK_PHONE bigint,\n" +
            "FLAG_CONT_MOBILE bigint,\n" +
            "FLAG_PHONE bigint,\n" +
            "FLAG_EMAIL bigint,\n" +
            "OCCUPATION_TYPE string,\n" +
            "CNT_FAM_MEMBERS double,\n" +
            "REGION_RATING_CLIENT bigint,\n" +
            "REGION_RATING_CLIENT_W_CITY bigint,\n" +
            "WEEKDAY_APPR_PROCESS_START string,\n" +
            "HOUR_APPR_PROCESS_START bigint,\n" +
            "REG_REGION_NOT_LIVE_REGION bigint,\n" +
            "REG_REGION_NOT_WORK_REGION bigint,\n" +
            "LIVE_REGION_NOT_WORK_REGION bigint,\n" +
            "REG_CITY_NOT_LIVE_CITY bigint,\n" +
            "REG_CITY_NOT_WORK_CITY bigint,\n" +
            "LIVE_CITY_NOT_WORK_CITY bigint,\n" +
            "ORGANIZATION_TYPE string,\n" +
            "EXT_SOURCE_1 double,\n" +
            "EXT_SOURCE_2 double,\n" +
            "EXT_SOURCE_3 double,\n" +
            "APARTMENTS_AVG double,\n" +
            "BASEMENTAREA_AVG double,\n" +
            "YEARS_BEGINEXPLUATATION_AVG double,\n" +
            "YEARS_BUILD_AVG double,\n" +
            "COMMONAREA_AVG double,\n" +
            "ELEVATORS_AVG double,\n" +
            "ENTRANCES_AVG double,\n" +
            "FLOORSMAX_AVG double,\n" +
            "FLOORSMIN_AVG double,\n" +
            "LANDAREA_AVG double,\n" +
            "LIVINGAPARTMENTS_AVG double,\n" +
            "LIVINGAREA_AVG double,\n" +
            "NONLIVINGAPARTMENTS_AVG double,\n" +
            "NONLIVINGAREA_AVG double,\n" +
            "APARTMENTS_MODE double,\n" +
            "BASEMENTAREA_MODE double,\n" +
            "YEARS_BEGINEXPLUATATION_MODE double,\n" +
            "YEARS_BUILD_MODE double,\n" +
            "COMMONAREA_MODE double,\n" +
            "ELEVATORS_MODE double,\n" +
            "ENTRANCES_MODE double,\n" +
            "FLOORSMAX_MODE double,\n" +
            "FLOORSMIN_MODE double,\n" +
            "LANDAREA_MODE double,\n" +
            "LIVINGAPARTMENTS_MODE double,\n" +
            "LIVINGAREA_MODE double,\n" +
            "NONLIVINGAPARTMENTS_MODE double,\n" +
            "NONLIVINGAREA_MODE double,\n" +
            "APARTMENTS_MEDI double,\n" +
            "BASEMENTAREA_MEDI double,\n" +
            "YEARS_BEGINEXPLUATATION_MEDI double,\n" +
            "YEARS_BUILD_MEDI double,\n" +
            "COMMONAREA_MEDI double,\n" +
            "ELEVATORS_MEDI double,\n" +
            "ENTRANCES_MEDI double,\n" +
            "FLOORSMAX_MEDI double,\n" +
            "FLOORSMIN_MEDI double,\n" +
            "LANDAREA_MEDI double,\n" +
            "LIVINGAPARTMENTS_MEDI double,\n" +
            "LIVINGAREA_MEDI double,\n" +
            "NONLIVINGAPARTMENTS_MEDI double,\n" +
            "NONLIVINGAREA_MEDI double,\n" +
            "FONDKAPREMONT_MODE string,\n" +
            "HOUSETYPE_MODE string,\n" +
            "TOTALAREA_MODE double,\n" +
            "WALLSMATERIAL_MODE string,\n" +
            "EMERGENCYSTATE_MODE string,\n" +
            "OBS_30_CNT_SOCIAL_CIRCLE double,\n" +
            "DEF_30_CNT_SOCIAL_CIRCLE double,\n" +
            "OBS_60_CNT_SOCIAL_CIRCLE double,\n" +
            "DEF_60_CNT_SOCIAL_CIRCLE double,\n" +
            "DAYS_LAST_PHONE_CHANGE double,\n" +
            "FLAG_DOCUMENT_2 bigint,\n" +
            "FLAG_DOCUMENT_3 bigint,\n" +
            "FLAG_DOCUMENT_4 bigint,\n" +
            "FLAG_DOCUMENT_5 bigint,\n" +
            "FLAG_DOCUMENT_6 bigint,\n" +
            "FLAG_DOCUMENT_7 bigint,\n" +
            "FLAG_DOCUMENT_8 bigint,\n" +
            "FLAG_DOCUMENT_9 bigint,\n" +
            "FLAG_DOCUMENT_10 bigint,\n" +
            "FLAG_DOCUMENT_11 bigint,\n" +
            "FLAG_DOCUMENT_12 bigint,\n" +
            "FLAG_DOCUMENT_13 bigint,\n" +
            "FLAG_DOCUMENT_14 bigint,\n" +
            "FLAG_DOCUMENT_15 bigint,\n" +
            "FLAG_DOCUMENT_16 bigint,\n" +
            "FLAG_DOCUMENT_17 bigint,\n" +
            "FLAG_DOCUMENT_18 bigint,\n" +
            "FLAG_DOCUMENT_19 bigint,\n" +
            "FLAG_DOCUMENT_20 bigint,\n" +
            "FLAG_DOCUMENT_21 bigint,\n" +
            "AMT_REQ_CREDIT_BUREAU_HOUR double,\n" +
            "AMT_REQ_CREDIT_BUREAU_DAY double,\n" +
            "AMT_REQ_CREDIT_BUREAU_WEEK double,\n" +
            "AMT_REQ_CREDIT_BUREAU_MON double,\n" +
            "AMT_REQ_CREDIT_BUREAU_QRT double,\n" +
            "AMT_REQ_CREDIT_BUREAU_YEAR double,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=32d)\n" +
            ");\n";

    public void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setZkSessionTimeout(10000);
        sqlExecutor = new SqlClusterExecutor(option);
        sqlExecutor.executeDDL(db, dropDdl);
        sqlExecutor.executeDDL(db, ddl);
    }

    public void test() throws SQLException {
        String sql = "select * from main;";
        PreparedStatement requestPs = sqlExecutor.getRequestPreparedStmt("test", sql);
        ResultSetMetaData metaData = requestPs.getMetaData();
        Object[] objs = {100716,0, "Cash loans","F","Y","N",1,135000.0,761067.0,60259.5,657000.0,
                "Unaccompanied","Commercial associate","Secondary / secondary special","Married", "With parents",
                0.018801,-11981,-2419,-5181.0,-3173,20.0,1,1,0,1,0,0,null,3.0,2,2,"SATURDAY",13,0,0,0,0,0,0,
                "Business Entity Type 3",0.4645496632652224,0.3184658179977818,0.7295666907060153,0.0928,0.0884,
                0.9762,0.6736,0.0158,0.0,0.2069,0.1667,0.2083,0.0774,0.0756,0.0868,0.0,0.0,0.0945,0.091,0.9757,
                0.6798,0.0131,0.0,0.2069,0.1667,0.2083,0.0735,0.0826,0.0902,0.0,0.0,0.0937,0.0884,0.9762,
                0.6779999999999999,0.0159,0.0,0.2069,0.1667,0.2083,0.0787,0.077,0.0883,0.0,0.0,"reg oper spec account",
                "block of flats",0.0681,"Panel","No",1.0,0.0,1.0,0.0,-1604.0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0.0,0.0,0.0,0.0,0.0,3.0, Timestamp.valueOf("2020-02-02 08:00:00.0").getTime()};
        System.out.println("object array size: " + objs.length);
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            Object obj = objs[i];
//            if (obj == null) {
//                requestPs.setNull(i + 1, 0);
//                continue;
//            }
//            System.out.println("object:: " + obj + ", index:" + i);
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                requestPs.setBoolean(i + 1, (Boolean) obj);
            } else if (columnType == Types.SMALLINT) {
                requestPs.setShort(i + 1, (Short) obj);
            } else if (columnType == Types.INTEGER) {
                requestPs.setInt(i + 1, (Integer) obj);
            } else if (columnType == Types.BIGINT) {
                requestPs.setLong(i + 1, Long.parseLong(obj.toString()));
            } else if (columnType == Types.FLOAT) {
                requestPs.setFloat(i + 1, Float.parseFloat(obj.toString()));
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, Double.parseDouble(obj.toString()));
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(Long.parseLong(obj.toString())));
            } else if (columnType == Types.DATE) {
                try {
                    Date date = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(obj.toString() + " 00:00:00").getTime());
                    logger.info("build request row: obj: {}, append date: {},  {}, {}, {}",
                            obj, date.toString(), date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                    requestPs.setDate(i + 1, date);
                } catch (ParseException e) {
                    logger.error("Fail convert {} to date", obj.toString());
                    return;
                }
            } else if (columnType == Types.VARCHAR) {
                requestPs.setString(i + 1, (String)obj);
            } else {
                logger.error("fail to build request row: invalid data type {]", columnType);
                return;
            }
        }
        ResultSet resultSet = requestPs.executeQuery();
        resultSet.next();
        System.out.println(resultSet.getNString(1));
    }

    public static void main(String[] args) {
        RequestPstmtExample example = new RequestPstmtExample();
        try {
            example.init();
            example.test();
        } catch (Exception throwables) {
            throwables.printStackTrace();
        }
    }
}
