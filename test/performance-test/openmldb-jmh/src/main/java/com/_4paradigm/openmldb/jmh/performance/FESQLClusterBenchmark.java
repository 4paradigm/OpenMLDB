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
package com._4paradigm.openmldb.jmh.performance;

import com._4paradigm.openmldb.jmh.BenchmarkConfig;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 1)
public class FESQLClusterBenchmark {
    private static Logger logger = LoggerFactory.getLogger(FESQLClusterBenchmark.class);
    private SqlExecutor executor;
    private SdkOption option;
    private String db = "db" + System.nanoTime();
    private Map<String, Map<String, String>> tables = new HashMap<>();
    private String partitionNum = "7";
    private int pkNum = 1;
    @Param({"10", "100", "1000"})
    private int windowNum = 10;
    private String previousApplication = "create table `previous_application`(" +
            "`SK_ID_PREV` bigint," +
            "`SK_ID_CURR` bigint," +
            "`NAME_CONTRACT_TYPE` string," +
            "`AMT_ANNUITY` double," +
            "`AMT_APPLICATION` double," +
            "`AMT_CREDIT` double," +
            "`AMT_DOWN_PAYMENT` double," +
            "`AMT_GOODS_PRICE` double," +
            "`WEEKDAY_APPR_PROCESS_START` string," +
            "`HOUR_APPR_PROCESS_START` bigint," +
            "`FLAG_LAST_APPL_PER_CONTRACT` string," +
            "`NFLAG_LAST_APPL_IN_DAY` bigint," +
            "`RATE_DOWN_PAYMENT` double," +
            "`RATE_INTEREST_PRIMARY` double," +
            "`RATE_INTEREST_PRIVILEGED` double," +
            "`NAME_CASH_LOAN_PURPOSE` string," +
            "`NAME_CONTRACT_STATUS` string," +
            "`DAYS_DECISION` bigint," +
            "`NAME_PAYMENT_TYPE` string," +
            "`CODE_REJECT_REASON` string," +
            "`NAME_TYPE_SUITE` string," +
            "`NAME_CLIENT_TYPE` string," +
            "`NAME_GOODS_CATEGORY` string," +
            "`NAME_PORTFOLIO` string," +
            "`NAME_PRODUCT_TYPE` string," +
            "`CHANNEL_TYPE` string," +
            "`SELLERPLACE_AREA` bigint," +
            "`NAME_SELLER_INDUSTRY` string," +
            "`CNT_PAYMENT` double," +
            "`NAME_YIELD_GROUP` string," +
            "`PRODUCT_COMBINATION` string," +
            "`DAYS_FIRST_DRAWING` double," +
            "`DAYS_FIRST_DUE` double," +
            "`DAYS_LAST_DUE_1ST_VERSION` double," +
            "`DAYS_LAST_DUE` double," +
            "`DAYS_TERMINATION` double," +
            "`NFLAG_INSURED_ON_APPROVAL` double," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=(46080m, 1), ttl_type=absandlat)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";
    private String posCashBalance = "create table `POS_CASH_balance`(" +
            "`SK_ID_PREV` bigint," +
            "`SK_ID_CURR` bigint," +
            "`MONTHS_BALANCE` bigint," +
            "`CNT_INSTALMENT` double," +
            "`CNT_INSTALMENT_FUTURE` double," +
            "`NAME_CONTRACT_STATUS` string," +
            "`SK_DPD` bigint," +
            "`SK_DPD_DEF` bigint," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=1, ttl_type=latest)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";
    private String installmentsPayments = "create table `installments_payments`(" +
            "`SK_ID_PREV` bigint," +
            "`SK_ID_CURR` bigint," +
            "`NUM_INSTALMENT_VERSION` double," +
            "`NUM_INSTALMENT_NUMBER` bigint," +
            "`DAYS_INSTALMENT` double," +
            "`DAYS_ENTRY_PAYMENT` double," +
            "`AMT_INSTALMENT` double," +
            "`AMT_PAYMENT` double," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=(46080m, 1), ttl_type=absandlat)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";
    private String bureauBalance = "create table `bureau_balance`(" +
            "`SK_ID_BUREAU` bigint," +
            "`MONTHS_BALANCE` bigint," +
            "`STATUS` string," +
            "`SK_ID_CURR` bigint," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=(46080m, 1), ttl_type=absandlat)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";
    private String main = "create table `main`(" +
            "`SK_ID_CURR` bigint," +
            "`TARGET` bigint," +
            "`NAME_CONTRACT_TYPE` string," +
            "`CODE_GENDER` string," +
            "`FLAG_OWN_CAR` string," +
            "`FLAG_OWN_REALTY` string," +
            "`CNT_CHILDREN` bigint," +
            "`AMT_INCOME_TOTAL` double," +
            "`AMT_CREDIT` double," +
            "`AMT_ANNUITY` double," +
            "`AMT_GOODS_PRICE` double," +
            "`NAME_TYPE_SUITE` string," +
            "`NAME_INCOME_TYPE` string," +
            "`NAME_EDUCATION_TYPE` string," +
            "`NAME_FAMILY_STATUS` string," +
            "`NAME_HOUSING_TYPE` string," +
            "`REGION_POPULATION_RELATIVE` double," +
            "`DAYS_BIRTH` bigint," +
            "`DAYS_EMPLOYED` bigint," +
            "`DAYS_REGISTRATION` double," +
            "`DAYS_ID_PUBLISH` bigint," +
            "`OWN_CAR_AGE` double," +
            "`FLAG_MOBIL` bigint," +
            "`FLAG_EMP_PHONE` bigint," +
            "`FLAG_WORK_PHONE` bigint," +
            "`FLAG_CONT_MOBILE` bigint," +
            "`FLAG_PHONE` bigint," +
            "`FLAG_EMAIL` bigint," +
            "`OCCUPATION_TYPE` string," +
            "`CNT_FAM_MEMBERS` double," +
            "`REGION_RATING_CLIENT` bigint," +
            "`REGION_RATING_CLIENT_W_CITY` bigint," +
            "`WEEKDAY_APPR_PROCESS_START` string," +
            "`HOUR_APPR_PROCESS_START` bigint," +
            "`REG_REGION_NOT_LIVE_REGION` bigint," +
            "`REG_REGION_NOT_WORK_REGION` bigint," +
            "`LIVE_REGION_NOT_WORK_REGION` bigint," +
            "`REG_CITY_NOT_LIVE_CITY` bigint," +
            "`REG_CITY_NOT_WORK_CITY` bigint," +
            "`LIVE_CITY_NOT_WORK_CITY` bigint," +
            "`ORGANIZATION_TYPE` string," +
            "`EXT_SOURCE_1` double," +
            "`EXT_SOURCE_2` double," +
            "`EXT_SOURCE_3` double," +
            "`APARTMENTS_AVG` double," +
            "`BASEMENTAREA_AVG` double," +
            "`YEARS_BEGINEXPLUATATION_AVG` double," +
            "`YEARS_BUILD_AVG` double," +
            "`COMMONAREA_AVG` double," +
            "`ELEVATORS_AVG` double," +
            "`ENTRANCES_AVG` double," +
            "`FLOORSMAX_AVG` double," +
            "`FLOORSMIN_AVG` double," +
            "`LANDAREA_AVG` double," +
            "`LIVINGAPARTMENTS_AVG` double," +
            "`LIVINGAREA_AVG` double," +
            "`NONLIVINGAPARTMENTS_AVG` double," +
            "`NONLIVINGAREA_AVG` double," +
            "`APARTMENTS_MODE` double," +
            "`BASEMENTAREA_MODE` double," +
            "`YEARS_BEGINEXPLUATATION_MODE` double," +
            "`YEARS_BUILD_MODE` double," +
            "`COMMONAREA_MODE` double," +
            "`ELEVATORS_MODE` double," +
            "`ENTRANCES_MODE` double," +
            "`FLOORSMAX_MODE` double," +
            "`FLOORSMIN_MODE` double," +
            "`LANDAREA_MODE` double," +
            "`LIVINGAPARTMENTS_MODE` double," +
            "`LIVINGAREA_MODE` double," +
            "`NONLIVINGAPARTMENTS_MODE` double," +
            "`NONLIVINGAREA_MODE` double," +
            "`APARTMENTS_MEDI` double," +
            "`BASEMENTAREA_MEDI` double," +
            "`YEARS_BEGINEXPLUATATION_MEDI` double," +
            "`YEARS_BUILD_MEDI` double," +
            "`COMMONAREA_MEDI` double," +
            "`ELEVATORS_MEDI` double," +
            "`ENTRANCES_MEDI` double," +
            "`FLOORSMAX_MEDI` double," +
            "`FLOORSMIN_MEDI` double," +
            "`LANDAREA_MEDI` double," +
            "`LIVINGAPARTMENTS_MEDI` double," +
            "`LIVINGAREA_MEDI` double," +
            "`NONLIVINGAPARTMENTS_MEDI` double," +
            "`NONLIVINGAREA_MEDI` double," +
            "`FONDKAPREMONT_MODE` string," +
            "`HOUSETYPE_MODE` string," +
            "`TOTALAREA_MODE` double," +
            "`WALLSMATERIAL_MODE` string," +
            "`EMERGENCYSTATE_MODE` string," +
            "`OBS_30_CNT_SOCIAL_CIRCLE` double," +
            "`DEF_30_CNT_SOCIAL_CIRCLE` double," +
            "`OBS_60_CNT_SOCIAL_CIRCLE` double," +
            "`DEF_60_CNT_SOCIAL_CIRCLE` double," +
            "`DAYS_LAST_PHONE_CHANGE` double," +
            "`FLAG_DOCUMENT_2` bigint," +
            "`FLAG_DOCUMENT_3` bigint," +
            "`FLAG_DOCUMENT_4` bigint," +
            "`FLAG_DOCUMENT_5` bigint," +
            "`FLAG_DOCUMENT_6` bigint," +
            "`FLAG_DOCUMENT_7` bigint," +
            "`FLAG_DOCUMENT_8` bigint," +
            "`FLAG_DOCUMENT_9` bigint," +
            "`FLAG_DOCUMENT_10` bigint," +
            "`FLAG_DOCUMENT_11` bigint," +
            "`FLAG_DOCUMENT_12` bigint," +
            "`FLAG_DOCUMENT_13` bigint," +
            "`FLAG_DOCUMENT_14` bigint," +
            "`FLAG_DOCUMENT_15` bigint," +
            "`FLAG_DOCUMENT_16` bigint," +
            "`FLAG_DOCUMENT_17` bigint," +
            "`FLAG_DOCUMENT_18` bigint," +
            "`FLAG_DOCUMENT_19` bigint," +
            "`FLAG_DOCUMENT_20` bigint," +
            "`FLAG_DOCUMENT_21` bigint," +
            "`AMT_REQ_CREDIT_BUREAU_HOUR` double," +
            "`AMT_REQ_CREDIT_BUREAU_DAY` double," +
            "`AMT_REQ_CREDIT_BUREAU_WEEK` double," +
            "`AMT_REQ_CREDIT_BUREAU_MON` double," +
            "`AMT_REQ_CREDIT_BUREAU_QRT` double," +
            "`AMT_REQ_CREDIT_BUREAU_YEAR` double," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=(46080m, 1), ttl_type=absandlat)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";
    private String creditCardBalance = "create table `credit_card_balance`(" +
            "`SK_ID_PREV` bigint," +
            "`SK_ID_CURR` bigint," +
            "`MONTHS_BALANCE` bigint," +
            "`AMT_BALANCE` double," +
            "`AMT_CREDIT_LIMIT_ACTUAL` bigint," +
            "`AMT_DRAWINGS_ATM_CURRENT` double," +
            "`AMT_DRAWINGS_CURRENT` double," +
            "`AMT_DRAWINGS_OTHER_CURRENT` double," +
            "`AMT_DRAWINGS_POS_CURRENT` double," +
            "`AMT_INST_MIN_REGULARITY` double," +
            "`AMT_PAYMENT_CURRENT` double," +
            "`AMT_PAYMENT_TOTAL_CURRENT` double," +
            "`AMT_RECEIVABLE_PRINCIPAL` double," +
            "`AMT_RECIVABLE` double," +
            "`AMT_TOTAL_RECEIVABLE` double," +
            "`CNT_DRAWINGS_ATM_CURRENT` double," +
            "`CNT_DRAWINGS_CURRENT` bigint," +
            "`CNT_DRAWINGS_OTHER_CURRENT` double," +
            "`CNT_DRAWINGS_POS_CURRENT` double," +
            "`CNT_INSTALMENT_MATURE_CUM` double," +
            "`NAME_CONTRACT_STATUS` string," +
            "`SK_DPD` bigint," +
            "`SK_DPD_DEF` bigint," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=(46080m, 1), ttl_type=absandlat)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";
    private String bureau = "create table `bureau`(" +
            "`SK_ID_CURR` bigint," +
            "`SK_ID_BUREAU` bigint," +
            "`CREDIT_ACTIVE` string," +
            "`CREDIT_CURRENCY` string," +
            "`DAYS_CREDIT` bigint," +
            "`CREDIT_DAY_OVERDUE` bigint," +
            "`DAYS_CREDIT_ENDDATE` double," +
            "`DAYS_ENDDATE_FACT` double," +
            "`AMT_CREDIT_MAX_OVERDUE` double," +
            "`CNT_CREDIT_PROLONG` bigint," +
            "`AMT_CREDIT_SUM` double," +
            "`AMT_CREDIT_SUM_DEBT` double," +
            "`AMT_CREDIT_SUM_LIMIT` double," +
            "`AMT_CREDIT_SUM_OVERDUE` double," +
            "`CREDIT_TYPE` string," +
            "`DAYS_CREDIT_UPDATE` bigint," +
            "`AMT_ANNUITY` double," +
            "`time` timestamp," +
            "index(key=(`SK_ID_CURR`), ts=`time`, ttl=(46080m, 1), ttl_type=absandlat)" +
            ")" +
            "partitionnum=" + partitionNum +
            ";";

    String benSql = "select * from " +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_1," +
            "    SK_ID_CURR as main_SK_ID_CURR_0," +
            "    TARGET as main_TARGET_1," +
            "    NAME_CONTRACT_TYPE as main_NAME_CONTRACT_TYPE_2," +
            "    CODE_GENDER as main_CODE_GENDER_3," +
            "    FLAG_OWN_CAR as main_FLAG_OWN_CAR_4," +
            "    FLAG_OWN_REALTY as main_FLAG_OWN_REALTY_5," +
            "    CNT_CHILDREN as main_CNT_CHILDREN_6," +
            "    AMT_INCOME_TOTAL as main_AMT_INCOME_TOTAL_7," +
            "    AMT_CREDIT as main_AMT_CREDIT_8," +
            "    AMT_ANNUITY as main_AMT_ANNUITY_9," +
            "    AMT_GOODS_PRICE as main_AMT_GOODS_PRICE_10," +
            "    NAME_TYPE_SUITE as main_NAME_TYPE_SUITE_11," +
            "    NAME_INCOME_TYPE as main_NAME_INCOME_TYPE_12," +
            "    NAME_EDUCATION_TYPE as main_NAME_EDUCATION_TYPE_13," +
            "    NAME_FAMILY_STATUS as main_NAME_FAMILY_STATUS_14," +
            "    NAME_HOUSING_TYPE as main_NAME_HOUSING_TYPE_15," +
            "    REGION_POPULATION_RELATIVE as main_REGION_POPULATION_RELATIVE_16," +
            "    DAYS_BIRTH as main_DAYS_BIRTH_17," +
            "    DAYS_EMPLOYED as main_DAYS_EMPLOYED_18," +
            "    DAYS_REGISTRATION as main_DAYS_REGISTRATION_19," +
            "    DAYS_ID_PUBLISH as main_DAYS_ID_PUBLISH_20," +
            "    OWN_CAR_AGE as main_OWN_CAR_AGE_21," +
            "    FLAG_MOBIL as main_FLAG_MOBIL_22," +
            "    FLAG_EMP_PHONE as main_FLAG_EMP_PHONE_23," +
            "    FLAG_WORK_PHONE as main_FLAG_WORK_PHONE_24," +
            "    FLAG_CONT_MOBILE as main_FLAG_CONT_MOBILE_25," +
            "    FLAG_PHONE as main_FLAG_PHONE_26," +
            "    FLAG_EMAIL as main_FLAG_EMAIL_27," +
            "    OCCUPATION_TYPE as main_OCCUPATION_TYPE_28," +
            "    CNT_FAM_MEMBERS as main_CNT_FAM_MEMBERS_29," +
            "    REGION_RATING_CLIENT as main_REGION_RATING_CLIENT_30," +
            "    REGION_RATING_CLIENT_W_CITY as main_REGION_RATING_CLIENT_W_CITY_31," +
            "    WEEKDAY_APPR_PROCESS_START as main_WEEKDAY_APPR_PROCESS_START_32," +
            "    HOUR_APPR_PROCESS_START as main_HOUR_APPR_PROCESS_START_33," +
            "    REG_REGION_NOT_LIVE_REGION as main_REG_REGION_NOT_LIVE_REGION_34," +
            "    REG_REGION_NOT_WORK_REGION as main_REG_REGION_NOT_WORK_REGION_35," +
            "    LIVE_REGION_NOT_WORK_REGION as main_LIVE_REGION_NOT_WORK_REGION_36," +
            "    REG_CITY_NOT_LIVE_CITY as main_REG_CITY_NOT_LIVE_CITY_37," +
            "    REG_CITY_NOT_WORK_CITY as main_REG_CITY_NOT_WORK_CITY_38," +
            "    LIVE_CITY_NOT_WORK_CITY as main_LIVE_CITY_NOT_WORK_CITY_39," +
            "    ORGANIZATION_TYPE as main_ORGANIZATION_TYPE_40," +
            "    EXT_SOURCE_1 as main_EXT_SOURCE_1_41," +
            "    EXT_SOURCE_2 as main_EXT_SOURCE_2_42," +
            "    EXT_SOURCE_3 as main_EXT_SOURCE_3_43," +
            "    APARTMENTS_AVG as main_APARTMENTS_AVG_44," +
            "    BASEMENTAREA_AVG as main_BASEMENTAREA_AVG_45," +
            "    YEARS_BEGINEXPLUATATION_AVG as main_YEARS_BEGINEXPLUATATION_AVG_46," +
            "    YEARS_BUILD_AVG as main_YEARS_BUILD_AVG_47," +
            "    COMMONAREA_AVG as main_COMMONAREA_AVG_48," +
            "    ELEVATORS_AVG as main_ELEVATORS_AVG_49," +
            "    ENTRANCES_AVG as main_ENTRANCES_AVG_50," +
            "    FLOORSMAX_AVG as main_FLOORSMAX_AVG_51," +
            "    FLOORSMIN_AVG as main_FLOORSMIN_AVG_52," +
            "    LANDAREA_AVG as main_LANDAREA_AVG_53," +
            "    LIVINGAPARTMENTS_AVG as main_LIVINGAPARTMENTS_AVG_54," +
            "    LIVINGAREA_AVG as main_LIVINGAREA_AVG_55," +
            "    NONLIVINGAPARTMENTS_AVG as main_NONLIVINGAPARTMENTS_AVG_56," +
            "    NONLIVINGAREA_AVG as main_NONLIVINGAREA_AVG_57," +
            "    APARTMENTS_MODE as main_APARTMENTS_MODE_58," +
            "    BASEMENTAREA_MODE as main_BASEMENTAREA_MODE_59," +
            "    YEARS_BEGINEXPLUATATION_MODE as main_YEARS_BEGINEXPLUATATION_MODE_60," +
            "    YEARS_BUILD_MODE as main_YEARS_BUILD_MODE_61," +
            "    COMMONAREA_MODE as main_COMMONAREA_MODE_62," +
            "    ELEVATORS_MODE as main_ELEVATORS_MODE_63," +
            "    ENTRANCES_MODE as main_ENTRANCES_MODE_64," +
            "    FLOORSMAX_MODE as main_FLOORSMAX_MODE_65," +
            "    FLOORSMIN_MODE as main_FLOORSMIN_MODE_66," +
            "    LANDAREA_MODE as main_LANDAREA_MODE_67," +
            "    LIVINGAPARTMENTS_MODE as main_LIVINGAPARTMENTS_MODE_68," +
            "    LIVINGAREA_MODE as main_LIVINGAREA_MODE_69," +
            "    NONLIVINGAPARTMENTS_MODE as main_NONLIVINGAPARTMENTS_MODE_70," +
            "    NONLIVINGAREA_MODE as main_NONLIVINGAREA_MODE_71," +
            "    APARTMENTS_MEDI as main_APARTMENTS_MEDI_72," +
            "    BASEMENTAREA_MEDI as main_BASEMENTAREA_MEDI_73," +
            "    YEARS_BEGINEXPLUATATION_MEDI as main_YEARS_BEGINEXPLUATATION_MEDI_74," +
            "    YEARS_BUILD_MEDI as main_YEARS_BUILD_MEDI_75," +
            "    COMMONAREA_MEDI as main_COMMONAREA_MEDI_76," +
            "    ELEVATORS_MEDI as main_ELEVATORS_MEDI_77," +
            "    ENTRANCES_MEDI as main_ENTRANCES_MEDI_78," +
            "    FLOORSMAX_MEDI as main_FLOORSMAX_MEDI_79," +
            "    FLOORSMIN_MEDI as main_FLOORSMIN_MEDI_80," +
            "    LANDAREA_MEDI as main_LANDAREA_MEDI_81," +
            "    LIVINGAPARTMENTS_MEDI as main_LIVINGAPARTMENTS_MEDI_82," +
            "    LIVINGAREA_MEDI as main_LIVINGAREA_MEDI_83," +
            "    NONLIVINGAPARTMENTS_MEDI as main_NONLIVINGAPARTMENTS_MEDI_84," +
            "    NONLIVINGAREA_MEDI as main_NONLIVINGAREA_MEDI_85," +
            "    FONDKAPREMONT_MODE as main_FONDKAPREMONT_MODE_86," +
            "    HOUSETYPE_MODE as main_HOUSETYPE_MODE_87," +
            "    TOTALAREA_MODE as main_TOTALAREA_MODE_88," +
            "    WALLSMATERIAL_MODE as main_WALLSMATERIAL_MODE_89," +
            "    EMERGENCYSTATE_MODE as main_EMERGENCYSTATE_MODE_90," +
            "    OBS_30_CNT_SOCIAL_CIRCLE as main_OBS_30_CNT_SOCIAL_CIRCLE_91," +
            "    DEF_30_CNT_SOCIAL_CIRCLE as main_DEF_30_CNT_SOCIAL_CIRCLE_92," +
            "    OBS_60_CNT_SOCIAL_CIRCLE as main_OBS_60_CNT_SOCIAL_CIRCLE_93," +
            "    DEF_60_CNT_SOCIAL_CIRCLE as main_DEF_60_CNT_SOCIAL_CIRCLE_94," +
            "    DAYS_LAST_PHONE_CHANGE as main_DAYS_LAST_PHONE_CHANGE_95," +
            "    FLAG_DOCUMENT_2 as main_FLAG_DOCUMENT_2_96," +
            "    FLAG_DOCUMENT_3 as main_FLAG_DOCUMENT_3_97," +
            "    FLAG_DOCUMENT_4 as main_FLAG_DOCUMENT_4_98," +
            "    FLAG_DOCUMENT_5 as main_FLAG_DOCUMENT_5_99," +
            "    FLAG_DOCUMENT_6 as main_FLAG_DOCUMENT_6_100," +
            "    FLAG_DOCUMENT_7 as main_FLAG_DOCUMENT_7_101," +
            "    FLAG_DOCUMENT_8 as main_FLAG_DOCUMENT_8_102," +
            "    FLAG_DOCUMENT_9 as main_FLAG_DOCUMENT_9_103," +
            "    FLAG_DOCUMENT_10 as main_FLAG_DOCUMENT_10_104," +
            "    FLAG_DOCUMENT_11 as main_FLAG_DOCUMENT_11_105," +
            "    FLAG_DOCUMENT_12 as main_FLAG_DOCUMENT_12_106," +
            "    FLAG_DOCUMENT_13 as main_FLAG_DOCUMENT_13_107," +
            "    FLAG_DOCUMENT_14 as main_FLAG_DOCUMENT_14_108," +
            "    FLAG_DOCUMENT_15 as main_FLAG_DOCUMENT_15_109," +
            "    FLAG_DOCUMENT_16 as main_FLAG_DOCUMENT_16_110," +
            "    FLAG_DOCUMENT_17 as main_FLAG_DOCUMENT_17_111," +
            "    FLAG_DOCUMENT_18 as main_FLAG_DOCUMENT_18_112," +
            "    FLAG_DOCUMENT_19 as main_FLAG_DOCUMENT_19_113," +
            "    FLAG_DOCUMENT_20 as main_FLAG_DOCUMENT_20_114," +
            "    FLAG_DOCUMENT_21 as main_FLAG_DOCUMENT_21_115," +
            "    AMT_REQ_CREDIT_BUREAU_HOUR as main_AMT_REQ_CREDIT_BUREAU_HOUR_116," +
            "    AMT_REQ_CREDIT_BUREAU_DAY as main_AMT_REQ_CREDIT_BUREAU_DAY_117," +
            "    AMT_REQ_CREDIT_BUREAU_WEEK as main_AMT_REQ_CREDIT_BUREAU_WEEK_118," +
            "    AMT_REQ_CREDIT_BUREAU_MON as main_AMT_REQ_CREDIT_BUREAU_MON_119," +
            "    AMT_REQ_CREDIT_BUREAU_QRT as main_AMT_REQ_CREDIT_BUREAU_QRT_120," +
            "    AMT_REQ_CREDIT_BUREAU_YEAR as main_AMT_REQ_CREDIT_BUREAU_YEAR_121," +
            "    `time` as main_time_122 " +
            "from" +
            "    main" +
            "    )" +
            "as out0 " +
            "last join " +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_124," +
            "    count(CNT_INSTALMENT) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_ID_CURR_time_0s_32d_CNT," +
            "    max(CNT_INSTALMENT) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_123," +
            "    avg(CNT_INSTALMENT) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_124," +
            "    max(CNT_INSTALMENT_FUTURE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_FUTURE_125," +
            "    avg(CNT_INSTALMENT_FUTURE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_FUTURE_126," +
            "    min(MONTHS_BALANCE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_MONTHS_BALANCE_127," +
            "    avg(MONTHS_BALANCE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_MONTHS_BALANCE_128," +
            "    avg(SK_DPD) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_129," +
            "    min(SK_DPD) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_130," +
            "    max(SK_DPD_DEF) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_DEF_131," +
            "    avg(SK_DPD_DEF) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_DEF_132," +
            "    distinct_count(NAME_CONTRACT_STATUS) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_NAME_CONTRACT_STATUS_133," +
            "    distinct_count(SK_ID_PREV) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_ID_PREV_134 " +
            "from" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, bigint(0) as MONTHS_BALANCE, double(0) as CNT_INSTALMENT, double(0) as CNT_INSTALMENT_FUTURE, '' as NAME_CONTRACT_STATUS, bigint(0) as SK_DPD, bigint(0) as SK_DPD_DEF, `time` as `time` from main)" +
            "    window POS_CASH_balance_SK_ID_CURR_time_0s_32d as (" +
            "UNION POS_CASH_balance partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))" +
            "as out1 " +
            "on SK_ID_CURR_1 = out1.SK_ID_CURR_124 " +
            "last join" +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_136," +
            "    count(AMT_ANNUITY) over bureau_SK_ID_CURR_time_0s_32d as bureau_SK_ID_CURR_time_0s_32d_CNT," +
            "    min(AMT_ANNUITY) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_ANNUITY_135," +
            "    avg(AMT_ANNUITY) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_ANNUITY_136," +
            "    max(AMT_CREDIT_MAX_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_MAX_OVERDUE_137," +
            "    avg(AMT_CREDIT_MAX_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_MAX_OVERDUE_138," +
            "    avg(AMT_CREDIT_SUM) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_139," +
            "    min(AMT_CREDIT_SUM) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_140," +
            "    avg(AMT_CREDIT_SUM_DEBT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_DEBT_141," +
            "    max(AMT_CREDIT_SUM_DEBT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_DEBT_142," +
            "    min(AMT_CREDIT_SUM_LIMIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_LIMIT_143," +
            "    max(AMT_CREDIT_SUM_LIMIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_LIMIT_144," +
            "    max(AMT_CREDIT_SUM_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_OVERDUE_145," +
            "    avg(AMT_CREDIT_SUM_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_OVERDUE_146," +
            "    max(CNT_CREDIT_PROLONG) over bureau_SK_ID_CURR_time_0s_32d as bureau_CNT_CREDIT_PROLONG_147," +
            "    min(CNT_CREDIT_PROLONG) over bureau_SK_ID_CURR_time_0s_32d as bureau_CNT_CREDIT_PROLONG_148," +
            "    max(CREDIT_DAY_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_DAY_OVERDUE_149," +
            "    avg(CREDIT_DAY_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_DAY_OVERDUE_150," +
            "    max(DAYS_CREDIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_151," +
            "    min(DAYS_CREDIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_152," +
            "    max(DAYS_CREDIT_ENDDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_ENDDATE_153," +
            "    min(DAYS_CREDIT_ENDDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_ENDDATE_154," +
            "    max(DAYS_CREDIT_UPDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_UPDATE_155," +
            "    min(DAYS_CREDIT_UPDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_UPDATE_156," +
            "    max(DAYS_ENDDATE_FACT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_ENDDATE_FACT_157," +
            "    min(DAYS_ENDDATE_FACT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_ENDDATE_FACT_158," +
            "    distinct_count(CREDIT_ACTIVE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_ACTIVE_159," +
            "    distinct_count(CREDIT_CURRENCY) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_CURRENCY_160," +
            "    distinct_count(CREDIT_TYPE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_TYPE_161 " +
            "from" +
            "    (select SK_ID_CURR as SK_ID_CURR, bigint(0) as SK_ID_BUREAU, '' as CREDIT_ACTIVE, '' as CREDIT_CURRENCY, bigint(0) as DAYS_CREDIT, bigint(0) as CREDIT_DAY_OVERDUE, double(0) as DAYS_CREDIT_ENDDATE, double(0) as DAYS_ENDDATE_FACT, double(0) as AMT_CREDIT_MAX_OVERDUE, bigint(0) as CNT_CREDIT_PROLONG, double(0) as AMT_CREDIT_SUM, double(0) as AMT_CREDIT_SUM_DEBT, double(0) as AMT_CREDIT_SUM_LIMIT, double(0) as AMT_CREDIT_SUM_OVERDUE, '' as CREDIT_TYPE, bigint(0) as DAYS_CREDIT_UPDATE, double(0) as AMT_ANNUITY, `time` as `time` from main)" +
            "    window bureau_SK_ID_CURR_time_0s_32d as (" +
            "UNION bureau partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))" +
            "as out2 " +
            "on SK_ID_CURR_1 = out2.SK_ID_CURR_136 " +
            "last join" +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_163," +
            "    count(MONTHS_BALANCE) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_SK_ID_CURR_time_0s_32d_CNT," +
            "    max(MONTHS_BALANCE) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_MONTHS_BALANCE_162," +
            "    avg(MONTHS_BALANCE) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_MONTHS_BALANCE_163," +
            "    distinct_count(`STATUS`) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_STATUS_164 " +
            "from" +
            "    (select bigint(0) as SK_ID_BUREAU, bigint(0) as MONTHS_BALANCE, '' as `STATUS`, SK_ID_CURR as SK_ID_CURR, `time` as `time` from main)" +
            "    window bureau_balance_SK_ID_CURR_time_0s_32d as (" +
            "UNION bureau_balance partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))" +
            "as out3 " +
            "on SK_ID_CURR_1 = out3.SK_ID_CURR_163 " +
            "last join" +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_166," +
            "    count(AMT_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_ID_CURR_time_0s_32d_CNT," +
            "    avg(AMT_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_BALANCE_165," +
            "    max(AMT_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_BALANCE_166," +
            "    max(AMT_CREDIT_LIMIT_ACTUAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_CREDIT_LIMIT_ACTUAL_167," +
            "    avg(AMT_CREDIT_LIMIT_ACTUAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_CREDIT_LIMIT_ACTUAL_168," +
            "    avg(AMT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_ATM_CURRENT_169," +
            "    max(AMT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_ATM_CURRENT_170," +
            "    avg(AMT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_CURRENT_171," +
            "    max(AMT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_CURRENT_172," +
            "    max(AMT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_OTHER_CURRENT_173," +
            "    avg(AMT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_OTHER_CURRENT_174," +
            "    max(AMT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_POS_CURRENT_175," +
            "    avg(AMT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_POS_CURRENT_176," +
            "    avg(AMT_INST_MIN_REGULARITY) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_INST_MIN_REGULARITY_177," +
            "    max(AMT_INST_MIN_REGULARITY) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_INST_MIN_REGULARITY_178," +
            "    max(AMT_PAYMENT_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_CURRENT_179," +
            "    avg(AMT_PAYMENT_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_CURRENT_180," +
            "    avg(AMT_PAYMENT_TOTAL_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_TOTAL_CURRENT_181," +
            "    max(AMT_PAYMENT_TOTAL_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_TOTAL_CURRENT_182," +
            "    avg(AMT_RECEIVABLE_PRINCIPAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECEIVABLE_PRINCIPAL_183," +
            "    max(AMT_RECEIVABLE_PRINCIPAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECEIVABLE_PRINCIPAL_184," +
            "    avg(AMT_RECIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECIVABLE_185," +
            "    max(AMT_RECIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECIVABLE_186," +
            "    avg(AMT_TOTAL_RECEIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_TOTAL_RECEIVABLE_187," +
            "    max(AMT_TOTAL_RECEIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_TOTAL_RECEIVABLE_188," +
            "    avg(CNT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_ATM_CURRENT_189," +
            "    max(CNT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_ATM_CURRENT_190," +
            "    avg(CNT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_CURRENT_191," +
            "    max(CNT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_CURRENT_192," +
            "    max(CNT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_OTHER_CURRENT_193," +
            "    avg(CNT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_OTHER_CURRENT_194," +
            "    max(CNT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_POS_CURRENT_195," +
            "    avg(CNT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_POS_CURRENT_196," +
            "    max(CNT_INSTALMENT_MATURE_CUM) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_INSTALMENT_MATURE_CUM_197," +
            "    avg(CNT_INSTALMENT_MATURE_CUM) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_INSTALMENT_MATURE_CUM_198," +
            "    min(MONTHS_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_MONTHS_BALANCE_199," +
            "    avg(MONTHS_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_MONTHS_BALANCE_200," +
            "    min(SK_DPD) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_201," +
            "    avg(SK_DPD) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_202," +
            "    min(SK_DPD_DEF) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_DEF_203," +
            "    avg(SK_DPD_DEF) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_DEF_204," +
            "    distinct_count(NAME_CONTRACT_STATUS) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_NAME_CONTRACT_STATUS_205," +
            "    distinct_count(SK_ID_PREV) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_ID_PREV_206 " +
            "from" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, bigint(0) as MONTHS_BALANCE, double(0) as AMT_BALANCE, bigint(0) as AMT_CREDIT_LIMIT_ACTUAL, double(0) as AMT_DRAWINGS_ATM_CURRENT, double(0) as AMT_DRAWINGS_CURRENT, double(0) as AMT_DRAWINGS_OTHER_CURRENT, double(0) as AMT_DRAWINGS_POS_CURRENT, double(0) as AMT_INST_MIN_REGULARITY, double(0) as AMT_PAYMENT_CURRENT, double(0) as AMT_PAYMENT_TOTAL_CURRENT, double(0) as AMT_RECEIVABLE_PRINCIPAL, double(0) as AMT_RECIVABLE, double(0) as AMT_TOTAL_RECEIVABLE, double(0) as CNT_DRAWINGS_ATM_CURRENT, bigint(0) as CNT_DRAWINGS_CURRENT, double(0) as CNT_DRAWINGS_OTHER_CURRENT, double(0) as CNT_DRAWINGS_POS_CURRENT, double(0) as CNT_INSTALMENT_MATURE_CUM, '' as NAME_CONTRACT_STATUS, bigint(0) as SK_DPD, bigint(0) as SK_DPD_DEF, `time` as `time` from main)" +
            "    window credit_card_balance_SK_ID_CURR_time_0s_32d as (" +
            "UNION credit_card_balance partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))" +
            "as out4 " +
            "on SK_ID_CURR_1 = out4.SK_ID_CURR_166 " +
            "last join " +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_208," +
            "    count(AMT_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_SK_ID_CURR_time_0s_32d_CNT," +
            "    max(AMT_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_INSTALMENT_207," +
            "    avg(AMT_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_INSTALMENT_208," +
            "    max(AMT_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_PAYMENT_209," +
            "    min(AMT_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_PAYMENT_210," +
            "    avg(DAYS_ENTRY_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_ENTRY_PAYMENT_211," +
            "    max(DAYS_ENTRY_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_ENTRY_PAYMENT_212," +
            "    avg(DAYS_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_INSTALMENT_213," +
            "    min(DAYS_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_INSTALMENT_214," +
            "    max(NUM_INSTALMENT_NUMBER) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_NUMBER_215," +
            "    avg(NUM_INSTALMENT_NUMBER) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_NUMBER_216," +
            "    avg(NUM_INSTALMENT_VERSION) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_VERSION_217," +
            "    min(NUM_INSTALMENT_VERSION) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_VERSION_218," +
            "    distinct_count(SK_ID_PREV) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_SK_ID_PREV_219 " +
            "from" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, double(0) as NUM_INSTALMENT_VERSION, bigint(0) as NUM_INSTALMENT_NUMBER, double(0) as DAYS_INSTALMENT, double(0) as DAYS_ENTRY_PAYMENT, double(0) as AMT_INSTALMENT, double(0) as AMT_PAYMENT, `time` as `time` from main)" +
            "    window installments_payments_SK_ID_CURR_time_0s_32d as (" +
            "UNION installments_payments partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))" +
            "as out5 " +
            "on SK_ID_CURR_1 = out5.SK_ID_CURR_208 " +
            "last join" +
            "(" +
            "select" +
            "    SK_ID_CURR as SK_ID_CURR_221," +
            "    count(AMT_ANNUITY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SK_ID_CURR_time_0s_32d_CNT," +
            "    max(AMT_ANNUITY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_ANNUITY_220," +
            "    min(AMT_ANNUITY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_ANNUITY_221," +
            "    max(AMT_APPLICATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_APPLICATION_222," +
            "    min(AMT_APPLICATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_APPLICATION_223," +
            "    max(AMT_CREDIT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_CREDIT_224," +
            "    min(AMT_CREDIT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_CREDIT_225," +
            "    max(AMT_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_DOWN_PAYMENT_226," +
            "    avg(AMT_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_DOWN_PAYMENT_227," +
            "    max(AMT_GOODS_PRICE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_GOODS_PRICE_228," +
            "    min(AMT_GOODS_PRICE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_GOODS_PRICE_229," +
            "    max(CNT_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CNT_PAYMENT_230," +
            "    avg(CNT_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CNT_PAYMENT_231," +
            "    avg(DAYS_DECISION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_DECISION_232," +
            "    min(DAYS_DECISION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_DECISION_233," +
            "    avg(DAYS_FIRST_DRAWING) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DRAWING_234," +
            "    min(DAYS_FIRST_DRAWING) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DRAWING_235," +
            "    avg(DAYS_FIRST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DUE_236," +
            "    max(DAYS_FIRST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DUE_237," +
            "    avg(DAYS_LAST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_238," +
            "    max(DAYS_LAST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_239," +
            "    max(DAYS_LAST_DUE_1ST_VERSION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_1ST_VERSION_240," +
            "    avg(DAYS_LAST_DUE_1ST_VERSION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_1ST_VERSION_241," +
            "    max(DAYS_TERMINATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_TERMINATION_242," +
            "    avg(DAYS_TERMINATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_TERMINATION_243," +
            "    max(HOUR_APPR_PROCESS_START) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_HOUR_APPR_PROCESS_START_244," +
            "    avg(HOUR_APPR_PROCESS_START) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_HOUR_APPR_PROCESS_START_245," +
            "    avg(NFLAG_INSURED_ON_APPROVAL) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_INSURED_ON_APPROVAL_246," +
            "    min(NFLAG_INSURED_ON_APPROVAL) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_INSURED_ON_APPROVAL_247," +
            "    min(NFLAG_LAST_APPL_IN_DAY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_LAST_APPL_IN_DAY_248," +
            "    max(NFLAG_LAST_APPL_IN_DAY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_LAST_APPL_IN_DAY_249," +
            "    avg(RATE_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_DOWN_PAYMENT_250," +
            "    max(RATE_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_DOWN_PAYMENT_251," +
            "    min(RATE_INTEREST_PRIMARY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIMARY_252," +
            "    avg(RATE_INTEREST_PRIMARY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIMARY_253," +
            "    min(RATE_INTEREST_PRIVILEGED) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIVILEGED_254," +
            "    max(RATE_INTEREST_PRIVILEGED) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIVILEGED_255," +
            "    min(SELLERPLACE_AREA) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SELLERPLACE_AREA_256," +
            "    avg(SELLERPLACE_AREA) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SELLERPLACE_AREA_257," +
            "    distinct_count(CHANNEL_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CHANNEL_TYPE_258," +
            "    distinct_count(CODE_REJECT_REASON) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CODE_REJECT_REASON_259," +
            "    distinct_count(FLAG_LAST_APPL_PER_CONTRACT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_FLAG_LAST_APPL_PER_CONTRACT_260," +
            "    distinct_count(NAME_CASH_LOAN_PURPOSE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CASH_LOAN_PURPOSE_261," +
            "    distinct_count(NAME_CLIENT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CLIENT_TYPE_262," +
            "    distinct_count(NAME_CONTRACT_STATUS) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CONTRACT_STATUS_263," +
            "    distinct_count(NAME_CONTRACT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CONTRACT_TYPE_264," +
            "    distinct_count(NAME_GOODS_CATEGORY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_GOODS_CATEGORY_265," +
            "    distinct_count(NAME_PAYMENT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_PAYMENT_TYPE_266," +
            "    distinct_count(NAME_PORTFOLIO) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_PORTFOLIO_267," +
            "    distinct_count(NAME_PRODUCT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_PRODUCT_TYPE_268," +
            "    distinct_count(NAME_SELLER_INDUSTRY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_SELLER_INDUSTRY_269," +
            "    distinct_count(NAME_TYPE_SUITE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_TYPE_SUITE_270," +
            "    distinct_count(NAME_YIELD_GROUP) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_YIELD_GROUP_271," +
            "    distinct_count(PRODUCT_COMBINATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_PRODUCT_COMBINATION_272," +
            "    distinct_count(SK_ID_PREV) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SK_ID_PREV_273," +
            "    distinct_count(WEEKDAY_APPR_PROCESS_START) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_WEEKDAY_APPR_PROCESS_START_274 " +
            "from" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, '' as NAME_CONTRACT_TYPE, double(0) as AMT_ANNUITY, double(0) as AMT_APPLICATION, double(0) as AMT_CREDIT, double(0) as AMT_DOWN_PAYMENT, double(0) as AMT_GOODS_PRICE, '' as WEEKDAY_APPR_PROCESS_START, bigint(0) as HOUR_APPR_PROCESS_START, '' as FLAG_LAST_APPL_PER_CONTRACT, bigint(0) as NFLAG_LAST_APPL_IN_DAY, double(0) as RATE_DOWN_PAYMENT, double(0) as RATE_INTEREST_PRIMARY, double(0) as RATE_INTEREST_PRIVILEGED, '' as NAME_CASH_LOAN_PURPOSE, '' as NAME_CONTRACT_STATUS, bigint(0) as DAYS_DECISION, '' as NAME_PAYMENT_TYPE, '' as CODE_REJECT_REASON, '' as NAME_TYPE_SUITE, '' as NAME_CLIENT_TYPE, '' as NAME_GOODS_CATEGORY, '' as NAME_PORTFOLIO, '' as NAME_PRODUCT_TYPE, '' as CHANNEL_TYPE, bigint(0) as SELLERPLACE_AREA, '' as NAME_SELLER_INDUSTRY, double(0) as CNT_PAYMENT, '' as NAME_YIELD_GROUP, '' as PRODUCT_COMBINATION, double(0) as DAYS_FIRST_DRAWING, double(0) as DAYS_FIRST_DUE, double(0) as DAYS_LAST_DUE_1ST_VERSION, double(0) as DAYS_LAST_DUE, double(0) as DAYS_TERMINATION, double(0) as NFLAG_INSURED_ON_APPROVAL, `time` as `time` from main)" +
            "    window previous_application_SK_ID_CURR_time_0s_32d as (" +
            "UNION previous_application partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))" +
            "as out6 " +
            "on SK_ID_CURR_1 = out6.SK_ID_CURR_221" +
            ";";

    public FESQLClusterBenchmark() {
        this(false);
    }
    public FESQLClusterBenchmark(boolean enableDebug) {
        SdkOption sdkOption = new SdkOption();
        sdkOption.setZkSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        sdkOption.setEnableDebug(enableDebug);
        this.option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getTableName(String sql) {
        String[] arr = sql.split("\\(");
        return arr[0].split(" ")[2].replaceAll("`", "");
    }

    private List<String> getSchema(String sql) {
        String[] arr = sql.split("\\(");
        String[] filed = arr[1].split(",");
        List<String> schema = new ArrayList<>();
        for (int i = 0; i < filed.length; i++) {
            String[] tmp = filed[i].split(" ");
            if (tmp.length < 2) {
                continue;
            }
            schema.add(tmp[1].trim());
        }
        return schema;
    }

    private int getPos(String sql, String delimiter) {
        String key = sql.split(delimiter)[1].split(",")[0].replaceAll("`", "").replaceAll("\\(", "").replaceAll("\\)", "");
        String[] arr = sql.split("\\(");
        String[] fields = arr[1].split(",");
        int pos = 0;
        for (int i = 0; i < fields.length; i++) {
            String[] tmp = fields[i].split(" ");
            if (tmp.length < 2) {
                continue;
            }
            String field = tmp[0].replaceAll("`", "");
            if (field.equals(key)) {
                return pos;
            }
            pos++;
        }
        return -1;
    }

    private int getTsPos(String sql) {
        return getPos(sql, "ts=");
    }

    private int getIndexPos(String sql) {
        return getPos(sql, "key=");
    }

    private void putData(String sql, int pkNum, int windowNum) {
        logger.info("putData sql {}, pkNum {}, windowNum {}", sql, pkNum, windowNum);
        String name = getTableName(sql);
        int indexPos = getIndexPos(sql);
        int tsPos = getTsPos(sql);
        List<String> schema = getSchema(sql);
        long pkBase = 1000000l;
        for (int i = 0; i < pkNum; i++) {
            long ts = System.currentTimeMillis();
            for (int tsCnt = 0; tsCnt < windowNum; tsCnt++) {
                StringBuilder builder = new StringBuilder();
                builder.append("insert into ");
                builder.append(name);
                builder.append(" values(");
                for (int pos = 0; pos < schema.size(); pos++) {
                    if (pos > 0) {
                        builder.append(", ");
                    }
                    String type = schema.get(pos);
                    if (type.equals("string")) {
                        builder.append("'");
                        builder.append("col");
                        builder.append(pos);
                        if (pos == indexPos) {
                            builder.append(i);
                        }
                        builder.append("'");
                    } else if (type.equals("float")) {
                        builder.append(1.3);
                    } else if (type.equals("double")) {
                        builder.append("1.4");
                    } else if (type.equals("bigint") || type.equals("timestamp") || type.equals("int")) {
                        if (pos == indexPos) {
                            builder.append(pkBase + i);
                        } else if (pos == tsPos) {
                            builder.append(ts + tsCnt);
                        } else {
                            if (type.equals("timestamp")) {
                                builder.append(ts);
                            } else {
                                builder.append(pos);
                            }
                        }
                    } else if (type.equals("bool")) {
                        builder.append(true);
                    } else {
                        System.out.println("invalid type");
                    }
                }
                builder.append(");");
                String exeSql = builder.toString();
                executor.executeInsert(db, exeSql);
            }
        }
    }

    private PreparedStatement getPreparedStatement() throws SQLException {
        PreparedStatement requestPs = executor.getRequestPreparedStmt(db, benSql);
        ResultSetMetaData metaData = requestPs.getMetaData();
        long pkBase = 1000000l;
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            int columnType = metaData.getColumnType(i + 1);
            if (columnName.equals("SK_ID_CURR")) {
                requestPs.setLong(i + 1, pkBase);
            } else if (columnType == Types.VARCHAR) {
                requestPs.setString(i + 1, "col" + String.valueOf(i));
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, 1.4d);
            } else if (columnType == Types.INTEGER) {
                requestPs.setInt(i + 1, i);
            } else if (columnType == Types.BIGINT) {
                requestPs.setLong(i + 1, i);
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(System.currentTimeMillis() + 1000));
            }
        }
        return requestPs;
    }

    @Setup
    public void setup() throws SQLException {
        if (!executor.createDB(db)) {
            return;
        }
        if (!executor.executeDDL(db, previousApplication)) {
            return;
        }
        if (!executor.executeDDL(db, posCashBalance)) {
            return;
        }
        if (!executor.executeDDL(db, installmentsPayments)) {
            return;
        }
        if (!executor.executeDDL(db, bureauBalance)) {
            return;
        }
        if (!executor.executeDDL(db, main)) {
            return;
        }
        if (!executor.executeDDL(db, creditCardBalance)) {
            return;
        }
        if (!executor.executeDDL(db, bureau)) {
            return;
        }
        putData(previousApplication, pkNum, windowNum);
        putData(posCashBalance, pkNum, windowNum);
        putData(installmentsPayments, pkNum, windowNum);
        putData(bureauBalance, pkNum, windowNum);
        putData(main, pkNum, windowNum);
        putData(creditCardBalance, pkNum, windowNum);
        putData(bureau, pkNum, windowNum);
    }

    @TearDown
    public void teardown() throws SQLException {
        executor.executeDDL(db, "drop table previous_application;");
        executor.executeDDL(db, "drop table POS_CASH_balance;");
        executor.executeDDL(db, "drop table installments_payments;");
        executor.executeDDL(db, "drop table bureau_balance;");
        executor.executeDDL(db, "drop table main;");
        executor.executeDDL(db, "drop table credit_card_balance;");
        executor.executeDDL(db, "drop table bureau;");
        executor.dropDB(db);
    }
    public void setWindowNum(int windowNum) {
        this.windowNum = windowNum;
    }
    public Map<String, String> execSQLTest() {
        try {
            PreparedStatement ps = getPreparedStatement();
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, String> val = new HashMap<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    val.put(columnName, String.valueOf(resultSet.getString(i + 1)));
                } else if (columnType == Types.DOUBLE) {
                    val.put(columnName, String.valueOf(resultSet.getDouble(i + 1)));
                } else if (columnType == Types.INTEGER) {
                    val.put(columnName, String.valueOf(resultSet.getInt(i + 1)));
                } else if (columnType == Types.BIGINT) {
                    val.put(columnName, String.valueOf(resultSet.getLong(i + 1)));
                } else if (columnType == Types.TIMESTAMP) {
                    val.put(columnName, String.valueOf(resultSet.getTimestamp(i + 1)));
                }
            }
            logger.info("result map: {}", val);
            ps.close();
            return val;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    @Benchmark
    public void execSQL() {
        try {
            PreparedStatement ps = getPreparedStatement();
            ResultSet resultSet = ps.executeQuery();
            /*resultSet.next();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, String> val = new HashMap<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    val.put(columnName, String.valueOf(resultSet.getString(i + 1)));
                } else if (columnType == Types.DOUBLE) {
                    val.put(columnName, String.valueOf(resultSet.getDouble(i + 1)));
                } else if (columnType == Types.INTEGER) {
                    val.put(columnName, String.valueOf(resultSet.getInt(i + 1)));
                } else if (columnType == Types.BIGINT) {
                    val.put(columnName, String.valueOf(resultSet.getLong(i + 1)));
                } else if (columnType == Types.TIMESTAMP) {
                    val.put(columnName, String.valueOf(resultSet.getTimestamp(i + 1)));
                }
            }*/
            ps.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws RunnerException {
      /*  FESQLClusterBenchmark ben = new FESQLClusterBenchmark();
        try {
            ben.setup();
            ben.execSQL();
            ben.teardown();
        } catch (Exception e) {

        }*/
        Options opt = new OptionsBuilder()
                .include(FESQLClusterBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
