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

import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComplexSqlExample extends BaseExample {
    private static final Logger logger = LoggerFactory.getLogger(ComplexSqlExample.class);

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
            "index(key=(SK_ID_CURR), ts=`time`)\n" +
            ");\n" +
            "create table previous_application(\n" +
            "SK_ID_PREV bigint,\n" +
            "SK_ID_CURR bigint,\n" +
            "NAME_CONTRACT_TYPE string,\n" +
            "AMT_ANNUITY double,\n" +
            "AMT_APPLICATION double,\n" +
            "AMT_CREDIT double,\n" +
            "AMT_DOWN_PAYMENT double,\n" +
            "AMT_GOODS_PRICE double,\n" +
            "WEEKDAY_APPR_PROCESS_START string,\n" +
            "HOUR_APPR_PROCESS_START bigint,\n" +
            "FLAG_LAST_APPL_PER_CONTRACT string,\n" +
            "NFLAG_LAST_APPL_IN_DAY bigint,\n" +
            "RATE_DOWN_PAYMENT double,\n" +
            "RATE_INTEREST_PRIMARY double,\n" +
            "RATE_INTEREST_PRIVILEGED double,\n" +
            "NAME_CASH_LOAN_PURPOSE string,\n" +
            "NAME_CONTRACT_STATUS string,\n" +
            "DAYS_DECISION bigint,\n" +
            "NAME_PAYMENT_TYPE string,\n" +
            "CODE_REJECT_REASON string,\n" +
            "NAME_TYPE_SUITE string,\n" +
            "NAME_CLIENT_TYPE string,\n" +
            "NAME_GOODS_CATEGORY string,\n" +
            "NAME_PORTFOLIO string,\n" +
            "NAME_PRODUCT_TYPE string,\n" +
            "CHANNEL_TYPE string,\n" +
            "SELLERPLACE_AREA bigint,\n" +
            "NAME_SELLER_INDUSTRY string,\n" +
            "CNT_PAYMENT double,\n" +
            "NAME_YIELD_GROUP string,\n" +
            "PRODUCT_COMBINATION string,\n" +
            "DAYS_FIRST_DRAWING double,\n" +
            "DAYS_FIRST_DUE double,\n" +
            "DAYS_LAST_DUE_1ST_VERSION double,\n" +
            "DAYS_LAST_DUE double,\n" +
            "DAYS_TERMINATION double,\n" +
            "NFLAG_INSURED_ON_APPROVAL double,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=0m)\n" +
            ");\n" +
            "create table POS_CASH_balance(\n" +
            "SK_ID_PREV bigint,\n" +
            "SK_ID_CURR bigint,\n" +
            "MONTHS_BALANCE bigint,\n" +
            "CNT_INSTALMENT double,\n" +
            "CNT_INSTALMENT_FUTURE double,\n" +
            "NAME_CONTRACT_STATUS string,\n" +
            "SK_DPD bigint,\n" +
            "SK_DPD_DEF bigint,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=0m)\n" +
            ");\n" +
            "create table installments_payments(\n" +
            "SK_ID_PREV bigint,\n" +
            "SK_ID_CURR bigint,\n" +
            "NUM_INSTALMENT_VERSION double,\n" +
            "NUM_INSTALMENT_NUMBER bigint,\n" +
            "DAYS_INSTALMENT double,\n" +
            "DAYS_ENTRY_PAYMENT double,\n" +
            "AMT_INSTALMENT double,\n" +
            "AMT_PAYMENT double,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=0m)\n" +
            ");\n" +
            "create table bureau_balance(\n" +
            "SK_ID_BUREAU bigint,\n" +
            "MONTHS_BALANCE bigint,\n" +
            "`STATUS` string,\n" +
            "SK_ID_CURR bigint,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=0m)\n" +
            ");\n" +
            "create table credit_card_balance(\n" +
            "SK_ID_PREV bigint,\n" +
            "SK_ID_CURR bigint,\n" +
            "MONTHS_BALANCE bigint,\n" +
            "AMT_BALANCE double,\n" +
            "AMT_CREDIT_LIMIT_ACTUAL bigint,\n" +
            "AMT_DRAWINGS_ATM_CURRENT double,\n" +
            "AMT_DRAWINGS_CURRENT double,\n" +
            "AMT_DRAWINGS_OTHER_CURRENT double,\n" +
            "AMT_DRAWINGS_POS_CURRENT double,\n" +
            "AMT_INST_MIN_REGULARITY double,\n" +
            "AMT_PAYMENT_CURRENT double,\n" +
            "AMT_PAYMENT_TOTAL_CURRENT double,\n" +
            "AMT_RECEIVABLE_PRINCIPAL double,\n" +
            "AMT_RECIVABLE double,\n" +
            "AMT_TOTAL_RECEIVABLE double,\n" +
            "CNT_DRAWINGS_ATM_CURRENT double,\n" +
            "CNT_DRAWINGS_CURRENT bigint,\n" +
            "CNT_DRAWINGS_OTHER_CURRENT double,\n" +
            "CNT_DRAWINGS_POS_CURRENT double,\n" +
            "CNT_INSTALMENT_MATURE_CUM double,\n" +
            "NAME_CONTRACT_STATUS string,\n" +
            "SK_DPD bigint,\n" +
            "SK_DPD_DEF bigint,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=0m)\n" +
            ");\n" +
            "create table bureau(\n" +
            "SK_ID_CURR bigint,\n" +
            "SK_ID_BUREAU bigint,\n" +
            "CREDIT_ACTIVE string,\n" +
            "CREDIT_CURRENCY string,\n" +
            "DAYS_CREDIT bigint,\n" +
            "CREDIT_DAY_OVERDUE bigint,\n" +
            "DAYS_CREDIT_ENDDATE double,\n" +
            "DAYS_ENDDATE_FACT double,\n" +
            "AMT_CREDIT_MAX_OVERDUE double,\n" +
            "CNT_CREDIT_PROLONG bigint,\n" +
            "AMT_CREDIT_SUM double,\n" +
            "AMT_CREDIT_SUM_DEBT double,\n" +
            "AMT_CREDIT_SUM_LIMIT double,\n" +
            "AMT_CREDIT_SUM_OVERDUE double,\n" +
            "CREDIT_TYPE string,\n" +
            "DAYS_CREDIT_UPDATE bigint,\n" +
            "AMT_ANNUITY double,\n" +
            "`time` timestamp,\n" +
            "index(key=(SK_ID_CURR), ts=`time`, ttl=0m)\n" +
            ");";

    String sql = "\n" +
            "select * from \n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_1,\n" +
            "    SK_ID_CURR as main_SK_ID_CURR_0,\n" +
            "    TARGET as main_TARGET_1,\n" +
            "    NAME_CONTRACT_TYPE as main_NAME_CONTRACT_TYPE_2,\n" +
            "    CODE_GENDER as main_CODE_GENDER_3,\n" +
            "    FLAG_OWN_CAR as main_FLAG_OWN_CAR_4,\n" +
            "    FLAG_OWN_REALTY as main_FLAG_OWN_REALTY_5,\n" +
            "    CNT_CHILDREN as main_CNT_CHILDREN_6,\n" +
            "    AMT_INCOME_TOTAL as main_AMT_INCOME_TOTAL_7,\n" +
            "    AMT_CREDIT as main_AMT_CREDIT_8,\n" +
            "    AMT_ANNUITY as main_AMT_ANNUITY_9,\n" +
            "    AMT_GOODS_PRICE as main_AMT_GOODS_PRICE_10,\n" +
            "    NAME_TYPE_SUITE as main_NAME_TYPE_SUITE_11,\n" +
            "    NAME_INCOME_TYPE as main_NAME_INCOME_TYPE_12,\n" +
            "    NAME_EDUCATION_TYPE as main_NAME_EDUCATION_TYPE_13,\n" +
            "    NAME_FAMILY_STATUS as main_NAME_FAMILY_STATUS_14,\n" +
            "    NAME_HOUSING_TYPE as main_NAME_HOUSING_TYPE_15,\n" +
            "    REGION_POPULATION_RELATIVE as main_REGION_POPULATION_RELATIVE_16,\n" +
            "    DAYS_BIRTH as main_DAYS_BIRTH_17,\n" +
            "    DAYS_EMPLOYED as main_DAYS_EMPLOYED_18,\n" +
            "    DAYS_REGISTRATION as main_DAYS_REGISTRATION_19,\n" +
            "    DAYS_ID_PUBLISH as main_DAYS_ID_PUBLISH_20,\n" +
            "    OWN_CAR_AGE as main_OWN_CAR_AGE_21,\n" +
            "    FLAG_MOBIL as main_FLAG_MOBIL_22,\n" +
            "    FLAG_EMP_PHONE as main_FLAG_EMP_PHONE_23,\n" +
            "    FLAG_WORK_PHONE as main_FLAG_WORK_PHONE_24,\n" +
            "    FLAG_CONT_MOBILE as main_FLAG_CONT_MOBILE_25,\n" +
            "    FLAG_PHONE as main_FLAG_PHONE_26,\n" +
            "    FLAG_EMAIL as main_FLAG_EMAIL_27,\n" +
            "    OCCUPATION_TYPE as main_OCCUPATION_TYPE_28,\n" +
            "    CNT_FAM_MEMBERS as main_CNT_FAM_MEMBERS_29,\n" +
            "    REGION_RATING_CLIENT as main_REGION_RATING_CLIENT_30,\n" +
            "    REGION_RATING_CLIENT_W_CITY as main_REGION_RATING_CLIENT_W_CITY_31,\n" +
            "    WEEKDAY_APPR_PROCESS_START as main_WEEKDAY_APPR_PROCESS_START_32,\n" +
            "    HOUR_APPR_PROCESS_START as main_HOUR_APPR_PROCESS_START_33,\n" +
            "    REG_REGION_NOT_LIVE_REGION as main_REG_REGION_NOT_LIVE_REGION_34,\n" +
            "    REG_REGION_NOT_WORK_REGION as main_REG_REGION_NOT_WORK_REGION_35,\n" +
            "    LIVE_REGION_NOT_WORK_REGION as main_LIVE_REGION_NOT_WORK_REGION_36,\n" +
            "    REG_CITY_NOT_LIVE_CITY as main_REG_CITY_NOT_LIVE_CITY_37,\n" +
            "    REG_CITY_NOT_WORK_CITY as main_REG_CITY_NOT_WORK_CITY_38,\n" +
            "    LIVE_CITY_NOT_WORK_CITY as main_LIVE_CITY_NOT_WORK_CITY_39,\n" +
            "    ORGANIZATION_TYPE as main_ORGANIZATION_TYPE_40,\n" +
            "    EXT_SOURCE_1 as main_EXT_SOURCE_1_41,\n" +
            "    EXT_SOURCE_2 as main_EXT_SOURCE_2_42,\n" +
            "    EXT_SOURCE_3 as main_EXT_SOURCE_3_43,\n" +
            "    APARTMENTS_AVG as main_APARTMENTS_AVG_44,\n" +
            "    BASEMENTAREA_AVG as main_BASEMENTAREA_AVG_45,\n" +
            "    YEARS_BEGINEXPLUATATION_AVG as main_YEARS_BEGINEXPLUATATION_AVG_46,\n" +
            "    YEARS_BUILD_AVG as main_YEARS_BUILD_AVG_47,\n" +
            "    COMMONAREA_AVG as main_COMMONAREA_AVG_48,\n" +
            "    ELEVATORS_AVG as main_ELEVATORS_AVG_49,\n" +
            "    ENTRANCES_AVG as main_ENTRANCES_AVG_50,\n" +
            "    FLOORSMAX_AVG as main_FLOORSMAX_AVG_51,\n" +
            "    FLOORSMIN_AVG as main_FLOORSMIN_AVG_52,\n" +
            "    LANDAREA_AVG as main_LANDAREA_AVG_53,\n" +
            "    LIVINGAPARTMENTS_AVG as main_LIVINGAPARTMENTS_AVG_54,\n" +
            "    LIVINGAREA_AVG as main_LIVINGAREA_AVG_55,\n" +
            "    NONLIVINGAPARTMENTS_AVG as main_NONLIVINGAPARTMENTS_AVG_56,\n" +
            "    NONLIVINGAREA_AVG as main_NONLIVINGAREA_AVG_57,\n" +
            "    APARTMENTS_MODE as main_APARTMENTS_MODE_58,\n" +
            "    BASEMENTAREA_MODE as main_BASEMENTAREA_MODE_59,\n" +
            "    YEARS_BEGINEXPLUATATION_MODE as main_YEARS_BEGINEXPLUATATION_MODE_60,\n" +
            "    YEARS_BUILD_MODE as main_YEARS_BUILD_MODE_61,\n" +
            "    COMMONAREA_MODE as main_COMMONAREA_MODE_62,\n" +
            "    ELEVATORS_MODE as main_ELEVATORS_MODE_63,\n" +
            "    ENTRANCES_MODE as main_ENTRANCES_MODE_64,\n" +
            "    FLOORSMAX_MODE as main_FLOORSMAX_MODE_65,\n" +
            "    FLOORSMIN_MODE as main_FLOORSMIN_MODE_66,\n" +
            "    LANDAREA_MODE as main_LANDAREA_MODE_67,\n" +
            "    LIVINGAPARTMENTS_MODE as main_LIVINGAPARTMENTS_MODE_68,\n" +
            "    LIVINGAREA_MODE as main_LIVINGAREA_MODE_69,\n" +
            "    NONLIVINGAPARTMENTS_MODE as main_NONLIVINGAPARTMENTS_MODE_70,\n" +
            "    NONLIVINGAREA_MODE as main_NONLIVINGAREA_MODE_71,\n" +
            "    APARTMENTS_MEDI as main_APARTMENTS_MEDI_72,\n" +
            "    BASEMENTAREA_MEDI as main_BASEMENTAREA_MEDI_73,\n" +
            "    YEARS_BEGINEXPLUATATION_MEDI as main_YEARS_BEGINEXPLUATATION_MEDI_74,\n" +
            "    YEARS_BUILD_MEDI as main_YEARS_BUILD_MEDI_75,\n" +
            "    COMMONAREA_MEDI as main_COMMONAREA_MEDI_76,\n" +
            "    ELEVATORS_MEDI as main_ELEVATORS_MEDI_77,\n" +
            "    ENTRANCES_MEDI as main_ENTRANCES_MEDI_78,\n" +
            "    FLOORSMAX_MEDI as main_FLOORSMAX_MEDI_79,\n" +
            "    FLOORSMIN_MEDI as main_FLOORSMIN_MEDI_80,\n" +
            "    LANDAREA_MEDI as main_LANDAREA_MEDI_81,\n" +
            "    LIVINGAPARTMENTS_MEDI as main_LIVINGAPARTMENTS_MEDI_82,\n" +
            "    LIVINGAREA_MEDI as main_LIVINGAREA_MEDI_83,\n" +
            "    NONLIVINGAPARTMENTS_MEDI as main_NONLIVINGAPARTMENTS_MEDI_84,\n" +
            "    NONLIVINGAREA_MEDI as main_NONLIVINGAREA_MEDI_85,\n" +
            "    FONDKAPREMONT_MODE as main_FONDKAPREMONT_MODE_86,\n" +
            "    HOUSETYPE_MODE as main_HOUSETYPE_MODE_87,\n" +
            "    TOTALAREA_MODE as main_TOTALAREA_MODE_88,\n" +
            "    WALLSMATERIAL_MODE as main_WALLSMATERIAL_MODE_89,\n" +
            "    EMERGENCYSTATE_MODE as main_EMERGENCYSTATE_MODE_90,\n" +
            "    OBS_30_CNT_SOCIAL_CIRCLE as main_OBS_30_CNT_SOCIAL_CIRCLE_91,\n" +
            "    DEF_30_CNT_SOCIAL_CIRCLE as main_DEF_30_CNT_SOCIAL_CIRCLE_92,\n" +
            "    OBS_60_CNT_SOCIAL_CIRCLE as main_OBS_60_CNT_SOCIAL_CIRCLE_93,\n" +
            "    DEF_60_CNT_SOCIAL_CIRCLE as main_DEF_60_CNT_SOCIAL_CIRCLE_94,\n" +
            "    DAYS_LAST_PHONE_CHANGE as main_DAYS_LAST_PHONE_CHANGE_95,\n" +
            "    FLAG_DOCUMENT_2 as main_FLAG_DOCUMENT_2_96,\n" +
            "    FLAG_DOCUMENT_3 as main_FLAG_DOCUMENT_3_97,\n" +
            "    FLAG_DOCUMENT_4 as main_FLAG_DOCUMENT_4_98,\n" +
            "    FLAG_DOCUMENT_5 as main_FLAG_DOCUMENT_5_99,\n" +
            "    FLAG_DOCUMENT_6 as main_FLAG_DOCUMENT_6_100,\n" +
            "    FLAG_DOCUMENT_7 as main_FLAG_DOCUMENT_7_101,\n" +
            "    FLAG_DOCUMENT_8 as main_FLAG_DOCUMENT_8_102,\n" +
            "    FLAG_DOCUMENT_9 as main_FLAG_DOCUMENT_9_103,\n" +
            "    FLAG_DOCUMENT_10 as main_FLAG_DOCUMENT_10_104,\n" +
            "    FLAG_DOCUMENT_11 as main_FLAG_DOCUMENT_11_105,\n" +
            "    FLAG_DOCUMENT_12 as main_FLAG_DOCUMENT_12_106,\n" +
            "    FLAG_DOCUMENT_13 as main_FLAG_DOCUMENT_13_107,\n" +
            "    FLAG_DOCUMENT_14 as main_FLAG_DOCUMENT_14_108,\n" +
            "    FLAG_DOCUMENT_15 as main_FLAG_DOCUMENT_15_109,\n" +
            "    FLAG_DOCUMENT_16 as main_FLAG_DOCUMENT_16_110,\n" +
            "    FLAG_DOCUMENT_17 as main_FLAG_DOCUMENT_17_111,\n" +
            "    FLAG_DOCUMENT_18 as main_FLAG_DOCUMENT_18_112,\n" +
            "    FLAG_DOCUMENT_19 as main_FLAG_DOCUMENT_19_113,\n" +
            "    FLAG_DOCUMENT_20 as main_FLAG_DOCUMENT_20_114,\n" +
            "    FLAG_DOCUMENT_21 as main_FLAG_DOCUMENT_21_115,\n" +
            "    AMT_REQ_CREDIT_BUREAU_HOUR as main_AMT_REQ_CREDIT_BUREAU_HOUR_116,\n" +
            "    AMT_REQ_CREDIT_BUREAU_DAY as main_AMT_REQ_CREDIT_BUREAU_DAY_117,\n" +
            "    AMT_REQ_CREDIT_BUREAU_WEEK as main_AMT_REQ_CREDIT_BUREAU_WEEK_118,\n" +
            "    AMT_REQ_CREDIT_BUREAU_MON as main_AMT_REQ_CREDIT_BUREAU_MON_119,\n" +
            "    AMT_REQ_CREDIT_BUREAU_QRT as main_AMT_REQ_CREDIT_BUREAU_QRT_120,\n" +
            "    AMT_REQ_CREDIT_BUREAU_YEAR as main_AMT_REQ_CREDIT_BUREAU_YEAR_121,\n" +
            "    `time` as main_time_122\n" +
            "from\n" +
            "    main\n" +
            "    )\n" +
            "as out0\n" +
            "last join\n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_124,\n" +
            "    max(CNT_INSTALMENT) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_123,\n" +
            "    avg(CNT_INSTALMENT) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_124,\n" +
            "    max(CNT_INSTALMENT_FUTURE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_FUTURE_125,\n" +
            "    avg(CNT_INSTALMENT_FUTURE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_CNT_INSTALMENT_FUTURE_126,\n" +
            "    min(MONTHS_BALANCE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_MONTHS_BALANCE_127,\n" +
            "    avg(MONTHS_BALANCE) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_MONTHS_BALANCE_128,\n" +
            "    avg(SK_DPD) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_129,\n" +
            "    min(SK_DPD) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_130,\n" +
            "    max(SK_DPD_DEF) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_DEF_131,\n" +
            "    avg(SK_DPD_DEF) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_DPD_DEF_132,\n" +
            "    distinct_count(NAME_CONTRACT_STATUS) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_NAME_CONTRACT_STATUS_133,\n" +
            "    distinct_count(SK_ID_PREV) over POS_CASH_balance_SK_ID_CURR_time_0s_32d as POS_CASH_balance_SK_ID_PREV_134\n" +
            "from\n" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, bigint(0) as MONTHS_BALANCE, double(0) as CNT_INSTALMENT, double(0) as CNT_INSTALMENT_FUTURE, '' as NAME_CONTRACT_STATUS, bigint(0) as SK_DPD, bigint(0) as SK_DPD_DEF, `time` as `time` from main)\n" +
            "    window POS_CASH_balance_SK_ID_CURR_time_0s_32d as (\n" +
            "UNION POS_CASH_balance partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))\n" +
            "as out1\n" +
            "on out0.SK_ID_CURR_1 = out1.SK_ID_CURR_124\n" +
            "last join\n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_136,\n" +
            "    min(AMT_ANNUITY) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_ANNUITY_135,\n" +
            "    avg(AMT_ANNUITY) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_ANNUITY_136,\n" +
            "    max(AMT_CREDIT_MAX_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_MAX_OVERDUE_137,\n" +
            "    avg(AMT_CREDIT_MAX_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_MAX_OVERDUE_138,\n" +
            "    avg(AMT_CREDIT_SUM) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_139,\n" +
            "    min(AMT_CREDIT_SUM) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_140,\n" +
            "    avg(AMT_CREDIT_SUM_DEBT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_DEBT_141,\n" +
            "    max(AMT_CREDIT_SUM_DEBT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_DEBT_142,\n" +
            "    min(AMT_CREDIT_SUM_LIMIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_LIMIT_143,\n" +
            "    max(AMT_CREDIT_SUM_LIMIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_LIMIT_144,\n" +
            "    max(AMT_CREDIT_SUM_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_OVERDUE_145,\n" +
            "    avg(AMT_CREDIT_SUM_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_AMT_CREDIT_SUM_OVERDUE_146,\n" +
            "    max(CNT_CREDIT_PROLONG) over bureau_SK_ID_CURR_time_0s_32d as bureau_CNT_CREDIT_PROLONG_147,\n" +
            "    min(CNT_CREDIT_PROLONG) over bureau_SK_ID_CURR_time_0s_32d as bureau_CNT_CREDIT_PROLONG_148,\n" +
            "    max(CREDIT_DAY_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_DAY_OVERDUE_149,\n" +
            "    avg(CREDIT_DAY_OVERDUE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_DAY_OVERDUE_150,\n" +
            "    max(DAYS_CREDIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_151,\n" +
            "    min(DAYS_CREDIT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_152,\n" +
            "    max(DAYS_CREDIT_ENDDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_ENDDATE_153,\n" +
            "    min(DAYS_CREDIT_ENDDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_ENDDATE_154,\n" +
            "    max(DAYS_CREDIT_UPDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_UPDATE_155,\n" +
            "    min(DAYS_CREDIT_UPDATE) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_CREDIT_UPDATE_156,\n" +
            "    max(DAYS_ENDDATE_FACT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_ENDDATE_FACT_157,\n" +
            "    min(DAYS_ENDDATE_FACT) over bureau_SK_ID_CURR_time_0s_32d as bureau_DAYS_ENDDATE_FACT_158,\n" +
            "    distinct_count(CREDIT_ACTIVE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_ACTIVE_159,\n" +
            "    distinct_count(CREDIT_CURRENCY) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_CURRENCY_160,\n" +
            "    distinct_count(CREDIT_TYPE) over bureau_SK_ID_CURR_time_0s_32d as bureau_CREDIT_TYPE_161\n" +
            "from\n" +
            "    (select SK_ID_CURR as SK_ID_CURR, bigint(0) as SK_ID_BUREAU, '' as CREDIT_ACTIVE, '' as CREDIT_CURRENCY, bigint(0) as DAYS_CREDIT, bigint(0) as CREDIT_DAY_OVERDUE, double(0) as DAYS_CREDIT_ENDDATE, double(0) as DAYS_ENDDATE_FACT, double(0) as AMT_CREDIT_MAX_OVERDUE, bigint(0) as CNT_CREDIT_PROLONG, double(0) as AMT_CREDIT_SUM, double(0) as AMT_CREDIT_SUM_DEBT, double(0) as AMT_CREDIT_SUM_LIMIT, double(0) as AMT_CREDIT_SUM_OVERDUE, '' as CREDIT_TYPE, bigint(0) as DAYS_CREDIT_UPDATE, double(0) as AMT_ANNUITY, `time` as `time` from main)\n" +
            "    window bureau_SK_ID_CURR_time_0s_32d as (\n" +
            "UNION bureau partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))\n" +
            "as out2\n" +
            "on out0.SK_ID_CURR_1 = out2.SK_ID_CURR_136\n" +
            "last join\n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_163,\n" +
            "    max(MONTHS_BALANCE) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_MONTHS_BALANCE_162,\n" +
            "    avg(MONTHS_BALANCE) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_MONTHS_BALANCE_163,\n" +
            "    distinct_count(`STATUS`) over bureau_balance_SK_ID_CURR_time_0s_32d as bureau_balance_STATUS_164\n" +
            "from\n" +
            "    (select bigint(0) as SK_ID_BUREAU, bigint(0) as MONTHS_BALANCE, '' as `STATUS`, SK_ID_CURR as SK_ID_CURR, `time` as `time` from main)\n" +
            "    window bureau_balance_SK_ID_CURR_time_0s_32d as (\n" +
            "UNION bureau_balance partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))\n" +
            "as out3\n" +
            "on out0.SK_ID_CURR_1 = out3.SK_ID_CURR_163\n" +
            "last join\n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_166,\n" +
            "    avg(AMT_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_BALANCE_165,\n" +
            "    max(AMT_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_BALANCE_166,\n" +
            "    max(AMT_CREDIT_LIMIT_ACTUAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_CREDIT_LIMIT_ACTUAL_167,\n" +
            "    avg(AMT_CREDIT_LIMIT_ACTUAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_CREDIT_LIMIT_ACTUAL_168,\n" +
            "    avg(AMT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_ATM_CURRENT_169,\n" +
            "    max(AMT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_ATM_CURRENT_170,\n" +
            "    avg(AMT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_CURRENT_171,\n" +
            "    max(AMT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_CURRENT_172,\n" +
            "    max(AMT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_OTHER_CURRENT_173,\n" +
            "    avg(AMT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_OTHER_CURRENT_174,\n" +
            "    max(AMT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_POS_CURRENT_175,\n" +
            "    avg(AMT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_DRAWINGS_POS_CURRENT_176,\n" +
            "    avg(AMT_INST_MIN_REGULARITY) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_INST_MIN_REGULARITY_177,\n" +
            "    max(AMT_INST_MIN_REGULARITY) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_INST_MIN_REGULARITY_178,\n" +
            "    max(AMT_PAYMENT_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_CURRENT_179,\n" +
            "    avg(AMT_PAYMENT_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_CURRENT_180,\n" +
            "    avg(AMT_PAYMENT_TOTAL_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_TOTAL_CURRENT_181,\n" +
            "    max(AMT_PAYMENT_TOTAL_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_PAYMENT_TOTAL_CURRENT_182,\n" +
            "    avg(AMT_RECEIVABLE_PRINCIPAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECEIVABLE_PRINCIPAL_183,\n" +
            "    max(AMT_RECEIVABLE_PRINCIPAL) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECEIVABLE_PRINCIPAL_184,\n" +
            "    avg(AMT_RECIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECIVABLE_185,\n" +
            "    max(AMT_RECIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_RECIVABLE_186,\n" +
            "    avg(AMT_TOTAL_RECEIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_TOTAL_RECEIVABLE_187,\n" +
            "    max(AMT_TOTAL_RECEIVABLE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_AMT_TOTAL_RECEIVABLE_188,\n" +
            "    avg(CNT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_ATM_CURRENT_189,\n" +
            "    max(CNT_DRAWINGS_ATM_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_ATM_CURRENT_190,\n" +
            "    avg(CNT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_CURRENT_191,\n" +
            "    max(CNT_DRAWINGS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_CURRENT_192,\n" +
            "    max(CNT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_OTHER_CURRENT_193,\n" +
            "    avg(CNT_DRAWINGS_OTHER_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_OTHER_CURRENT_194,\n" +
            "    max(CNT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_POS_CURRENT_195,\n" +
            "    avg(CNT_DRAWINGS_POS_CURRENT) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_DRAWINGS_POS_CURRENT_196,\n" +
            "    max(CNT_INSTALMENT_MATURE_CUM) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_INSTALMENT_MATURE_CUM_197,\n" +
            "    avg(CNT_INSTALMENT_MATURE_CUM) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_CNT_INSTALMENT_MATURE_CUM_198,\n" +
            "    min(MONTHS_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_MONTHS_BALANCE_199,\n" +
            "    avg(MONTHS_BALANCE) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_MONTHS_BALANCE_200,\n" +
            "    min(SK_DPD) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_201,\n" +
            "    avg(SK_DPD) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_202,\n" +
            "    min(SK_DPD_DEF) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_DEF_203,\n" +
            "    avg(SK_DPD_DEF) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_DPD_DEF_204,\n" +
            "    distinct_count(NAME_CONTRACT_STATUS) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_NAME_CONTRACT_STATUS_205,\n" +
            "    distinct_count(SK_ID_PREV) over credit_card_balance_SK_ID_CURR_time_0s_32d as credit_card_balance_SK_ID_PREV_206\n" +
            "from\n" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, bigint(0) as MONTHS_BALANCE, double(0) as AMT_BALANCE, bigint(0) as AMT_CREDIT_LIMIT_ACTUAL, double(0) as AMT_DRAWINGS_ATM_CURRENT, double(0) as AMT_DRAWINGS_CURRENT, double(0) as AMT_DRAWINGS_OTHER_CURRENT, double(0) as AMT_DRAWINGS_POS_CURRENT, double(0) as AMT_INST_MIN_REGULARITY, double(0) as AMT_PAYMENT_CURRENT, double(0) as AMT_PAYMENT_TOTAL_CURRENT, double(0) as AMT_RECEIVABLE_PRINCIPAL, double(0) as AMT_RECIVABLE, double(0) as AMT_TOTAL_RECEIVABLE, double(0) as CNT_DRAWINGS_ATM_CURRENT, bigint(0) as CNT_DRAWINGS_CURRENT, double(0) as CNT_DRAWINGS_OTHER_CURRENT, double(0) as CNT_DRAWINGS_POS_CURRENT, double(0) as CNT_INSTALMENT_MATURE_CUM, '' as NAME_CONTRACT_STATUS, bigint(0) as SK_DPD, bigint(0) as SK_DPD_DEF, `time` as `time` from main)\n" +
            "    window credit_card_balance_SK_ID_CURR_time_0s_32d as (\n" +
            "UNION credit_card_balance partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))\n" +
            "as out4\n" +
            "on out0.SK_ID_CURR_1 = out4.SK_ID_CURR_166\n" +
            "last join\n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_208,\n" +
            "    max(AMT_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_INSTALMENT_207,\n" +
            "    avg(AMT_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_INSTALMENT_208,\n" +
            "    max(AMT_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_PAYMENT_209,\n" +
            "    min(AMT_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_AMT_PAYMENT_210,\n" +
            "    avg(DAYS_ENTRY_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_ENTRY_PAYMENT_211,\n" +
            "    max(DAYS_ENTRY_PAYMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_ENTRY_PAYMENT_212,\n" +
            "    avg(DAYS_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_INSTALMENT_213,\n" +
            "    min(DAYS_INSTALMENT) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_DAYS_INSTALMENT_214,\n" +
            "    max(NUM_INSTALMENT_NUMBER) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_NUMBER_215,\n" +
            "    avg(NUM_INSTALMENT_NUMBER) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_NUMBER_216,\n" +
            "    avg(NUM_INSTALMENT_VERSION) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_VERSION_217,\n" +
            "    min(NUM_INSTALMENT_VERSION) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_NUM_INSTALMENT_VERSION_218,\n" +
            "    distinct_count(SK_ID_PREV) over installments_payments_SK_ID_CURR_time_0s_32d as installments_payments_SK_ID_PREV_219\n" +
            "from\n" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, double(0) as NUM_INSTALMENT_VERSION, bigint(0) as NUM_INSTALMENT_NUMBER, double(0) as DAYS_INSTALMENT, double(0) as DAYS_ENTRY_PAYMENT, double(0) as AMT_INSTALMENT, double(0) as AMT_PAYMENT, `time` as `time` from main)\n" +
            "    window installments_payments_SK_ID_CURR_time_0s_32d as (\n" +
            "UNION installments_payments partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))\n" +
            "as out5\n" +
            "on out0.SK_ID_CURR_1 = out5.SK_ID_CURR_208\n" +
            "last join\n" +
            "(\n" +
            "select\n" +
            "    SK_ID_CURR as SK_ID_CURR_221,\n" +
            "    max(AMT_ANNUITY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_ANNUITY_220,\n" +
            "    min(AMT_ANNUITY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_ANNUITY_221,\n" +
            "    max(AMT_APPLICATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_APPLICATION_222,\n" +
            "    min(AMT_APPLICATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_APPLICATION_223,\n" +
            "    max(AMT_CREDIT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_CREDIT_224,\n" +
            "    min(AMT_CREDIT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_CREDIT_225,\n" +
            "    max(AMT_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_DOWN_PAYMENT_226,\n" +
            "    avg(AMT_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_DOWN_PAYMENT_227,\n" +
            "    max(AMT_GOODS_PRICE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_GOODS_PRICE_228,\n" +
            "    min(AMT_GOODS_PRICE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_AMT_GOODS_PRICE_229,\n" +
            "    max(CNT_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CNT_PAYMENT_230,\n" +
            "    avg(CNT_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CNT_PAYMENT_231,\n" +
            "    avg(DAYS_DECISION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_DECISION_232,\n" +
            "    min(DAYS_DECISION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_DECISION_233,\n" +
            "    avg(DAYS_FIRST_DRAWING) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DRAWING_234,\n" +
            "    min(DAYS_FIRST_DRAWING) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DRAWING_235,\n" +
            "    avg(DAYS_FIRST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DUE_236,\n" +
            "    max(DAYS_FIRST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_FIRST_DUE_237,\n" +
            "    avg(DAYS_LAST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_238,\n" +
            "    max(DAYS_LAST_DUE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_239,\n" +
            "    max(DAYS_LAST_DUE_1ST_VERSION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_1ST_VERSION_240,\n" +
            "    avg(DAYS_LAST_DUE_1ST_VERSION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_LAST_DUE_1ST_VERSION_241,\n" +
            "    max(DAYS_TERMINATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_TERMINATION_242,\n" +
            "    avg(DAYS_TERMINATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_DAYS_TERMINATION_243,\n" +
            "    max(HOUR_APPR_PROCESS_START) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_HOUR_APPR_PROCESS_START_244,\n" +
            "    avg(HOUR_APPR_PROCESS_START) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_HOUR_APPR_PROCESS_START_245,\n" +
            "    avg(NFLAG_INSURED_ON_APPROVAL) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_INSURED_ON_APPROVAL_246,\n" +
            "    min(NFLAG_INSURED_ON_APPROVAL) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_INSURED_ON_APPROVAL_247,\n" +
            "    min(NFLAG_LAST_APPL_IN_DAY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_LAST_APPL_IN_DAY_248,\n" +
            "    max(NFLAG_LAST_APPL_IN_DAY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NFLAG_LAST_APPL_IN_DAY_249,\n" +
            "    avg(RATE_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_DOWN_PAYMENT_250,\n" +
            "    max(RATE_DOWN_PAYMENT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_DOWN_PAYMENT_251,\n" +
            "    min(RATE_INTEREST_PRIMARY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIMARY_252,\n" +
            "    avg(RATE_INTEREST_PRIMARY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIMARY_253,\n" +
            "    min(RATE_INTEREST_PRIVILEGED) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIVILEGED_254,\n" +
            "    max(RATE_INTEREST_PRIVILEGED) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_RATE_INTEREST_PRIVILEGED_255,\n" +
            "    min(SELLERPLACE_AREA) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SELLERPLACE_AREA_256,\n" +
            "    avg(SELLERPLACE_AREA) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SELLERPLACE_AREA_257,\n" +
            "    distinct_count(CHANNEL_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CHANNEL_TYPE_258,\n" +
            "    distinct_count(CODE_REJECT_REASON) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_CODE_REJECT_REASON_259,\n" +
            "    distinct_count(FLAG_LAST_APPL_PER_CONTRACT) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_FLAG_LAST_APPL_PER_CONTRACT_260,\n" +
            "    distinct_count(NAME_CASH_LOAN_PURPOSE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CASH_LOAN_PURPOSE_261,\n" +
            "    distinct_count(NAME_CLIENT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CLIENT_TYPE_262,\n" +
            "    distinct_count(NAME_CONTRACT_STATUS) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CONTRACT_STATUS_263,\n" +
            "    distinct_count(NAME_CONTRACT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_CONTRACT_TYPE_264,\n" +
            "    distinct_count(NAME_GOODS_CATEGORY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_GOODS_CATEGORY_265,\n" +
            "    distinct_count(NAME_PAYMENT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_PAYMENT_TYPE_266,\n" +
            "    distinct_count(NAME_PORTFOLIO) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_PORTFOLIO_267,\n" +
            "    distinct_count(NAME_PRODUCT_TYPE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_PRODUCT_TYPE_268,\n" +
            "    distinct_count(NAME_SELLER_INDUSTRY) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_SELLER_INDUSTRY_269,\n" +
            "    distinct_count(NAME_TYPE_SUITE) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_TYPE_SUITE_270,\n" +
            "    distinct_count(NAME_YIELD_GROUP) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_NAME_YIELD_GROUP_271,\n" +
            "    distinct_count(PRODUCT_COMBINATION) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_PRODUCT_COMBINATION_272,\n" +
            "    distinct_count(SK_ID_PREV) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_SK_ID_PREV_273,\n" +
            "    distinct_count(WEEKDAY_APPR_PROCESS_START) over previous_application_SK_ID_CURR_time_0s_32d as previous_application_WEEKDAY_APPR_PROCESS_START_274\n" +
            "from\n" +
            "    (select bigint(0) as SK_ID_PREV, SK_ID_CURR as SK_ID_CURR, '' as NAME_CONTRACT_TYPE, double(0) as AMT_ANNUITY, double(0) as AMT_APPLICATION, double(0) as AMT_CREDIT, double(0) as AMT_DOWN_PAYMENT, double(0) as AMT_GOODS_PRICE, '' as WEEKDAY_APPR_PROCESS_START, bigint(0) as HOUR_APPR_PROCESS_START, '' as FLAG_LAST_APPL_PER_CONTRACT, bigint(0) as NFLAG_LAST_APPL_IN_DAY, double(0) as RATE_DOWN_PAYMENT, double(0) as RATE_INTEREST_PRIMARY, double(0) as RATE_INTEREST_PRIVILEGED, '' as NAME_CASH_LOAN_PURPOSE, '' as NAME_CONTRACT_STATUS, bigint(0) as DAYS_DECISION, '' as NAME_PAYMENT_TYPE, '' as CODE_REJECT_REASON, '' as NAME_TYPE_SUITE, '' as NAME_CLIENT_TYPE, '' as NAME_GOODS_CATEGORY, '' as NAME_PORTFOLIO, '' as NAME_PRODUCT_TYPE, '' as CHANNEL_TYPE, bigint(0) as SELLERPLACE_AREA, '' as NAME_SELLER_INDUSTRY, double(0) as CNT_PAYMENT, '' as NAME_YIELD_GROUP, '' as PRODUCT_COMBINATION, double(0) as DAYS_FIRST_DRAWING, double(0) as DAYS_FIRST_DUE, double(0) as DAYS_LAST_DUE_1ST_VERSION, double(0) as DAYS_LAST_DUE, double(0) as DAYS_TERMINATION, double(0) as NFLAG_INSURED_ON_APPROVAL, `time` as `time` from main)\n" +
            "    window previous_application_SK_ID_CURR_time_0s_32d as (\n" +
            "UNION previous_application partition by SK_ID_CURR order by `time` rows_range between 32d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW))\n" +
            "as out6\n" +
            "on out0.SK_ID_CURR_1 = out6.SK_ID_CURR_221\n" +
            ";";

    private SqlExecutor sqlExecutor = null;
    private String db = "test_db2";

    public void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setZkSessionTimeout(10000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    public void initDDL() throws Exception {
        sqlExecutor.dropDB(db);
        sqlExecutor.createDB(db);
        String[] split = ddl.split(";");
        for (String s : split) {
            boolean ok = sqlExecutor.executeDDL(db, s + ";");
            System.out.println("create table ok");
        }
        Schema inputSchema = sqlExecutor.getInputSchema(db, sql);
        System.out.println("init ddl finished");
    }

    public static void run() {
        final ComplexSqlExample example = new ComplexSqlExample();
        try {
            example.init();
            System.out.println("init success");
            example.initDDL();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        run();
    }

}
