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

package com._4paradigm.openmldb.batch.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 **/
public class SkewSqlUtils {

    public static String genPercentileSql(String table1, int quantile, List<String> keys, String ts, String cnt) {
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT \n");
        for (String e : keys) {
            sql.append(String.format("`%s`,\n", e));
        }
        List<String> newkeys = new ArrayList<>();
        for (String e : keys) {
            newkeys.add(String.format("`%s`", e));
        }
        // TODO: Support count multiple column, This way has problem
        sql.append(String.format("count(%s) AS %s,\n", StringUtils.join(newkeys, ","), cnt));
        double factor = 1.0 / (double) quantile;
        for (int i = 0; i < quantile; i++) {
            double v = i * factor;
            sql.append(String.format("percentile_approx(`%s`, %s) AS percentile_%s,\n", ts, v, i));
        }
        sql.append(String.format("percentile_approx(`%s`, 1) AS percentile_%s\n", ts, quantile));
        sql.append(String.format("FROM `%s`\nGROUP BY ", table1));
        sql.append(StringUtils.join(newkeys, " , "));
        sql.append(";");
        return sql.toString();
    }

    public static String genPercentileTagSql(String table1, String table2, int quantile, List<String> schemas,
                                             Map<String, String> keysMap, String ts, String tag1, String tag2,
                                             String tag3, long tag4) {
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT \n");
        for (String e : schemas) {
            sql.append(table1 + ".`" + e + "`,");
        }

        sql.append(caseWhenTag(table1, table2, ts, quantile, tag1, tag3, tag4));
        sql.append(",");
        sql.append(caseWhenTag(table1, table2, ts, quantile, tag2, tag3, tag4));


        sql.append(String.format("FROM `%s` \nLEFT JOIN `%s` ON ", table1, table2));
        List<String> conditions = new ArrayList<>();
        for (Map.Entry<String, String> e : keysMap.entrySet()) {
            String cond = String.format("`%s`.`%s` = `%s`.`%s`", table1, e.getKey(), table2, e.getValue());
            conditions.add(cond);
        }
        sql.append(StringUtils.join(conditions, " AND "));
        sql.append(";");
        return sql.toString();
    }

    public static String caseWhenTag(String table1, String table2, String ts, int quantile, String output,
                                     String con1, long cnt) {
        StringBuffer sql = new StringBuffer();
        sql.append("\nCASE\n");
        sql.append(String.format("WHEN `%s`.`%s` < %s THEN 1\n", table2, con1, cnt));
        // TODO: It is not necessary to append all percentile_tag columns
        for (int i = 0; i < quantile; i++) {
            if (i == 0) {
                sql.append(String.format("WHEN `%s`.`%s` <= percentile_%s THEN %d\n", table1, ts, i, quantile - i));
            }

            sql.append(String.format("WHEN `%s`.`%s` > percentile_%s and `%s`.`%s` <= percentile_%d THEN %d\n",
                    table1, ts, i, table1, ts, i + 1, quantile - i));
            if (i == quantile) {
                sql.append(String.format("WHEN `%s`.`%s` > percentile_%s THEN %d\n", table1, ts, i + 1, quantile - i));
            }
        }
        sql.append("END AS " + output + "\n");
        return sql.toString();
    }

    public static String explodeDataSql(String table, int quantile, List<String> schemas, String tag1, String tag2,
                                        long watershed, long windowSize) {
        // watershed 水位线 windowSize 窗口的大小，0表示无限

        List<String> sqls = new ArrayList<>();
        boolean isClibing = true;
        // if window size = 0, then there is no conut window, only time window
        if (windowSize > 0 && watershed / quantile > windowSize) {
            isClibing = false;
        }
        // gen lots of sql
        for (int i = 0; i < quantile; i++) {
            if (i == 0) {
                String sql = String.format("SELECT * FROM %s", table);
                sqls.add(sql);
                continue;
            }

            StringBuffer sql = new StringBuffer();
            sql.append("SELECT \n");
            for (String e : schemas) {
                sql.append(table + ".`" + e + "`,");
            }
            sql.append("\n");

            List<String> whereExpr = new ArrayList<>();
            if (isClibing) {
                sql.append(String.format("%d as `%s`, %s.`%s` FROM %s\nWHERE ", i, tag1, table, tag2, table));
                // explode 1, 2, 3, 4
                for (int explode = i + 1; explode <= quantile; explode++) {
                    whereExpr.add(String.format("`%s` = %d", tag2, explode));
                }
            } else {
                sql.append(String.format("%s.`%s` - 1, %s.`%s` FROM %s\nWHERE ", table, tag1, table, tag2, table));
                sql.append(String.format("`%s` != 1", tag2));
                sqls.add(sql.toString());
                break;
            }
            sql.append(StringUtils.join(whereExpr, " or "));
            sqls.add(sql.toString());
        }
        String res = StringUtils.join(sqls, "\nUNION\n") + ";";
        return res;
    }
}

