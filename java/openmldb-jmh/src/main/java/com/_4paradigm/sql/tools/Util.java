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

package com._4paradigm.sql.tools;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

// import com._4paradigm.featuredb.proto.Base;
import com._4paradigm.sql.BenchmarkConfig;

public class Util {
    public static String getContent(String httpUrl) {
        try {
            URL url = new URL(httpUrl);
            HttpURLConnection con = null;
            if (BenchmarkConfig.NeedProxy()) {
                con = (HttpURLConnection) url.openConnection(new Proxy(Proxy.Type.SOCKS,
                        new InetSocketAddress("127.0.0.1",1080)));
            } else {
                con = (HttpURLConnection) url.openConnection();
            }
            con.setRequestMethod("GET");
            con.connect();
            if (con.getResponseCode() == 200) {
                InputStream is = con.getInputStream();
                StringBuilder builder = new StringBuilder();
                int len = 0;
                byte[] buffer = new byte[1024];
                while ((len = is.read(buffer)) != -1) {
                    byte[] temp = new byte[len];
                    System.arraycopy(buffer, 0 , temp, 0, len);
                    builder.append(new String(temp, "utf-8"));
                }
                return builder.toString();
            } else {
                System.out.println("request failed");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
    public static Base.FeatureDBType getFeatureDBType(String type) throws Exception {
        switch (type.toLowerCase()) {
            case "bool":
            case "boolean":
                return Base.FeatureDBType.kBoolean;
            case "short":
            case "int16":
                return Base.FeatureDBType.kInt16;
            case "int":
            case "int32":
                return Base.FeatureDBType.kInt32;
            case "long":
            case "int64":
                return Base.FeatureDBType.kInt64;
            case "float":
                return Base.FeatureDBType.kFloat;
            case "double":
                return Base.FeatureDBType.kDouble;
            case "date":
                return Base.FeatureDBType.kDate;
            case "timestamp":
            case "timestamp-millis":
                return Base.FeatureDBType.kTimestamp;
            case "string":
                return Base.FeatureDBType.kString;
            case "list":
            case "array":
                return Base.FeatureDBType.kList;
            case "map":
                return Base.FeatureDBType.kMap;
            case "feature":
                return Base.FeatureDBType.kFeature;
            default:
                throw new Exception("type " + type + " is not supported");
        }
    }*/

    public static Map<String, TableInfo> parseDDL(String ddlUrl, Relation relation) {
        String ddl = Util.getContent(ddlUrl);
        String[] arr = ddl.split(";");
        Map<String, TableInfo> tableMap = new HashMap<>();
        for (String item : arr) {
            item = item.trim().replace("\n", "");
            if (item.isEmpty()) {
                continue;
            }
            TableInfo table = new TableInfo(item, relation);
            tableMap.put(table.getName(), table);
        }
        return tableMap;
    }

    public static String getCreateProcedureDDL(String pName, TableInfo mainTable, String script) {
        String ddl = "create PROCEDURE " + pName + "(" + mainTable.getTypeString() + ") \n BEGIN \n" + script + "\n END;";
        return ddl;
    }
}
