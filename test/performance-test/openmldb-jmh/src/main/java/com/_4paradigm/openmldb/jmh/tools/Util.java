package com._4paradigm.openmldb.jmh.tools;


import com._4paradigm.openmldb.jmh.BenchmarkConfig;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


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
