package com._4paradigm.rtidb.client.base;

import com._4paradigm.rtidb.ns.NS;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestUtil {

    public static void ExecuteGc(NS.TableInfo table) {
        int tid = table.getTid();
        for(NS.TablePartition tablePartition : table.getTablePartitionList()) {
            int pid = tablePartition.getPid();
            for(NS.PartitionMeta meta : tablePartition.getPartitionMetaList()) {
                String endpoint = meta.getEndpoint();
                String httpUrl = String.format("http://%s/TabletServer/ExecuteGc", endpoint);
                String jsonStr = String.format("{\"tid\":%d, \"pid\":%d}", tid, pid);
                SendHttpRequest(httpUrl, jsonStr);
            }
        }
    }

    public static int GetTableRecordCnt(NS.TableInfo table) {
        int count = 0;
        for (NS.TablePartition tablePartition : table.getTablePartitionList()) {
            count += tablePartition.getRecordCnt();
        }
        return count;
    }

    private static String SendHttpRequest(String httpUrl, String body) {
        Map<String,String> headers = new HashMap<>();
        headers.put("accept","application/json;charset=UTF-8");
        headers.put("Content-Type","application/json;charset=UTF-8");

        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(15000);
            connection.setReadTimeout(60000);

            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("Authorization", "Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0");
            os = connection.getOutputStream();
            os.write(body.getBytes());
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            connection.disconnect();
        }
        return result;
    }
}
