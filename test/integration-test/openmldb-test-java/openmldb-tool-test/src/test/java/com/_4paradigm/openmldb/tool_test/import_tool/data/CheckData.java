package com._4paradigm.openmldb.tool_test.import_tool.data;



import com._4paradigm.openmldb.java_sdk_test.common.FedbClient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class CheckData {
    public void testImportDataRight() throws Exception {
        String filePath = "data/smoke.csv";
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line = null;
        int count = 0;
        while((line = in.readLine())!=null){
            count++;
            if(count==1) continue;
            String[] data = line.split(",");

        }
        FedbClient fedbClient = new FedbClient("172.24.4.55:10015","/openmldb");

    }
}
