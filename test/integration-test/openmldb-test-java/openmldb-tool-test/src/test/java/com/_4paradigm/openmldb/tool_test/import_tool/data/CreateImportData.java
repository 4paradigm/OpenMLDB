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
package com._4paradigm.openmldb.tool_test.import_tool.data;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class CreateImportData {
    @Test
    public void createCsvDataBy100Million() throws Exception {
        Random ran = new Random();
        File dataDirectory = new File("data");
        if(!dataDirectory.exists()){
            dataDirectory.mkdir();
        }
        String fileName = "csv-import-1000w.csv";
        String filePath = dataDirectory.getAbsolutePath()+"/"+fileName;
        PrintWriter out = new PrintWriter(filePath);
        out.println("id,c1_smallint,c2_int,c3_bigint,c4_float,c5_double,c6_string,c7_timestamp,c8_date,c9_bool");
        int size = 10000;
        int total = 10000000;
        for(int i=1;i<=total;i++){
            int id = i;
            short c1_smallint = (short) ran.nextInt();
            int c2_int = ran.nextInt();
            long c3_bigint = ran.nextLong();
            float c4_float = ran.nextFloat();
            double c5_double = ran.nextDouble();
            String c6_string = RandomStringUtils.randomAlphanumeric(16);
            long c7_timestamp = System.currentTimeMillis();
            long randomLong = RandomUtils.nextLong(0, c7_timestamp);
            String c8_date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(randomLong));
            boolean c9_bool = ran.nextBoolean();
            String line = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",id,c1_smallint,c2_int,c3_bigint,c4_float,c5_double,c6_string,c7_timestamp,c8_date,c9_bool);
            out.println(line);
            if(i%size==0){
                System.out.println("i = " + i);
            }
        }
        out.close();
    }
}
