/*
 * CsvFileUtil.java
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

package com._4paradigm.fesql.flink.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class CsvFileUtil {

    public static void checkCsvFiles(String flinksqlCsvFile, String fesqlCsvFile) throws Exception {
        final String DELIMITER_CHAR = ",";
        final int keyIndex = 0;

        Map<String, String> flinksqlResultMap = getCsvResultMap(flinksqlCsvFile, DELIMITER_CHAR, keyIndex);
        Map<String, String> fesqlResultMap = getCsvResultMap(fesqlCsvFile, DELIMITER_CHAR, keyIndex);

        for (String key : flinksqlResultMap.keySet()) {

            String value1 = flinksqlResultMap.get(key);
            String value2 = fesqlResultMap.get(key);

            if (!value1.equals(value2)) {
                System.out.println("Result not consistent, value1: " + value1 + ", value2: " + value2);
                return;
            }

        }

        System.out.println("All data is consistent, case number: " + flinksqlResultMap.size());

    }

    public static Map<String, String> getCsvResultMap(String csvFilePath, String delimiter, int keyIndex) throws Exception {
        Map<String, String> resultMap = new HashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(csvFilePath));

        String line;
        while ((line = br.readLine()) != null) {
            String[] values = line.split(delimiter);
            if (line.endsWith(",")) {
                line = line.substring(0, line.length()-2);

            }
            resultMap.put(values[keyIndex], line);
        }

        return resultMap;
    }


    public static void main(String[] argv) throws Exception {

        String flinksqlCsvFile = "/tmp/flink_csv_output40_flinksql";
        String fesqlCsvFile = "/tmp/flink_csv_output40_fesql";

        checkCsvFiles(flinksqlCsvFile, fesqlCsvFile);
    }

}
