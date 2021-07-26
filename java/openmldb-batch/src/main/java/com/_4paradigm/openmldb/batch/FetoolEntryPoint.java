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

package com._4paradigm.openmldb.batch;

import py4j.GatewayServer;
import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;


public class FetoolEntryPoint {

    /*
    public String genFedbDdlWithSchemaAndSqlFiles(String schemaPath, String sqlPath, int replicaNumber, int partitionNumber) {
        File file = new File(schemaPath);
        File sql = new File(sqlPath);
        String ddl = null;
        try {
            ddl = DDLEngine.genDDL(FileUtils.readFileToString(sql, "UTF-8"), FileUtils.readFileToString(file, "UTF-8"), replicaNumber, partitionNumber);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ddl;
    }

    public String genFedbDdl(Map<String, String> nameParquetMap, String sql, int replicaNum, int partitionNum) {
        String ddl = null;
        try {
            ddl = DDLEngine.genFedbDdl(nameParquetMap, sql ,replicaNum, partitionNum);
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: Return error when catching exceptions
            ddl = e.getMessage();
        }
        return ddl;
    }
    */

    public static void main(String[] args) {
        GatewayServer gatewayServer = new GatewayServer(new FetoolEntryPoint());
        gatewayServer.start();
    }

}
