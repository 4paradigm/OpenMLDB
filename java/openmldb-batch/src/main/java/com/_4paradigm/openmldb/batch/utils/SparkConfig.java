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

import lombok.Data;
import java.util.*;


@Data
public class SparkConfig {
    private String sql = "";
    private Map<String, String> tables = new LinkedHashMap<>();
    private List<String> sparkConfig = new ArrayList<>();
    private String outputPath = "";
    private String instanceFormat = "parquet";

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, String> getTables() {
        return tables;
    }

    public void setTables(Map<String, String> tables) {
        this.tables = tables;
    }

    public List<String> getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig(List<String> sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getInstanceFormat() {
        return instanceFormat;
    }

    public void setInstanceFormat(String instanceFormat) {
        this.instanceFormat = instanceFormat;
    }

}
