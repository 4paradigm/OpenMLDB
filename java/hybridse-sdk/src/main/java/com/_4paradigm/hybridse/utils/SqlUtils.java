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

package com._4paradigm.hybridse.utils;

import com._4paradigm.hybridse.element.SparkConfig;
import com.google.gson.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SqlUtils {
    private static Logger logger = LoggerFactory.getLogger(SqlUtils.class);
    public static SparkConfig parseFeconfigJsonPath(String json) {
        try {
            return parseFeconfigJson(FileUtils.readFileToString(new File(json), "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("failed to parse json");
        }
    }

    public static SparkConfig parseFeconfigJson(String json) {
        logger.info("fesql config: {}", json);
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonObject jsonElement = parser.parse(json).getAsJsonObject();
        SparkConfig config = new SparkConfig();
//        String sqlEle = jsonElement.get("script").getAsString();
        if (jsonElement.has("script")) {
            String script = jsonElement.get("script").getAsString();
            if (script.startsWith("file://")) {
                try {
                    script = ConfigReader.readConf(script.replaceFirst("file://", ""));
                } catch (Exception e) {
                    logger.error("fail to read {} {}", script, e);
                }
            }
            config.setSql(script);
        } else {
            throw new RuntimeException("config json has no script field");
        }
        if (jsonElement.has("inputs")) {
            JsonArray array = jsonElement.get("inputs").getAsJsonArray();
            for (JsonElement e : array) {
                config.getTables().put(e.getAsJsonObject().get("name").getAsString(), e.getAsJsonObject().get("inputPath").getAsString());
            }
        } else {
            throw new RuntimeException("config json has no inputs field");
        }
        if (jsonElement.has("sparkConfig")) {
            String sparkConfig = jsonElement.get("sparkConfig").getAsString();
            String[] c = sparkConfig.split(" ");
            for (String e : c) {
                config.getSparkConfig().add(e);
            }
//            config.getSparkConfig().addAll(new ArrayList<String>(sparkConfig.split(" ").))
        }
        if (jsonElement.has("outputPath")) {
            String outputPath = jsonElement.get("outputPath").getAsString();
            config.setOutputPath(outputPath);
        } else {
            throw new RuntimeException("config json has no outputPath field");
        }

        if (jsonElement.has("instanceFormat")) {
            String outputPath = jsonElement.get("instanceFormat").getAsString();
            config.setInstanceFormat(outputPath);
        }
        return config;
    }

    public static String checkTablesIsSameSql(String table1, String table2, List<String> schema) {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("select count(*) from %s inner join %s on\n", table1, table2));
        List<String> conds = new ArrayList<>();
        for (String e : schema) {
            String con = String.format("`%s`.`%s` = `%s`.`%s`", table1, e, table2, e);
            conds.add(con);
        }
        sb.append(StringUtils.join(conds, " and "));
        sb.append(";");
        return sb.toString();
    }

}
