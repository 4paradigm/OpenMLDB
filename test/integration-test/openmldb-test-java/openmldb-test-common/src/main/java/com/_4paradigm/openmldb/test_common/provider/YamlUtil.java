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

package com._4paradigm.openmldb.test_common.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * @author zhaowei
 * @date 2020/6/11 3:19 PM
 */
@Data
@Slf4j
public class YamlUtil {

    public static final String FAIL_CASE= "FailCase";

    public static <T> T getObject(String caseFile, Class<T> clazz)  {
        try {
            Yaml yaml = new Yaml();
            FileInputStream testDataStream = new FileInputStream(caseFile);
            T t = yaml.loadAs(testDataStream, clazz);
            return t;
        } catch (Exception e) {
            log.error("fail to load yaml: ", e);
            e.printStackTrace();
            return null;
        }
    }

    public static void writeYamlFile(Object obj,String yamlPath)  {
        try {
            Yaml yaml = new Yaml();
            PrintWriter out = new PrintWriter(yamlPath);
            yaml.dump(obj,out);
        } catch (Exception e) {
            log.error("fail to write yaml: ", e);
            e.printStackTrace();
        }
    }
}

