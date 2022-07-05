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

package com._4paradigm.openmldb.test_common.restful.util;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

@Slf4j
public class OpenMLDBTool {

    public static String getFilePath(String filename) {
        return OpenMLDBTool.class.getClassLoader().getResource(filename).getFile();
    }

    public static String getCasePath(String yamlCaseDir, String casePath) {
        String caseDir = StringUtils.isEmpty(yamlCaseDir) ? OpenMLDBTool.openMLDBDir().getAbsolutePath() : yamlCaseDir;
        Assert.assertNotNull(caseDir);
        String caseAbsPath = caseDir + "/cases/" + casePath;
        log.debug("case absolute path: {}", caseAbsPath);
        return caseAbsPath;
    }

    public static File openMLDBDir() {
        File directory = new File(".");
        directory = directory.getAbsoluteFile();
        while (null != directory) {
            if (directory.isDirectory() && "OpenMLDB".equals(directory.getName())) {
                break;
            }
            log.debug("current directory name {}", directory.getName());
            directory = directory.getParentFile();
        }

        if ("OpenMLDB".equals(directory.getName())) {
            return directory;
        } else {
            return null;
        }
    }

    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getPaths(File directory) {
        List<String> list = new ArrayList<>();
        Collection<File> files = FileUtils.listFiles(directory, null, true);
        for (File f : files) {
            list.add(f.getAbsolutePath());
        }
        Collections.sort(list);
        return list;
    }


    public static Properties getProperties(String fileName) {
        Properties ps = new Properties();
        try {
            ps.load(OpenMLDBTool.class.getClassLoader().getResourceAsStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return ps;
    }

    public static String uuid() {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return uuid;
    }

    public static <T> void mergeObject(T origin, T destination) {
        if (origin == null || destination == null)
            return;
        if (!origin.getClass().equals(destination.getClass()))
            return;
        Field[] fields = origin.getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                fields[i].setAccessible(true);
                Object originValue = fields[i].get(origin);
                Object destValue = fields[i].get(destination);
                if (null == destValue) {
                    fields[i].set(destination, originValue);
                }
                fields[i].setAccessible(false);
            } catch (Exception e) {
            }
        }
    }

}












