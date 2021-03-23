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

package com._4paradigm.fesql_auto_test.util;


import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Tool {
    private static final Logger logger = LoggerFactory.getLogger(Tool.class);

    public static String getFilePath(String filename) {
        return Tool.class.getClassLoader().getResource(filename).getFile();
    }

    public static String getCasePath(String yamlCaseDir, String casePath) {
        String caseDir = StringUtils.isEmpty(yamlCaseDir) ? Tool.rtidbDir().getAbsolutePath() : yamlCaseDir;
        Assert.assertNotNull(caseDir);
        String caseAbsPath = caseDir + "/cases/" + casePath;
        logger.debug("fesql case absolute path: {}", caseAbsPath);
        return caseAbsPath;
    }

    public static File rtidbDir() {
        File directory = new File(".");
        directory = directory.getAbsoluteFile();
        while (null != directory) {
            if (directory.isDirectory() && "rtidb".equals(directory.getName())) {
                break;
            }
            logger.debug("current directory name {}", directory.getName());
            directory = directory.getParentFile();
        }

        if ("rtidb".equals(directory.getName())) {
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

    /**
     * 验证一个字符串是不是json格式
     *
     * @param test
     * @return 如果是返回true，否则返回false
     */
    public final static boolean isJSONValid(String test) {
        try {
            JSONObject.parse(test);
        } catch (JSONException ex) {
            return false;
        }
        return true;
    }

    public static Properties getProperties(String fileName) {
        Properties ps = new Properties();
        try {
            ps.load(Tool.class.getClassLoader().getResourceAsStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return ps;
    }

    public static String uuid() {
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return uuid;
    }

    public static String md5ByResources(String path) {
        try {
            return DigestUtils.md5Hex(Tool.class.getClassLoader().getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String md5(String path) {
        try {
            return DigestUtils.md5Hex(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(md5("mona-lisa.jpg"));
    }
}












