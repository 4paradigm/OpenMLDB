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
package com._4paradigm.openmldb.test_common.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import sun.misc.BASE64Encoder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class Tool {
    public static final Pattern PATTERN = Pattern.compile("<(.*?)>");

    public static String getFilePath(String filename) {
        return Tool.class.getClassLoader().getResource(filename).getFile();
    }

    public static String getCasePath(String yamlCaseDir, String casePath) {
        String caseDir = StringUtils.isEmpty(yamlCaseDir) ? Tool.openMLDBDir().getAbsolutePath() : yamlCaseDir;
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
            ps.load(Tool.class.getClassLoader().getResourceAsStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return ps;
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

    public static void genStr(String str, Map<String, List<String>> map, List<String> list){
        Matcher matcher = PATTERN.matcher(str);
        if (matcher.find()){
            String replace = matcher.group();
            String key = matcher.group(1);
            List<String> params = map.get(key);
            for(String param:params){
                String newStr = str.replace(replace,param);
                genStr(newStr,map,list);
            }
        }else{
            list.add(str);
        }
    }

    /**
     * 验证一个字符串是不是json格式
     * @param test
     * @return 如果是返回true，否则返回false
     */
    public final static boolean isJSONValid(String test) {
        try {
            JsonElement jsonElement = new JsonParser().parse(test);
            return jsonElement.isJsonObject() || jsonElement.isJsonArray();
        } catch (Exception ex) {
            return false;
        }
    }

    public static String base64(String md5Str,boolean isUpper){
        log.info(md5Str);
        byte[] b = null;
        String s = null;
        try {
            if(isUpper)
            {
                b = md5Str.toUpperCase().getBytes();
            }else {
                b= md5Str.toLowerCase().getBytes();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (b != null) {
            s = new BASE64Encoder().encode(b);
        }
        return s;
    }

    public static String md5(String s){
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str).toLowerCase();
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }
    }


    public static String uuid(){
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return uuid;
    }

}












