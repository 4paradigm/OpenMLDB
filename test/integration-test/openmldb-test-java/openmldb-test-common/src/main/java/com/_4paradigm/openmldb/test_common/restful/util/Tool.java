package com._4paradigm.openmldb.test_common.restful.util;


import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.message.BasicNameValuePair;
import sun.misc.BASE64Encoder;

import java.io.IOException;
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

    public static Properties getProperties(String path,Class c){
        Properties ps = new Properties();
        try {
            ps.load(c.getClassLoader().getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return ps;
    }
    public static  List<BasicNameValuePair> getBasicNameValuePair(Map<String,Object>dataMap){
        List<BasicNameValuePair> nvps = new ArrayList<BasicNameValuePair>();
        for (String key:dataMap.keySet()){
            BasicNameValuePair nv = new BasicNameValuePair(key,String.valueOf(dataMap.get(key)));
            nvps.add(nv);
        }
        return nvps;
    }

    public static String strTime(String format,long time){
        SimpleDateFormat strFormat = new SimpleDateFormat(format);
        if(time == 0){
            time = new Date().getTime();
        }
        return strFormat.format(time);
    }

    public static <T>String ArrToString(T[] arr){
        String str = "";
        for (int i=0;i<arr.length;i++) {
            str += arr[i]+",";
        }
        return str;
    }


    public static  String getUri(String urlString) throws MalformedURLException {
        if(urlString.endsWith("/")){
            urlString = urlString.substring(0,urlString.length()-1);
        }
        URL url = new URL(urlString);
        return url.getPath();
    }
    /*
    public static String getShortClassName(String longName){
        String[] clsname = longName.split("\\.");
        return clsname[clsname.length-1];
    }
    */

    /**
     * 根据 参数C，返回对应类型的 in对象，
     *
     * @param c
     * @param in
     * @return
     */
    public static Object converter(Class c, String in) {
        if(in.equals("null")){
            return null;
        }
        if (c == String.class) {
            return in;
        }
        if (c == int.class) {
            int index = in.lastIndexOf('.');
            if (index != -1)
                return Integer.parseInt(in.substring(0, index));
            return Integer.parseInt(in);
        }
        if (c == long.class) {
            return Long.parseLong(in);
        }
        if (c == double.class) {
            return Double.parseDouble(in);
        }
        if (c == float.class) {
            return Float.parseFloat(in);
        }
        if (c == Boolean.class) {
            return Boolean.parseBoolean(in);
        }
        if (c == boolean.class) {
            return Boolean.parseBoolean(in);
        }
        if (c == int[].class) {
            if(in.equals("null")) {
                return null;
            } else {
                return StringArrayToIntArray(in.split(",", -1));
            }
        }

        if (c == String[].class) {
            if(in.equals("null")) {
                return null;
            } else {
                return in.split(",", -1);
            }
        }
        return null;
    }

    public  static int [] StringArrayToIntArray(String[] src){
        int len = src.length;
        System.out.print(src);

        int [] result = new int[len];
        for (int i=0;i<len;i++){
            result[i] = Integer.valueOf(src[i]);
        }
        System.out.print(result);
        return result;
    }

    public static String uuid(){
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        return uuid;
    }


    private static final String PROJECT_NAME="InterfaceAutoTestFinance/";

    public static String getModelName(Class c){
        String classPath = c.getResource("").getPath();
        String[] ss = classPath.split(PROJECT_NAME);
        String modelName = ss[1].substring(0,ss[1].indexOf("/"));
        return modelName;
    }
    public static void setModelName(Class c){
        String modelName = getModelName(c);
        System.setProperty("model.name",modelName);
    }

    public static void main(String []  args) throws MalformedURLException {
     //   System.out.println(isJSONValid("{\"x\":\"b\"}"));
        System.out.println(uuid());
    }
}












