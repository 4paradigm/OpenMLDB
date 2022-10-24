package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.jdbc.SQLResultSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
@Slf4j
public class DataUtil {

    public static String parseBinary(String str,String type){
        int length = str.length();
//        System.out.println("length = " + length);
        String binaryStr = BinaryUtil.strToBinaryStr(str);
        switch (type){
            case "smallint":
                return String.valueOf(Short.parseShort(binaryStr, 2));
            case "int":
                return String.valueOf(Integer.parseInt(binaryStr, 2));
            case "bigint":
                return String.valueOf(Long.parseLong(binaryStr, 2));
            case "timestamp":
                String binary = "";
                for (int i = 0; i < length; i++) {
                    String s = Integer.toBinaryString(str.charAt(i));
                    System.out.println("s = " + s);
                    s = StringUtils.leftPad(s, 16, "0");
                    System.out.println("AAAAA s = " + s);
                    binary += s;
                }
                System.out.println("binary = " + binary);
                return String.valueOf(Long.parseLong(binary, 2));
            case "float":
//                return String.valueOf(Float.intBitsToFloat(new BigInteger(binaryStr, 2).intValue()));
                return BinaryUtil.strToStr(str);
            case "double":
                return String.valueOf(Double.longBitsToDouble(new BigInteger(binaryStr, 2).longValue()));
            case "date":
                int year = (int)(str.charAt(2))+1900;
                int month = (int)(str.charAt(1))+1;
                int day = str.charAt(0);
                return year+"-"+(month<10?"0"+month:month)+"-"+(day<10?"0"+day:day);
            case "string":
                return str;
            default:
                throw new IllegalArgumentException("parse binary not support type:"+type);
        }

    }

    public static Object parseTime(Object data){
        String dataStr = String.valueOf(data);
        if(dataStr.equals("{currentTime}")){
            return System.currentTimeMillis();
        }else if(dataStr.startsWith("{currentTime}-")){
            long t = Long.parseLong(dataStr.substring(14));
            return System.currentTimeMillis()-t;
        }else if(dataStr.startsWith("{currentTime}+")){
            long t = Long.parseLong(dataStr.substring(14));
           return System.currentTimeMillis()+t;
        }
        return data;
    }
    public static Object parseRules(String data){
        Object obj = null;
        if(data.equals("{currentTime}")){
            obj = System.currentTimeMillis();
        }else if(data.startsWith("{currentTime}-")){
            long t = Long.parseLong(data.substring(14));
            obj = System.currentTimeMillis()-t;
        }else if(data.startsWith("{currentTime}+")){
            long t = Long.parseLong(data.substring(14));
            obj = System.currentTimeMillis()+t;
        }else{
            obj = data;
        }
        return obj;
    }
    public static boolean setPreparedData(PreparedStatement ps, List<String> parameterType, List<Object> objects) throws SQLException {
        for(int i=0;i<objects.size();i++){
            String type = parameterType.get(i);
            Object value = objects.get(i);
            switch (type){
                case "varchar":
                case "string":
                    ps.setString(i+1,String.valueOf(value));
                    break;
                case "bool":
                    ps.setBoolean(i+1,Boolean.parseBoolean(String.valueOf(value)));
                    break;
                case "int16":
                case "smallint":
                    ps.setShort(i+1,Short.parseShort(String.valueOf(value)));
                    break;
                case "int":
                    ps.setInt(i+1,Integer.parseInt(String.valueOf(value)));
                    break;
                case "int64":
                case "long":
                case "bigint":
                    ps.setLong(i+1,Long.parseLong(String.valueOf(value)));
                    break;
                case "float":
                    ps.setFloat(i+1,Float.parseFloat(String.valueOf(value)));
                    break;
                case "double":
                    ps.setDouble(i+1,Double.parseDouble(String.valueOf(value)));
                    break;
                case "timestamp":
                    ps.setTimestamp(i+1,new Timestamp(Long.parseLong(String.valueOf(value))));
                    break;
                case "date":
                    try {
                        Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse(String.valueOf(value)).getTime());
                        ps.setDate(i + 1, date);
                        break;
                    }catch (ParseException e){
                        e.printStackTrace();
                        return false;
                    }
                default:
                    throw new IllegalArgumentException("type not match");
            }
        }
        return true;
    }
    public static boolean setRequestData(PreparedStatement requestPs, List<Object> objects) throws SQLException {
        ResultSetMetaData metaData = requestPs.getMetaData();
        int totalSize = 0;
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            if (null == objects.get(i)) {
                continue;
            }
            if (metaData.getColumnType(i + 1) == Types.VARCHAR) {
                totalSize += objects.get(i).toString().length();
            }
        }
        log.info("init request row: {}", totalSize);
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            Object obj = objects.get(i);
            if (null == obj || obj.toString().equalsIgnoreCase("null")) {
                requestPs.setNull(i + 1, 0);
                continue;
            }
            obj = DataUtil.parseTime(obj);
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                requestPs.setBoolean(i + 1, Boolean.parseBoolean(obj.toString()));
            } else if (columnType == Types.SMALLINT) {
                requestPs.setShort(i + 1, Short.parseShort(obj.toString()));
            } else if (columnType == Types.INTEGER) {
                requestPs.setInt(i + 1, Integer.parseInt(obj.toString()));
            } else if (columnType == Types.BIGINT) {
                requestPs.setLong(i + 1, Long.parseLong(obj.toString()));
            } else if (columnType == Types.FLOAT) {
                requestPs.setFloat(i + 1, Float.parseFloat(obj.toString()));
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, Double.parseDouble(obj.toString()));
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(Long.parseLong(obj.toString())));
            } else if (columnType == Types.DATE) {
                if (obj instanceof java.util.Date) {
                    requestPs.setDate(i + 1, new Date(((java.util.Date) obj).getTime()));
                } else if (obj instanceof Date) {
                    requestPs.setDate(i + 1, (Date) (obj));
                }
//                else if (obj instanceof DateTime) {
//                    requestPs.setDate(i + 1, new Date(((DateTime) obj).getMillis()));
//                }
                else {
                    try {
                        Date date = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(obj.toString() + " 00:00:00").getTime());
                        log.info("build request row: obj: {}, append date: {},  {}, {}, {}",obj, date.toString(), date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                        requestPs.setDate(i + 1, date);
                    } catch (ParseException e) {
                        log.error("Fail convert {} to date: {}", obj, e);
                        return false;
                    }
                }
            } else if (columnType == Types.VARCHAR) {
                requestPs.setString(i + 1, obj.toString());
            } else {
                log.error("fail to build request row: invalid data type {]", columnType);
                return false;
            }
        }
        return true;
    }
    public static List<List<Object>> convertRows(List<List<Object>> rows, List<String> columns) throws ParseException {
        List<List<Object>> list = new ArrayList<>();
        for (List row : rows) {
            list.add(DataUtil.convertList(row, columns));
        }
        return list;
    }


    public static List<Object> convertList(List<Object> datas, List<String> columns) throws ParseException {
        List<Object> list = new ArrayList();
        for (int i = 0; i < datas.size(); i++) {
            if (datas.get(i) == null) {
                list.add(null);
            } else {
                String obj = datas.get(i).toString();
                String column = columns.get(i);
                list.add(convertData(obj, column));
            }
        }
        return list;
    }

    public static Object convertData(String data, String column) throws ParseException {
        String[] ss = column.split("\\s+");
        String type = ss[ss.length - 1];
        Object obj = null;
        if(data == null){
            return null;
        }
        if ("null".equalsIgnoreCase(data)) {
            return "null";
        }
        switch (type) {
            case "smallint":
            case "int16":
                obj = Short.parseShort(data);
                break;
            case "int32":
            case "i32":
            case "int":
                obj = Integer.parseInt(data);
                break;
            case "int64":
            case "bigint":
                obj = Long.parseLong(data);
                break;
            case "float": {
                if (data.equalsIgnoreCase("nan")||data.equalsIgnoreCase("-nan")) {
                    obj = Float.NaN;
                }else if(data.equalsIgnoreCase("inf")){
                    obj = Float.POSITIVE_INFINITY;
                }else if(data.equalsIgnoreCase("-inf")){
                    obj = Float.NEGATIVE_INFINITY;
                }else {
                    obj = Float.parseFloat(data);
                }
                break;
            }
            case "double": {
                if (data.equalsIgnoreCase("nan")||data.equalsIgnoreCase("-nan")) {
                    obj = Double.NaN;
                }else if(data.equalsIgnoreCase("inf")){
                    obj = Double.POSITIVE_INFINITY;
                }else if(data.equalsIgnoreCase("-inf")){
                    obj = Double.NEGATIVE_INFINITY;
                }else {
                    obj = Double.parseDouble(data);
                }
                break;
            }
            case "bool":
                obj = Boolean.parseBoolean(data);
                break;
            case "string":
                obj = data;
                break;
            case "timestamp":
                if(data.matches("^\\d+$")){
                    obj = new Timestamp(Long.parseLong(data));
                }else{
                    obj = new Timestamp(DateUtil.parseDateToLong(data));
                }
                break;
            case "date":
                try {
                    obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data.trim() + " 00:00:00").getTime());
                } catch (ParseException e) {
                    log.error("Fail convert {} to date", data.trim());
                    throw e;
                }
                break;
            default:
                obj = data;
                break;
        }
        return obj;
    }
}
