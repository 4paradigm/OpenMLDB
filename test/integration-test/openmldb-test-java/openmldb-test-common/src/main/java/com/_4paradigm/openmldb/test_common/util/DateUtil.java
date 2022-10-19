package com._4paradigm.openmldb.test_common.util;

import org.apache.commons.lang3.time.DateUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtil {
    public static final String FILE_PATTERN = "yyyy-MM-dd_HH-mm-ss-SSS";
    public static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss.S";
    private static String[] parsePatterns = {"yyyy-MM-dd","yyyy年MM月dd日","yyyy-MM-dd'T'HH:mm:ss.SSS+08:00",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy/MM/dd","yyyy-MM-dd HH:mm:ss.S",
            "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm"};

    public static String getCurrentTimeByFileName(){
        DateTimeFormatter df = DateTimeFormatter.ofPattern(FILE_PATTERN);
        String str = df.format(LocalDateTime.now());
        return str;
    }
    /**
     * 判断字符串是否是日期格式
     * @param dateStr
     * @return
     */
    public static boolean isValidDate(String dateStr){
        boolean convertSuccess=true;
        if(dateStr.equals("")||dateStr.equals(null)){
            return false;
        }
        try {
            DateUtils.parseDate(dateStr,parsePatterns);
        } catch (ParseException e) {
            convertSuccess=false;
        }
        return convertSuccess;
    }

    /**
     * 毫秒格式转换为年月日格式
     * @param time
     * @return
     */
    public static String  parseDate(long time){
        SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_PATTERN);
        Date date = new Date(time);
        String format = dateFormat.format(date);
        return format;
    }

    /**
     * 把不同的日期格式转换为同意的格式 => yyyy-MM-dd HH:mm:ss.SSS
     * @param str
     * @return
     */
    public static String parseDate(String str){
        try {
            Date date = DateUtils.parseDate(str, parsePatterns);
            DateFormat df = new SimpleDateFormat(DEFAULT_PATTERN);
            String s = df.format(date);
            return s;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static long parseDateToLong(String str){
        try {
            Date date = DateUtils.parseDate(str, parsePatterns);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("date parse failed, date:"+str);
        }
    }

    public static String timeFormat(long num) {
        long hour = 0;
        long minute = 0;
        long second = 0;
        second = num % 60;
        num -= second;
        if (num > 0) {
            num /= 60;
            minute = num % 60;
            num -= minute;
            if (num > 0) {
                hour = num / 60;
            }
        }
        return hour + "hrs " + minute + "mins " + second + "sec";
    }
}
