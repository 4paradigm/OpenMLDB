package com._4paradigm.openmldb.memoryusagecompare;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class Utils {

    public static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        while (length-- > 0) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }
        return result.toString();
    }

    public static long getTimestamp(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(dateStr);
        return date.getTime();
    }
}
