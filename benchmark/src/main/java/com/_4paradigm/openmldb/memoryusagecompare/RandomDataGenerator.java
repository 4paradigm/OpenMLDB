package com._4paradigm.openmldb.memoryusagecompare;

import java.security.SecureRandom;
import java.sql.Date;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomDataGenerator {
    private static final Random random = new SecureRandom();
    private static final String CHAR_LOWERCASE = "abcdefghijklmnopqrstuvwxyz";
    private static final String CHAR_UPPERCASE = CHAR_LOWERCASE.toUpperCase();
    private static final String CHAR_DIGITS = "0123456789";
    private static final String CHAR_ALPHANUMERIC = CHAR_LOWERCASE + CHAR_UPPERCASE + CHAR_DIGITS;

    // 1. 生成指定长度的随机字符串
    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(CHAR_ALPHANUMERIC.length());
            char randomChar = CHAR_ALPHANUMERIC.charAt(index);
            sb.append(randomChar);
        }
        return sb.toString();
    }

    // 2. 随机生成int32数据
    public static int generateRandomInt32() {
        return random.nextInt();
    }

    // 3. 随机生成int64(long)数据
    public static long generateRandomInt64() {
        return random.nextLong();
    }

    // 4. 随机生成float数据
    public static float generateRandomFloat() {
        return random.nextFloat();
    }

    // 5. 随机生成double数据
    public static double generateRandomDouble() {
        return random.nextDouble();
    }

    // 6. 随机生成date数据(格式: YYYY-mm-dd, 时间在2020-1-1到当前日期之前)
    public static Date generateRandomDate() {
        long startEpochMillis = 1577836800000L; // 2020-01-01 00:00:00
        long endEpochMillis = System.currentTimeMillis();
        long randomEpochMillis = startEpochMillis + (long) (random.nextDouble() * (endEpochMillis - startEpochMillis));

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(randomEpochMillis);
        return new Date(calendar.getTimeInMillis());
    }

    // 7. 随机生成timestamp数据(实际格式int64的时间戳, 时间点要求是date这一天的时间)
    public static long generateRandomTimestamp() {
        long startEpochMillis = 1577836800000L; // 2020-01-01 00:00:00
        long endEpochMillis = System.currentTimeMillis();
        return startEpochMillis + (long) (random.nextDouble() * (endEpochMillis - startEpochMillis));
    }


    public static void main(String[] args) {
        long startTime = System.nanoTime();
        for (int i = 0; i < 10000; i++) {
            System.out.println("随机str: " + generateRandomString(20));
            System.out.println("随机int32: " + generateRandomInt32());
            System.out.println("随机int64: " + generateRandomInt64());
            System.out.println("随机float: " + generateRandomFloat());
            System.out.println("随机double: " + generateRandomDouble());
            System.out.println("随机日期: " + generateRandomDate());

            System.out.println("随机时间戳: " + generateRandomTimestamp());
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Method execution time: " + TimeUnit.NANOSECONDS.toMillis(duration) + " ms");
    }
}