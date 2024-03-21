package com._4paradigm.openmldb.memoryusagecompare;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Random;

public class Utils {
    public static HashMap<String, ArrayList<TalkingData>> readTalkingDataFromCsv(String dataFile) {
        HashMap<String, ArrayList<TalkingData>> map = new HashMap<>();
        try (InputStreamReader reader = new InputStreamReader(Objects.requireNonNull(BenchmarkMemoryUsageByTalkingData.class.getClassLoader().getResourceAsStream(dataFile)))) {
            CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            for (CSVRecord record : parser) {
                TalkingData td = new TalkingData();
                String ip = record.get("ip");
                td.setIp(ip);
                td.setApp(Integer.parseInt(record.get("app")));
                td.setDevice(Integer.parseInt(record.get("device")));
                td.setOs(Integer.parseInt(record.get("os")));
                td.setChannel(Integer.parseInt(record.get("channel")));
                td.setClickTime(record.get("click_time"));
                td.setIsAttribute(Integer.parseInt(record.get("is_attributed")));
                map.computeIfAbsent(ip, v -> new ArrayList<>());
                map.get(ip).add(td);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder result = new StringBuilder();
        Random random = new Random();
        while (length-- > 0) {
            result.append(characters.charAt(random.nextInt(characters.length())));
        }
        return result.toString();
    }
}
