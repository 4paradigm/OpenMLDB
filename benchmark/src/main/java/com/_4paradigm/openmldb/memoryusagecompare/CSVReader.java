package com._4paradigm.openmldb.memoryusagecompare;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class CSVReader {

    private final CSVParser csvParser;

    public CSVReader(String filePath) throws IOException {
        InputStreamReader reader = new InputStreamReader(Objects.requireNonNull(CSVReader.class.getClassLoader().getResourceAsStream(filePath)));
        csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
    }

    public HashMap<String, ArrayList<TalkingData>> readCSV(int count) {
        HashMap<String, ArrayList<TalkingData>> map = new HashMap<>();

        for (int i = 0; i < count && csvParser.iterator().hasNext(); i++) {
            CSVRecord record = csvParser.iterator().next();
            convertData(record, map);
        }
        return map;
    }

    public void close() throws IOException {
        if (csvParser != null) {
            csvParser.close();
        }
    }

    private static void convertData(CSVRecord record, HashMap<String, ArrayList<TalkingData>> map) {
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

    public static void main(String[] args) {
        String filePath = "data/talkingdata.csv";
        int count = 1000; // 每次读取5行

        try {
            CSVReader reader = new CSVReader(filePath);
            HashMap<String, ArrayList<TalkingData>> data = reader.readCSV(count);

            for (ArrayList<TalkingData> rows : data.values()) {
                for (TalkingData row : rows) {
                    System.out.print(row + " ");
                }
                System.out.println();
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}