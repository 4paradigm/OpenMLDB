package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Summary {
    private static final Logger logger = LoggerFactory.getLogger(Summary.class);
    public final HashMap<String, HashMap<String, Integer>> summary = new HashMap<>();

    public void printSummary() {
        StringBuilder report = new StringBuilder();
        report.append(getRow("num", "redisMem", "OpenMLDBMem"));
        int colWidth = 20;
        String border = "-";
        report.append(getRow(repeatString(border, colWidth), repeatString(border, colWidth), repeatString(border, colWidth)));

        // Create a sorted list of entries based on num
        List<Map.Entry<String, HashMap<String, Integer>>> sortedEntries = new ArrayList<>(summary.entrySet());
        sortedEntries.sort(Comparator.comparingInt(entry -> Integer.parseInt(entry.getKey())));

        for (Map.Entry<String, HashMap<String, Integer>> entry : sortedEntries) {
            String num = entry.getKey();
            HashMap<String, Integer> memValues = entry.getValue();
            Integer redisMem = memValues.get("redis");
            Integer openmldbMem = memValues.get("openmldb");
            report.append(getRow(formatValue(num, colWidth), formatValue(redisMem, colWidth), formatValue(openmldbMem, colWidth)));
        }
        logger.info("Summary report:\n" + report);
    }


    private static String getRow(String... values) {
        StringBuilder row = new StringBuilder();
        row.append("|");
        for (String value : values) {
            row.append(" ").append(value).append(" |");
        }
        row.append("\n");
        return row.toString();
    }

    private static String repeatString(String str, int count) {
        StringBuilder repeatedStr = new StringBuilder();
        for (int i = 0; i < count; i++) {
            repeatedStr.append(str);
        }
        return repeatedStr.toString();
    }

    private static String formatValue(Object value, int maxLength) {
        return String.format("%" + (-maxLength) + "s", value.toString());
    }
}
