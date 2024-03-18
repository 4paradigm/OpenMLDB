package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Summary {
    private static final Logger logger = LoggerFactory.getLogger(Summary.class);
    public final HashMap<String, HashMap<String, Long>> summary = new HashMap<>();

    public void printSummary() {
        int colWidth = 20;
        StringBuilder report = new StringBuilder();
        report.append(getRow(formatValue("num", colWidth, "center"), formatValue("redisMem", colWidth, "center"), formatValue("OpenMLDBMem", colWidth, "center")));

        String border = "-";
        report.append(getRow(repeatString(border, colWidth), repeatString(border, colWidth), repeatString(border, colWidth)));

        // Create a sorted list of entries based on num
        List<Map.Entry<String, HashMap<String, Long>>> sortedEntries = new ArrayList<>(summary.entrySet());
        sortedEntries.sort(Comparator.comparingInt(entry -> Integer.parseInt(entry.getKey())));

        for (Map.Entry<String, HashMap<String, Long>> entry : sortedEntries) {
            String num = entry.getKey();
            HashMap<String, Long> memValues = entry.getValue();
            Long redisMem = memValues.get("redis");
            Long openmldbMem = memValues.get("openmldb");
            report.append(getRow(formatValue(num, colWidth), formatValue(redisMem, colWidth), formatValue(openmldbMem, colWidth)));
        }
        logger.info(
                "\n====================\n" +
                        "Summary report" +
                        "\n====================\n" +
                        report
        );
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

    private static String formatValue(Object value, int maxLength, String align) {
        String valueStr = value.toString().trim();
        int valueLength = valueStr.length();
        if ("center".equals(align)) {
            int totalSpaces = maxLength - valueLength;
            int leftSpaces = totalSpaces / 2;
            int rightSpaces = totalSpaces - leftSpaces;

            return String.format("%" + leftSpaces + "s", "") +
                    valueStr +
                    String.format("%" + rightSpaces + "s", "");
        } else if ("left".equals(align)) {
            String formatStr = "%" + (-maxLength) + "s";
            return String.format(formatStr, valueStr);
        } else {
            String formatStr = "%" + maxLength + "s";
            return String.format(formatStr, valueStr);
        }
    }
}
