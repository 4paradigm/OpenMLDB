package com._4paradigm.openmldb.memoryusagecompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Summary {
    private static final Logger logger = LoggerFactory.getLogger(Summary.class);
    public final HashMap<String, HashMap<String, Long>> memSummary = new HashMap<>();
    public final ArrayList<ResourceUsage> indexMemSummary = new ArrayList<>();

    public void printMemUsageSummary() {
        int colWidth = 20;
        StringBuilder report = new StringBuilder();
        report.append(getRow(formatValue("label", colWidth, "center"), formatValue("RedisMem(bytes)", colWidth, "center"), formatValue("OpenMLDBMem(bytes)", colWidth, "center")));

        String border = "-";
        report.append(getRow(repeatString(border, colWidth), repeatString(border, colWidth), repeatString(border, colWidth)));

        List<Map.Entry<String, HashMap<String, Long>>> sortedEntries = new ArrayList<>(memSummary.entrySet());
        sortedEntries.sort(Comparator.comparingInt(entry -> Integer.parseInt(entry.getKey())));

        for (Map.Entry<String, HashMap<String, Long>> entry : sortedEntries) {
            String num = entry.getKey();
            HashMap<String, Long> memValues = entry.getValue();
            Long redisMem = memValues.get("redis");
            Long openmldbMem = memValues.get("openmldb");
            report.append(getRow(formatValue(num, colWidth), formatValue(redisMem, colWidth), formatValue(openmldbMem, colWidth)));
        }
        logger.info("\n====================\n" +
                "Summary report" +
                "\n====================\n" +
                report
        );
    }

    public void printIndexMemUsageSummary() {
        int colWidth = 35;
        StringBuilder report = new StringBuilder();
        report.append(Summary.getRow(Summary.formatValue("label", colWidth, "center"), Summary.formatValue("OpenMLDBMem(bytes)", colWidth, "center")));

        String border = "-";
        report.append(Summary.getRow(Summary.repeatString(border, colWidth), Summary.repeatString(border, colWidth)));

        long preMemUsage = 0L;
        for (ResourceUsage usage : indexMemSummary) {
            String label = usage.label;
            Long openmldbMem = usage.memoryUsage;

            report.append(Summary.getRow(Summary.formatValue(label, colWidth), Summary.formatValue(openmldbMem, colWidth)));
            if (preMemUsage == 0L) {
                preMemUsage = openmldbMem;
            }
        }
        logger.info("\n====================\n" +
                "Summary report" +
                "\n====================\n" +
                report
        );
    }


    public static String getRow(String... values) {
        StringBuilder row = new StringBuilder();
        row.append("|");
        for (String value : values) {
            row.append(" ").append(value).append(" |");
        }
        row.append("\n");
        return row.toString();
    }

    public static String repeatString(String str, int count) {
        StringBuilder repeatedStr = new StringBuilder();
        for (int i = 0; i < count; i++) {
            repeatedStr.append(str);
        }
        return repeatedStr.toString();
    }

    public static String formatValue(Object value, int maxLength) {
        return String.format("%" + (-maxLength) + "s", value.toString());
    }

    public static String formatValue(Object value, int maxLength, String align) {
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
