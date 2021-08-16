package com._4paradigm.openmldb.batch.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Print the data in the result set into a table
 */
public class ResultSetUtil {
    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int ColumnCount = resultSetMetaData.getColumnCount();
        int[] columnMaxLengths = new int[ColumnCount];
        // Save data in ArrayList
        ArrayList<String[]> results = new ArrayList<>();
        while (rs.next()) {
            String[] columnStr = new String[ColumnCount];
            for (int i = 0; i < ColumnCount; i++) {
                columnStr[i] = rs.getString(i + 1);
                if (columnStr[i] != null) {
                    columnMaxLengths[i] = Math.max(columnMaxLengths[i],
                            Math.max(columnStr[i].length(), resultSetMetaData.getColumnName(i + 1).length()));
                } else {
                    columnMaxLengths[i] = Math.max(columnMaxLengths[i],
                            Math.max(4, resultSetMetaData.getColumnName(i + 1).length()));
                }
            }
            results.add(columnStr);
        }
        printSeparator(columnMaxLengths);
        printColumnName(resultSetMetaData, columnMaxLengths);
        printSeparator(columnMaxLengths);
        Iterator<String[]> iterator = results.iterator();
        String[] columnStr;
        while (iterator.hasNext()) {
            columnStr = iterator.next();
            for (int i = 0; i < ColumnCount; i++) {
                System.out.printf("|%" + columnMaxLengths[i] + "s", columnStr[i]);
            }
            System.out.println("|");
        }
        printSeparator(columnMaxLengths);
    }

    private static void printColumnName(ResultSetMetaData resultSetMetaData, int[] columnMaxLengths) throws SQLException {
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            System.out.printf("|%" + columnMaxLengths[i] + "s", resultSetMetaData.getColumnName(i + 1));
        }
        System.out.println("|");
    }

    private static void printSeparator(int[] columnMaxLengths) {
        for (int columnMaxLength : columnMaxLengths) {
            System.out.print("+");
            for (int j = 0; j < columnMaxLength; j++) {
                System.out.print("-");
            }
        }
        System.out.println("+");
    }

}
