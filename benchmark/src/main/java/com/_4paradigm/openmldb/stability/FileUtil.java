package com._4paradigm.openmldb.stability;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class FileUtil {
    public static String ReadFile(String path) {
        File file = new File(path);
        BufferedReader reader = null;
        StringBuilder builder = new StringBuilder();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr = null;
            while((tempStr = reader.readLine()) != null) {
                builder.append(tempStr);
                builder.append("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return builder.toString();
    }
}
