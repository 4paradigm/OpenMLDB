package com._4paradigm.fesql.utils;

import java.io.*;

public class ConfigReader {
    public static String readConf(String path) throws Exception{
        BufferedReader reader = null;
        if (path.startsWith("classpath:")) {
            String parsedPath = path.replace("classpath:", "");
            InputStream is = ConfigReader.class.getResourceAsStream(parsedPath);
            InputStreamReader isr = new InputStreamReader(is, "utf-8");
            reader = new BufferedReader(isr);
        }else {
            reader = new BufferedReader(new FileReader(new File(path)));
        }
        StringBuilder builder = new StringBuilder();
        String line = reader.readLine();
        while (line != null) {
            builder.append(line + System.lineSeparator());
            line = reader.readLine();
        }
        return builder.toString();
    }
}
