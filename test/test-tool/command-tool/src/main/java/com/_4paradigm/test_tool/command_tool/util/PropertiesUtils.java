package com._4paradigm.test_tool.command_tool.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

@Data
@Slf4j
public class PropertiesUtils {

    public static Properties getProperties(String fileName){
        Properties ps = new Properties();
        try {
            ps.load(PropertiesUtils.class.getClassLoader().getResourceAsStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ps;
    }
}
