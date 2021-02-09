package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql_auto_test.util.DeployUtil;
import com._4paradigm.fesql_auto_test.util.Tool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/11 11:34 AM
 */
@Slf4j
public class FesqlVersionConfig {

    public static final Properties CONFIG = Tool.getProperties("fesql_version.properties");

    public static String getUrl(String version){
        return CONFIG.getProperty(version);
    }
}
