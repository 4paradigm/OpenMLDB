package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.fe.command.common.LinuxUtil;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.common.FesqlGlobalVar;
import org.apache.commons.lang3.StringUtils;

/**
 * @author zhaowei
 * @date 2021/2/6 9:43 PM
 */
public class DeployUtil {
    public static String getTestPath(String testPath,String version){
        String userHome = LinuxUtil.getHome();
        if(FesqlConfig.BASE_PATH!=null){
            return userHome+FesqlConfig.BASE_PATH;
        }
        return userHome+"/"+testPath+"/"+ version;
    }
    public static String getTestPath(String version){
        return getTestPath("fedb-auto-test",version);
    }
    public static String getTestPath(){
        return getTestPath("fedb-auto-test",FesqlGlobalVar.version);
    }

    public static String getRtidbUrl(String version){
        String release_url = "http://pkg.4paradigm.com/rtidb/rtidb-cluster-%s.tar.gz";
        // String version = StringUtils.join(version.toCharArray(),'.');
        return String.format(release_url,version);
    }
}
