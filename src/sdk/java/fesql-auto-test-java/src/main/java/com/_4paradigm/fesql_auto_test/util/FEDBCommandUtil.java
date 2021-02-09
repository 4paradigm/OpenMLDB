package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.fe.command.common.ExecutorUtil;
import com._4paradigm.fe.command.common.LinuxUtil;
import org.testng.Assert;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/7 8:50 AM
 */
public class FEDBCommandUtil {
    public static void cpRtidb(String path,String fedbPath){
        boolean ok = LinuxUtil.cp(fedbPath,path+"/bin",path+"/bin/fedb");
        Assert.assertTrue(ok,"copy conf fail");
    }
    public static void cpConf(String path,String confPath){
        boolean ok = LinuxUtil.cp(confPath,path,path+"/conf");
        Assert.assertTrue(ok,"copy conf fail");
    }
}
