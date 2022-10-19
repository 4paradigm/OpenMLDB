/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.qa.openmldb_deploy.util;

import com._4paradigm.test_tool.command_tool.common.LinuxUtil;
import org.testng.Assert;

/**
 * @author zhaowei
 * @date 2021/2/7 8:50 AM
 */
public class OpenMLDBCommandUtil {
    public static void cpOpenMLDB(String path, String openMLDBPath){
        boolean ok = LinuxUtil.cp(openMLDBPath,path+"/bin",path+"/bin/openmldb");
        Assert.assertTrue(ok,"copy conf fail");
    }
    public static void cpConf(String path,String confPath){
        boolean ok = LinuxUtil.cp(confPath,path,path+"/conf");
        Assert.assertTrue(ok,"copy conf fail");
    }
}
