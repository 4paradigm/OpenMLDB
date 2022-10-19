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

/**
 * @author zhaowei
 * @date 2021/2/6 9:43 PM
 */
public class DeployUtil {
    public static String BASE_PATH ;
    public static String getTestPath(String testPath,String version){
        String userHome = LinuxUtil.getHome();
        if(BASE_PATH!=null){
            return userHome+ BASE_PATH;
        }
        return userHome+"/"+testPath+"/"+ version;
    }
    public static String getTestPath(String version){
        return getTestPath("openmldb-auto-test",version);
    }
//    public static String getTestPath(){
//        return getTestPath("fedb-auto-test", FedbGlobalVar.version);
//    }

    public static String getOpenMLDBUrl(String version){
        String release_url = "https://github.com/4paradigm/OpenMLDB/releases/download/v%s/openmldb-%s-linux.tar.gz";
        return String.format(release_url,version,version);
    }
}
