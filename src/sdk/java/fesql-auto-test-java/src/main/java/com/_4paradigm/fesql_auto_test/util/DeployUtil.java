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

package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.fe.command.common.LinuxUtil;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.common.FesqlGlobalVar;

/**
 * @author zhaowei
 * @date 2021/2/6 9:43 PM
 */
public class DeployUtil {
    public static String getTestPath(String testPath,String version){
        String userHome = LinuxUtil.getHome();
        if(FesqlConfig.BASE_PATH!=null){
            return userHome+ FesqlConfig.BASE_PATH;
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
