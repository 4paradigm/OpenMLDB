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

import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/16 10:42 PM
 */
public final class ReportLog {
    private ThreadLocal<List<String>> threadLog;
    private ReportLog() {
        threadLog = new ThreadLocal<>();
    }

    private static class ClassHolder {
        private static final ReportLog INSTANCE = new ReportLog();
    }

    public static ReportLog of() {
        return ClassHolder.INSTANCE;
    }

    public List<String> getLogs(){
        List<String> logs = threadLog.get();
        if(logs==null){
            logs = new ArrayList<>();
            threadLog.set(logs);
        }
        return logs;
    }

    public void info(String log){
        if(FesqlConfig.ADD_REPORT_LOG) {
            getLogs().add(log);
        }
    }

    public void info(String log,Object... objs){
        if(FesqlConfig.ADD_REPORT_LOG) {
            for (Object obj : objs) {
                log = StringUtils.replaceOnce(log, "{}", String.valueOf(obj));
            }
            info(log);
        }
    }

    public void error(String log){
        info(log);
    }

    public void error(String log,Object... objs){
        info(log,objs);
    }

    public void warn(String log){
        info(log);
    }

    public void warn(String log,Object... objs){
        info(log,objs);
    }

    public void clean(){
        List<String> logs = threadLog.get();
        if(logs!=null){
            logs.clear();
        }
    }

    public void remove(){
        List<String> logs = threadLog.get();
        if(logs!=null){
            threadLog.set(null);
        }
    }
}
