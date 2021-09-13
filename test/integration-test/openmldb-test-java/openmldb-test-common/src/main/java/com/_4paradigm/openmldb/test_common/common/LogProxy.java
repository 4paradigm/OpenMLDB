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
package com._4paradigm.openmldb.test_common.common;

import org.slf4j.Logger;

/**
 * @author zhaowei
 * @date 2021/3/25 5:52 PM
 */
public class LogProxy extends LoggerAdapter{
    private Logger log;
    private ReportLog reportLog = ReportLog.of();
    private boolean isAddReport = true;
    public LogProxy(Logger log) {
        this.log = log;
    }
    public LogProxy(Logger log, boolean isAddReport) {
        this.log = log;
        this.isAddReport = isAddReport;
    }
    @Override
    public void info(String msg){
        log.info(msg);
        if(isAddReport) {
            reportLog.info(msg);
        }
    }
    @Override
    public void info(String msg,Object obj){
        log.info(msg,obj);
        if(isAddReport) {
            reportLog.info(msg, obj);
        }
    }
    @Override
    public void info(String msg,Object... objs){
        log.info(msg,objs);
        if(isAddReport) {
            reportLog.info(msg, objs);
        }
    }
    @Override
    public void error(String msg){
        log.error(msg);
        if(isAddReport) {
            reportLog.error(msg);
        }
    }
    @Override
    public void error(String msg,Object... objs){
        log.error(msg,objs);
        if(isAddReport) {
            reportLog.error(msg, objs);
        }
    }
    @Override
    public void warn(String msg){
        log.warn(msg);
        if(isAddReport) {
            reportLog.warn(msg);
        }
    }
    @Override
    public void warn(String msg,Object... objs){
        log.warn(msg,objs);
        if(isAddReport) {
            reportLog.warn(msg, objs);
        }
    }
}
