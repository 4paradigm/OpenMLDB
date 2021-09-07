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
