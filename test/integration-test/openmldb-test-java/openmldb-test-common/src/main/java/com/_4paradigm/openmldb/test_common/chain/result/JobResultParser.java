package com._4paradigm.openmldb.test_common.chain.result;

import com._4paradigm.openmldb.test_common.bean.*;
import com._4paradigm.openmldb.test_common.util.ResultUtil;


public class JobResultParser extends AbstractResultHandler {

    @Override
    public boolean preHandle(SQLType sqlType) {
        return sqlType == SQLType.JOB;
    }

    @Override
    public void onHandle(OpenMLDBResult openMLDBResult) {
        OpenMLDBJob openMLDBJob = ResultUtil.parseJob(openMLDBResult);
        openMLDBResult.setOpenMLDBJob(openMLDBJob);
        if (!openMLDBJob.getState().equals("FINISHED")) {
            openMLDBResult.setOk(false);
            openMLDBResult.setMsg(openMLDBJob.getError());
        }
    }
}
