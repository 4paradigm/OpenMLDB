package com._4paradigm.openmldb.test_common.chain.result;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;

public class ResultParserManager {
    private AbstractResultHandler resultHandler;
    private ResultParserManager() {
        DescResultParser descResultParser = new DescResultParser();
        JobResultParser jobResultParser = new JobResultParser();
        OfflineSelectResultParser offlineSelectResultParser = new OfflineSelectResultParser();
        descResultParser.setNextHandler(jobResultParser);
        jobResultParser.setNextHandler(offlineSelectResultParser);
        resultHandler = descResultParser;
    }

    private static class ClassHolder {
        private static final ResultParserManager holder = new ResultParserManager();
    }

    public static ResultParserManager of() {
        return ClassHolder.holder;
    }
    public void parseResult(OpenMLDBResult openMLDBResult){
        resultHandler.doHandle(openMLDBResult);
    }
    
}
