package com._4paradigm.openmldb.http_test.util;


import com._4paradigm.openmldb.http_test.common.RestfulGlobalVar;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;

public class CaseUtil {

    public static String caseNameFormat(RestfulCase sqlCase) {
        return String.format("%s_%s_%s",
                RestfulGlobalVar.env, sqlCase.getCaseId(), sqlCase.getDesc());
    }
}
