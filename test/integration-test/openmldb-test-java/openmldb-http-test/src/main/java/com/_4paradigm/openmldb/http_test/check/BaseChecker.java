package com._4paradigm.openmldb.http_test.check;


import com._4paradigm.openmldb.test_common.common.Checker;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.restful.model.Expect;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

@Slf4j
public abstract class BaseChecker implements Checker {
    protected HttpResult httpResult;
    protected Expect expect;

    protected Logger logger = new LogProxy(log);

    public BaseChecker(HttpResult httpResult, Expect expect) {
        this.httpResult = httpResult;
        this.expect = expect;
    }
}
