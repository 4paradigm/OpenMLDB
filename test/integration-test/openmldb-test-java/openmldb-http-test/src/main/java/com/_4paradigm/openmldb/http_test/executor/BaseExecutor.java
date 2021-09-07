package com._4paradigm.openmldb.http_test.executor;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.common.IExecutor;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.testng.Assert;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/13 10:18 AM
 */
@Slf4j
public abstract class BaseExecutor implements IExecutor {
    protected Logger logger = new LogProxy(log);
    protected HttpResult httpResult;
    protected RestfulCase restfulCase;
    protected SqlExecutor executor;
    protected FesqlResult fesqlResult;
    protected List<String> tableNames;

    public BaseExecutor(SqlExecutor executor,RestfulCase restfulCase){
        this.executor = executor;
        this.restfulCase = restfulCase;
    }
    @Override
    public boolean verify() {
        return false;
    }

    @Override
    public void run() {
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        System.out.println(className+"."+methodName+":"+restfulCase.getDesc() + " Begin!");
        logger.info(className+"."+methodName+":"+restfulCase.getDesc() + " Begin!");
//        boolean verify = false;
        try {
//            verify = verify();
//            if(!verify) return;
            if (null == restfulCase) {
                Assert.fail("executor run with null case");
                return;
            }
            prepare();
            execute();
            check();
            afterAction();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(className+"."+methodName+":"+restfulCase.getDesc() + " FAIL!");
            Assert.fail("executor run with exception");
        }finally {
            tearDown();
            System.out.println(className+"."+methodName+":"+restfulCase.getDesc() + " DONE!");
        }
    }

    protected abstract void afterAction();
}
