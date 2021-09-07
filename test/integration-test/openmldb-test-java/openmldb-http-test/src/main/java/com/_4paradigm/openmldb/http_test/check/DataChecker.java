package com._4paradigm.openmldb.http_test.check;

import com._4paradigm.openmldb.test_common.restful.model.Expect;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;

import java.util.Map;

public class DataChecker extends BaseChecker {
    public DataChecker(HttpResult httpResult, Expect expect) {
        super(httpResult, expect);
    }

    @Override
    public void check() throws Exception {
        logger.info("data check begin");
        Map<String, Object> data = expect.getData();
        if(MapUtils.isEmpty(data)){
            return ;
        }
        String resultData = httpResult.getData();
        if(data.containsKey("code")){
            Object expectCode = data.get("code");
            Object actualCode = JsonPath.read(resultData, "$.code");
            data.remove("code");
            Assert.assertEquals(actualCode,expectCode,"code不一致");
        }
        if(data.containsKey("msg")){
            Object expectMsg = data.get("msg");
            Object actualMsg = JsonPath.read(resultData, "$.msg");
            data.remove("msg");
            Assert.assertEquals(actualMsg,expectMsg,"msg不一致");
        }
        if(data.containsKey("msg-contains")){
            String expectMsg = (String)data.get("msg-contains");
            String actualMsg = JsonPath.read(resultData, "$.msg");
            data.remove("msg-contains");
            Assert.assertTrue(actualMsg.contains(expectMsg),"actualMsg不包含expectMsg，actualMsg："+actualMsg+"，expectMsg："+expectMsg);
        }
        if(MapUtils.isNotEmpty(data)){
            for(String key:data.keySet()){
                Object expectValue = data.get(key);
                Object actualValue = JsonPath.read(resultData,key);
                Assert.assertEquals(actualValue,expectValue,"data value 不一致");
            }
        }
    }
}
