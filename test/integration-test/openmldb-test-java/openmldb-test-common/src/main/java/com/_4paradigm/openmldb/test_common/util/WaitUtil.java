package com._4paradigm.openmldb.test_common.util;


import com._4paradigm.openmldb.test_common.common.Condition;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class WaitUtil {
    public static boolean waitCondition(Condition condition) {
        return waitCondition(condition,10,600);
    }
    public static boolean waitCondition(Condition condition,Condition fail) {
        return waitCondition(condition,fail,10,600);
    }

    /**
     *
     * @param condition 等待的条件
     * @param interval 轮询间隔，单位为秒
     * @param timeout 轮询超时时间，单位为秒
     * @return 条件为真返回真，否则返回false
     * @throws Exception
     */
    private static boolean waitCondition(Condition condition, int interval, int timeout) {
        int count = 1;
        while (timeout > 0){
            log.info("retry count:{}",count);
            if (condition.execute()){
                return true;
            }else {
                timeout -= interval;
                Tool.sleep(interval*1000);
            }
            count++;
        }
        log.info("wait timeout!");
        return false;
    }
    /**
     *
     * @param condition 等待的条件
     * @param interval 轮询间隔，单位为秒
     * @param timeout 轮询超时时间，单位为秒
     * @return 条件为真返回真，否则返回false
     * @throws Exception
     */
    private static boolean waitCondition(Condition condition, Condition fail, int interval, int timeout) {
        int count = 1;
        while (timeout > 0){
            log.info("retry count:{}",count);
            if (condition.execute()){
                return true;
            } else if(fail.execute()){
                return false;
            }else {
                timeout -= interval;
                Tool.sleep(interval*1000);
            }
            count++;
        }
        log.info("wait timeout!");
        return false;
    }

}
