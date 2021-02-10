package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;

/**
 * 预期结果不一致
 */
@Data
public class UnequalExpect {
    /** 请求模式预期，查找顺序
     * 1.请求预期  2.在线预期 3.所有结果一致预期  4.expect */
    private ExpectDesc request_expect;
    /** batch模式预期，查找顺序
     * 1. batch预期  2.在线预期 3.所有结果一致预期  4.expect*/
    private ExpectDesc batch_expect;
    /** 在线预期，查找顺序
     * 1.在线预期 2.所有结果一致预期  3.expect */
    private ExpectDesc realtime_expect;
    /** 离线式预期，查找顺序
     * 1.离线预期 2.所有结果一致预期  3.expect */
    private ExpectDesc offline_expect;
    /** 请求批量模式预期，查找顺序
     * 1.请求批量模式预期 2.所有结果一致预期  3 expect */
    private ExpectDesc request_batch_expect;
    /** 存储过程预期，查找顺序
     * 1.存储过程预期 2.请求预期  3.在线预期 4.所有结果一致预期  5.expect */
    private ExpectDesc sp_expect;
    /** 所有结果一致时预期 */
    private ExpectDesc expect;
}
