package com._4paradigm.openmldb.test_common.restful.model;


import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class AfterAction implements Serializable {
    private List<String> sqls;
    private ExpectDesc expect;
}
