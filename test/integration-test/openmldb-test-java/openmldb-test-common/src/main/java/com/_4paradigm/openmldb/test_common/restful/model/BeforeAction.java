package com._4paradigm.openmldb.test_common.restful.model;



import com._4paradigm.openmldb.test_common.model.InputDesc;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class BeforeAction implements Serializable {
    private List<InputDesc> tables;
    private List<String> sqls;
}
