package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import org.testng.Assert;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CheckerStrategy {

    public static List<Checker> build(FesqlCase fesqlCase,FesqlResult fesqlResult) {
        List<Checker> checkList = new ArrayList<>();
        if (null == fesqlCase) {
            return checkList;
        }
        Map<String,Object> checkMap = fesqlCase.getExpect();
        for(String key:checkMap.keySet()){
            switch (key){
                case "rows":
                    checkList.add(new ResultChecker(fesqlCase,fesqlResult));
                    break;
                case "success":
                    checkList.add(new SuccessChecker(fesqlCase,fesqlResult));
                    break;
                case "result_column_name":
                    checkList.add(new ResultColumnNameChecker(fesqlCase,fesqlResult));
                    break;
                case "count":
                    checkList.add(new CountChecker(fesqlCase,fesqlResult));
                    break;
                case "result_column_type":
                    checkList.add(new ResultColumnTypeChecker(fesqlCase,fesqlResult));
                    break;
                case "columns":
                    checkList.add(new ColumnChecker(fesqlCase,fesqlResult));
                    break;
                case "order":
                    break;
                default:
                    Assert.assertFalse(true,"No Checker Available:"+key);
            }
        }
        return checkList;
    }
}
