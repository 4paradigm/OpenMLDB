package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.Schema;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class ResultChecker extends BaseChecker {

    public ResultChecker(FesqlCase fesqlCase, FesqlResult fesqlResult){
        super(fesqlCase,fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("result check");
        List<List> expect =  (List<List>)fesqlCase.getExpect().get("rows");
        List<List> actual = fesqlResult.getResult();
        log.info("expect:{}",expect);
        log.info("actual:{}",actual);
        Assert.assertEquals(actual.size(),expect.size());
        List<String> columns =  (List<String>)fesqlCase.getExpect().get("columns");
        if(columns!=null&&columns.size()>0){
            expect = convertRows(expect,columns);
        }
        String orderName = (String)fesqlCase.getExpect().get("order");
        if(orderName!=null&&orderName.length()>0){
            Schema schema = fesqlResult.getResultSchema();
            int index = FesqlUtil.getIndexByColumnName(schema,orderName);
            Collections.sort(expect,new RowsSort(index));
            Collections.sort(actual,new RowsSort(index));
            log.info("order expect:{}",expect);
            log.info("order actual:{}",actual);
        }
        if(columns!=null&&columns.size()>0) {
//            for(int i=0;i<actual.size();i++){
//                List actualList = actual.get(i);
//                List expectList = expect.get(i);
//                checkListAsString(actualList,expectList);
//            }
            Assert.assertEquals(actual, expect);
        }else{
            for(int i=0;i<actual.size();i++){
                List actualList = actual.get(i);
                List expectList = expect.get(i);
                checkListAsString(actualList,expectList);
            }
        }
    }
    private void checkListAsString(List actual,List expect){
        Assert.assertEquals(actual.size(),expect.size());
        for(int i=0;i<actual.size();i++){
            Object actualObj = actual.get(i);
//            System.out.println(actualObj);
//            System.out.println("》》："+(actualObj instanceof String));
//            System.out.println(actualObj.getClass());
            Object expectObj = expect.get(i);
            Assert.assertEquals(actualObj+"",expectObj+"");
        }
    }
    public class RowsSort implements Comparator<List>{
        private int index;
        public RowsSort(int index){
            this.index = index;
        }
        @Override
        public int compare(List o1, List o2) {
            Object obj1 = o1.get(index);
            Object obj2 = o2.get(index);
            if(obj1 instanceof Comparable && obj2 instanceof Comparable){
                return ((Comparable) obj1).compareTo(obj2);
            }else{
                return obj1.hashCode()-obj2.hashCode();
            }
        }
    }
    private List<List> convertRows(List<List> rows,List<String> columns){
        List<List> list = new ArrayList<>();
        for(List row:rows){
            list.add(convertList(row,columns));
        }
        return list;
    }
    private List convertList(List datas,List<String> columns){
        List list = new ArrayList();
        for(int i=0;i<datas.size();i++){
            Object obj = datas.get(i);
            if(obj==null){
                list.add(null);
            }else {
                String data = obj+"";
                String column = columns.get(i);
                list.add(convertData(data, column));
            }
        }
        return list;
    }
    private Object convertData(String data,String column){
        String[] ss = column.split("\\s+");
        String type = ss[ss.length-1];
        Object obj = null;
        switch (type){
            case "int":
                obj = Integer.parseInt(data);
                break;
            case "bigint":
                obj = Long.parseLong(data);
                break;
            case "float":
                obj = Float.parseFloat(data);
                break;
            case "double":
                obj = Double.parseDouble(data);
                break;
            case "bool":
                obj = Boolean.parseBoolean(data);
                break;
            case "string":
                obj = data;
                break;
            case "timestamp":
                obj = Long.parseLong(data);
                break;
            case "date":
                obj = data;
                break;
            default:
                obj = data;
                break;
        }
        return obj;
    }
}
