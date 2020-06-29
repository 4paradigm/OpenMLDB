package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.sql.Schema;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/15 11:36 AM
 */
@Data
public class FesqlResult {
    private boolean ok;
    private int count;
    private List<List> result;
    private Schema resultSchema;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("FesqlResult{ok=" + ok + ", count=" + count + "}");
        if(result!=null){
            builder.append("result="+result.size()+":\n");
            int columnCount = resultSchema.GetColumnCnt();
            String columnName = "i\t";
            for(int i=0;i<columnCount;i++){
                columnName+=resultSchema.GetColumnName(i)+"\t";
            }
            builder.append(columnName+"\n");
            for(int i=0;i<result.size();i++){
                List list = result.get(i);
                builder.append ((i+1)+"\t"+StringUtils.join(list,"\t")+"\n");
            }
        }
        return builder.toString();
    }
}
