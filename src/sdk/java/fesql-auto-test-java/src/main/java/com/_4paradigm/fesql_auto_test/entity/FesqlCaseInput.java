package com._4paradigm.fesql_auto_test.entity;

import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/11 3:28 PM
 */
@Data
public class FesqlCaseInput {
    private String name = "auto_"+ RandomStringUtils.randomAlphabetic(8);
    private List<String> indexs;
    private List<String> columns;
    private List<List> rows;
    private String sql;
    private String resource;

    public String getSql(){
        if(sql==null){
            StringBuilder builder = new StringBuilder("create table "+name+"(");
//            for(String column:columns){
//                String[] ss = column.split(":");
//                builder.append(ss[0]+" "+ss[1]+" NOT NULL,");
//            }
            for(String column:columns){
                builder.append(column+",");
            }
            for(String index:indexs){
                String[] ss = index.split(":");
                if(ss.length==3){
                    builder.append(String.format("index(key=(%s),ts=%s),",ss[1],ss[2]));
                }else if(ss.length==4){
                    builder.append(String.format("index(key=(%s),ts=%s,ttl=%s),",ss[1],ss[2],ss[3]));
                }else if(ss.length==5){
                    builder.append(String.format("index(key=(%s),ts=%s,ttl=%s,ttl_type=%s),",ss[1],ss[2],ss[3],ss[4]));
                }
            }
            sql = builder.toString();
            if(sql.endsWith(",")){
                sql = sql.substring(0,sql.length()-1);
            }
            sql+=");";
        }
        return sql;
    }
    public String getColumnType(int index){
        return columns.get(index).split("\\s+")[1];
    }
}
