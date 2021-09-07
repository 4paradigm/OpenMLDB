package com._4paradigm.openmldb.java_sdk_test.temp;


import com._4paradigm.openmldb.java_sdk_test.util.Sqlite3Conn;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author zhaowei
 * @date 2021/3/8 6:24 PM
 */
public class TestSplite3 {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            Connection connection= Sqlite3Conn.of().getConnection();//连接数据库zhou.db,不存在则创建
            Statement statement=connection.createStatement();   //创建连接对象，是Java的一个操作数据库的重要接口
            String sql="create table auto_dOIozCAS(\n" +
                    "c1 string,\n" +
                    "c2 smallint,\n" +
                    "c3 int,\n" +
                    "c4 bigint,\n" +
                    "c5 float,\n" +
                    "c6 double,\n" +
                    "c7 timestamp,\n" +
                    "c8 date,\n" +
                    "c9 bool" +
                    ")";
            statement.executeUpdate("drop table if exists auto_dOIozCAS");//判断是否有表tables的存在。有则删除
            int n = statement.executeUpdate(sql);
            System.out.println(n);
            statement.executeUpdate("create index index1 on auto_dOIozCAS(c1)");
            ResultSet rSet=statement.executeQuery("select name from sqlite_master where type='table' order by name");
            while (rSet.next()) {            //遍历这个数据集
                System.out.println("table："+rSet.getString(1));//依次输出 也可以这样写 rSet.getString(“name”)
            }
            //创建数据库
            statement.executeUpdate("insert into auto_dOIozCAS values('zhou',1,2,3,1.1,2.1,10000,'2020-01-01',true)");//向数据库中插入数据
            rSet=statement.executeQuery("select*from auto_dOIozCAS");//搜索数据库，将搜索的放入数据集ResultSet中
            while (rSet.next()) {            //遍历这个数据集
                System.out.println("姓名："+rSet.getString(1));//依次输出 也可以这样写 rSet.getString(“name”)
                System.out.println("密码："+rSet.getString("c8"));
            }
            rSet.close();//关闭数据集
            connection.close();//关闭数据库连接
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

