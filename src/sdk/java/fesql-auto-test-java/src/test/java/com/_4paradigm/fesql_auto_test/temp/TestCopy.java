package com._4paradigm.fesql_auto_test.temp;

import org.apache.commons.lang3.SerializationUtils;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/12/10 5:52 PM
 */
public class TestCopy {
    @Test
    public void test1(){
        List<Aoo> list = new ArrayList<>();
        Aoo aoo = new Aoo();
        for (int i=0;i<10;i++){
            Aoo a = SerializationUtils.clone(aoo);
            a.id=i;
            list.add(a);
            System.out.println(a.hashCode());
        }
        System.out.println(list);
    }
}
class Aoo extends Object implements Serializable{
    int id;

    @Override
    public String toString() {
        return "Aoo{" +
                "id=" + id +
                '}';
    }
}