/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
