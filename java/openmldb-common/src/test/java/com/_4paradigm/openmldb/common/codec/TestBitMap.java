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

package com._4paradigm.openmldb.common.codec;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestBitMap {
    @Test
    public void testValue() {
        ByteBitMap bitmap = new ByteBitMap(10);
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(bitmap.at(i));
        }
        for (int i = 0; i < 10; i++) {
            bitmap.atPut(i, true);
            Assert.assertTrue(bitmap.at(i));
        }
        bitmap.clear();
        List<Integer> v = Arrays.asList(1, 4, 7);
        for (int i : v) {
            bitmap.atPut(i, true);
        }
        for (int i = 0; i < 10; i++) {
            if (v.contains(i)) {
                Assert.assertTrue(bitmap.at(i));
            } else {
                Assert.assertFalse(bitmap.at(i));
            }
        }
    }

    @Test
    public void testClear() {
        ByteBitMap bitmap = new ByteBitMap(10);
        bitmap.clear();
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(bitmap.at(i));
        }
    }

    @Test
    public void testSize() {
        Assert.assertEquals(new ByteBitMap(1).getBuffer().length, 1);
        Assert.assertEquals(new ByteBitMap(7).getBuffer().length, 1);
        Assert.assertEquals(new ByteBitMap(8).getBuffer().length, 1);
        Assert.assertEquals(new ByteBitMap(9).getBuffer().length, 2);
        Assert.assertEquals(new ByteBitMap(15).getBuffer().length, 2);
        Assert.assertEquals(new ByteBitMap(16).getBuffer().length, 2);
        Assert.assertEquals(new ByteBitMap(17).getBuffer().length, 3);
        Assert.assertEquals(new ByteBitMap(100).getBuffer().length, 13);
    }

    @Test
    public void testSetted() {
        for (int i = 1; i < 1000; i++) {
            ByteBitMap bitmap = new ByteBitMap(i + 1);
            Assert.assertFalse(bitmap.allSetted());
            bitmap.atPut(i % 5, true);
            Assert.assertFalse(bitmap.allSetted());
            for (int j = 0; j < bitmap.size(); j++) {
                bitmap.atPut(j, true);
            }
            Assert.assertTrue(bitmap.allSetted());
        }

    }
}
