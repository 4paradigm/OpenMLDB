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

package com._4paradigm.openmldb.batch.utils;

import org.testng.annotations.Test;

public class TestByteArrayUtil {

    @Test
    public void testBytesToString() {
        byte[] inputBytes = new byte[] {0x00};
        String bytesString = ByteArrayUtil.bytesToString(inputBytes);
        assert bytesString.equals("00");
    }

    @Test
    public void testIntToByteArray() {
        int value = 1;
        byte[] bytes = ByteArrayUtil.intToByteArray(value);
        assert ByteArrayUtil.bytesToString(bytes).equals("00000001");
    }

    @Test
    public void testIntToOneByteArray() {
        int value = 1;
        byte[] bytes = ByteArrayUtil.intToOneByteArray(value);
        assert ByteArrayUtil.bytesToString(bytes).equals("01");
    }

}
