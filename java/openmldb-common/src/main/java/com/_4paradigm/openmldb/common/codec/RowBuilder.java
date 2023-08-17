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

import java.nio.ByteBuffer;
import java.sql.Date;

public interface RowBuilder {

    boolean appendNULL();
    boolean appendBool(boolean val);
    boolean appendInt(int val);
    boolean appendSmallInt(short val);
    boolean appendTimestamp(long val);
    boolean appendBigInt(long val);
    boolean appendFloat(float val);
    boolean appendDouble(double val);
    boolean appendDate(Date date);
    boolean appendString(String val);

    boolean setNULL(int idx);
    boolean setBool(int idx, boolean val);
    boolean setInt(int idx, int val);
    boolean setSmallInt(int idx, short val);
    boolean setTimestamp(int idx, long val);
    boolean setBigInt(int idx, long val);
    boolean setFloat(int idx, float val);
    boolean setDouble(int idx, double val);
    boolean setDate(int idx, Date date);
    boolean setString(int idx, String val);

    boolean build();
    ByteBuffer getValue();
}
