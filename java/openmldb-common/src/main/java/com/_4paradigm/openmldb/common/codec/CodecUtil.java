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

import com._4paradigm.openmldb.proto.Type.DataType;
import com._4paradigm.openmldb.proto.Common.ColumnDesc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CodecUtil {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public static final int VERSION_LENGTH = 2;
    public static final int SIZE_LENGTH = 4;
    public static final int HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
    public static final long UINT8_MAX = (1 << 8) - 1;
    public static final long UINT16_MAX = (1 << 16) - 1;
    public static final long UINT24_MAX = (1 << 24) - 1;
    public static final long UINT32_MAX = (1 << 32) - 1;
    public static final long DEFAULT_LONG = 1L;

    public static final Map<DataType, Integer> TYPE_SIZE_MAP = new HashMap<>();
    static {
        TYPE_SIZE_MAP.put(DataType.kBool, 1);
        TYPE_SIZE_MAP.put(DataType.kSmallInt, 2);
        TYPE_SIZE_MAP.put(DataType.kInt, 4);
        TYPE_SIZE_MAP.put(DataType.kFloat, 4);
        TYPE_SIZE_MAP.put(DataType.kBigInt, 8);
        TYPE_SIZE_MAP.put(DataType.kTimestamp, 8);
        TYPE_SIZE_MAP.put(DataType.kDouble, 8);
        TYPE_SIZE_MAP.put(DataType.kDate, 4);
    }

    public static int getBitMapSize(int size) {
        int tmp = 0;
        if (((size) & 0x07) > 0) {
            tmp = 1;
        }
        return (size >> 3) + tmp;
    }

    public static int getAddrLength(int size) {
        if (size <= UINT8_MAX) {
            return 1;
        } else if (size <= UINT16_MAX) {
            return 2;
        } else if (size <= UINT24_MAX) {
            return 3;
        } else {
            return 4;
        }
    }

    public static int calStrLength(Map<String, Object> row, List<ColumnDesc> schema) throws Exception {
        int strLength = 0;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row.get(columnDesc.getName());
            if (columnDesc.getDataType().equals(DataType.kVarchar) || columnDesc.getDataType().equals(DataType.kString)){
                if (!columnDesc.getNotNull() && column == null) {
                    continue;
                } else if (columnDesc.getNotNull() && column == null) {
                    throw new Exception("col " + columnDesc.getName() + " should not be null");
                }
                strLength += ((String)column).getBytes(CodecUtil.CHARSET).length;
            }
        }
        return strLength;
    }

    public static int calStrLength(Object[] row, List<ColumnDesc> schema) throws Exception {
        int strLength = 0;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row[i];
            if (columnDesc.getDataType().equals(DataType.kVarchar)
                    || columnDesc.getDataType().equals(DataType.kString)) {
                if (!columnDesc.getNotNull() && column == null) {
                    continue;
                } else if (columnDesc.getNotNull() && column == null) {
                    throw new Exception("col " + columnDesc.getName() + " should not be null");
                }
                strLength += ((String) column).length();
            }
        }
        return strLength;
    }


    /**
     * Convert date type to int value in OpenMLDB date format.
     *
     * @param date the date value
     * @return the int value
     */
    public static int dateToDateInt(Date date) {
        int year = date.getYear();
        int month = date.getMonth();
        int day = date.getDate();
        int returnValue = year << 16;
        returnValue = returnValue | (month << 8);
        returnValue = returnValue | day;
        return returnValue;
    }

    /**
     * Convert number of days to int value in OpenMLDB date format.
     *
     * @param days the number of days
     * @return the int value
     */
    public static int daysToDateInt(int days) {
        Date date = new Date(days * 86400000L);
        return dateToDateInt(date);
    }

    /**
     * Convert int value of OpenMLDB date format to date type.
     *
     * @param dateInt the int value
     * @return the date value
     */
    public static Date dateIntToDate(int dateInt) {
        int date = dateInt;
        int day = date & 0x0000000FF;
        date = date >> 8;
        int month = date & 0x0000FF;
        int year = date >> 8;
        return new Date(year, month, day);
    }

    /**
     * Convert int value of OpenMLDB date format to number of days.
     *
     * @param dateInt the int value
     * @return the number of days
     */
    public static int dateIntToDays(int dateInt) {
        Date date = dateIntToDate(dateInt);
        return (int)Math.ceil(date.getTime() / 86400000.0);
    }

}
