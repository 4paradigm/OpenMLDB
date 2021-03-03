/*
 * FesqlFlinkCodec.java
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

package com._4paradigm.fesql.flink.common;

import com._4paradigm.fesql.codec.RowBuilder;
import com._4paradigm.fesql.codec.RowView;
import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.CoreAPI;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static com._4paradigm.fesql.type.TypeOuterClass.Type.*;

public class FesqlFlinkCodec {

    private List<List<TypeOuterClass.ColumnDef>> columnDefLists;
    private int sliceNum;
    private List<List<Integer>> stringFieldIndexes;
    private List<RowBuilder> rowBuilders;
    private List<RowView> rowViews;
    private Map<Integer, Integer> sliceStartIndexMap;

    public FesqlFlinkCodec(List<List<TypeOuterClass.ColumnDef>> columnDefLists) {
        this.columnDefLists = columnDefLists;
        this.sliceNum = columnDefLists.size();

        // Init string field index
        this.stringFieldIndexes = new ArrayList<List<Integer>>();
        for (int i=0; i < this.sliceNum; i++) {
            List<Integer> stringFieldIndex = new ArrayList<Integer>();

            List<TypeOuterClass.ColumnDef> columnDefList = this.columnDefLists.get(i);
            for (int j=0; j < columnDefList.size(); ++j) {
                if (columnDefList.get(j).getType() == kVarchar) {
                    stringFieldIndex.add(j);
                }
            }

            this.stringFieldIndexes.add(stringFieldIndex);
        }

        // Init row builder and row view
        this.rowBuilders = new ArrayList<RowBuilder>();
        this.rowViews = new ArrayList<RowView>();
        for (int i=0; i < this.sliceNum; ++i) {
            RowBuilder rowBuilder = new RowBuilder(this.columnDefLists.get(i));
            this.rowBuilders.add(rowBuilder);

            RowView rowView = new RowView(this.columnDefLists.get(i));
            this.rowViews.add(rowView);
        }

        // Init the map whose key is slice index and value is start index
        this.sliceStartIndexMap = new HashMap<Integer ,Integer>();
        int currentOffset = 0;
        for (int i=0; i < this.sliceNum; ++i) {
            this.sliceStartIndexMap.put(i, currentOffset);
            currentOffset += this.columnDefLists.get(i).size();
        }

    }

    /**
     * Encode Flink row to FESQL row.
     */
    public com._4paradigm.fesql.codec.Row encodeFlinkRow(Row flinkRow) throws FesqlException {
        com._4paradigm.fesql.codec.Row fesqlRow = null;

        List<Integer> sliceSizes = getFesqlRowSliceSizes(flinkRow);

        for (int i=0; i < this.sliceNum; ++i) {
            int sliceSize = sliceSizes.get(i);

            if (i == 0) {
                // Init the row for the first item
                fesqlRow = CoreAPI.NewRow(sliceSize);
                long outputBufPointer = CoreAPI.GetRowBuf(fesqlRow, 0);
                encodeSingleFlinkRow(flinkRow, outputBufPointer, sliceSize, i);
            } else {
                // Append the slice data for the row
                long outputBufPointer = CoreAPI.AppendRow(fesqlRow, sliceSize);
                encodeSingleFlinkRow(flinkRow, outputBufPointer, sliceSize, i);
            }
        }

        return fesqlRow;
    }

    public Row decodeFesqlRow(com._4paradigm.fesql.codec.Row fesqlRow) throws FesqlException {

        int totalFieldNum = 0;
        for (int i=0; i < this.sliceNum; ++i) {
            totalFieldNum += this.columnDefLists.get(i).size();
        }

        Row flinkRow = new Row(totalFieldNum);

        for (int i=0; i < this.sliceNum; ++i) {
            RowView rowView = this.rowViews.get(i);
            List<TypeOuterClass.ColumnDef> columnDefs = this.columnDefLists.get(i);

            if (!rowView.Reset(fesqlRow.buf(i), fesqlRow.size(i))) {
                throw new FesqlException("Fail to setup row builder, maybe row buf is corrupted");
            }

            int fieldNum = columnDefs.size();
            int totalFieldIndex = this.sliceStartIndexMap.get(i);

            for (int j=0; j < fieldNum; ++j) {
                if (rowView.IsNULL(j)) {
                    flinkRow.setField(totalFieldIndex, null);
                } else {
                    TypeOuterClass.ColumnDef columnDef = columnDefs.get(j);
                    TypeOuterClass.Type columnType = columnDef.getType();

                    if (columnType == kInt16) {
                        flinkRow.setField(totalFieldIndex, rowView.GetInt16Unsafe(j));
                    } else if (columnType == kInt32) {
                        flinkRow.setField(totalFieldIndex, rowView.GetInt32Unsafe(j));
                    } else if (columnType == kInt64) {
                        //flinkRow.setField(totalFieldIndex, rowView.GetInt64Unsafe(j));

                        // TODO: Change to int to handle long data
                        //flinkRow.setField(totalFieldIndex, (int)rowView.GetInt64Unsafe(j));

                        // TODO: Return the BigInteger instead of long object
                        flinkRow.setField(totalFieldIndex, new java.math.BigInteger(String.valueOf(rowView.GetInt64Unsafe(j))));

                    } else if (columnType== kFloat) {
                        flinkRow.setField(totalFieldIndex, rowView.GetFloatUnsafe(j));
                    } else if (columnType == kDouble) {
                        flinkRow.setField(totalFieldIndex, rowView.GetDoubleUnsafe(j));
                    } else if (columnType == kBool) {
                        flinkRow.setField(totalFieldIndex, rowView.GetBoolUnsafe(j));
                    } else if (columnType == kVarchar) {
                        flinkRow.setField(totalFieldIndex, rowView.GetStringUnsafe(j));
                    } else if (columnType == kTimestamp) {
                        // Need to convert to Timestamp object
                        Timestamp value = new Timestamp(rowView.GetTimestampUnsafe(j));
                        flinkRow.setField(totalFieldIndex, value);
                    } else if (columnType == kDate) {
                        // TODO: Check date data object in Flink row
                        int days = rowView.GetDateUnsafe(j);
                        Date value = new Date(rowView.GetYearUnsafe(days)-1900,
                                rowView.GetMonthUnsafe(days), rowView.GetDayUnsafe(days));
                        flinkRow.setField(totalFieldIndex, value);
                    } else {
                        throw new FesqlException(String.format("Fail to decode row with type %s", columnType));
                    }
                }

                totalFieldIndex += 1;
            }

        }

        return flinkRow;
    }

    /**
     * Get the size of each slice and init row builder.
     */
    public List<Integer> getFesqlRowSliceSizes(Row flinkRow) {
        List<Integer> sliceSizes = new ArrayList<Integer>();
        for (int i=0; i < this.sliceNum; ++i) {
            int stringFieldSize = 0;
            List<Integer> stringFieldIndex = this.stringFieldIndexes.get(i);

            for (int j=0; j < stringFieldIndex.size(); ++j) {
                // TODO: Optimize the check null and get value
                if (flinkRow.getField(j) != null) {
                    stringFieldSize += String.valueOf(flinkRow.getField(j)).length();
                }
            }

            int totalSize = this.rowBuilders.get(i).CalTotalLength(stringFieldSize);
            sliceSizes.add(totalSize);
        }

        return sliceSizes;
    }

    public void encodeSingleFlinkRow(Row flinkRow, long outputBufPointer, int outputSize, int sliceIndex) throws FesqlException {
        RowBuilder rowBuilder = this.rowBuilders.get(sliceIndex);
        rowBuilder.SetBuffer(outputBufPointer, outputSize);

        List<TypeOuterClass.ColumnDef> columnDefs = this.columnDefLists.get(sliceIndex);
        int fieldNum = columnDefs.size();
        int totalFieldIndex = this.sliceStartIndexMap.get(sliceIndex);

        for (int i=0; i < fieldNum; ++i) {
            TypeOuterClass.ColumnDef columnDef = columnDefs.get(i);

            if (flinkRow.getField(totalFieldIndex) == null) {
                rowBuilder.AppendNULL();
            } else {
                Object value = flinkRow.getField(totalFieldIndex);
                TypeOuterClass.Type columnType = columnDef.getType();
                if (columnType == kInt16) {
                    rowBuilder.AppendInt16((short)value);
                } else if (columnType == kInt32) {
                    rowBuilder.AppendInt32((int)value);
                } else if (columnType == kInt64) {
                    //rowBuilder.AppendInt64((long)value);
                    // TODO: handle big int
                    if (value instanceof java.math.BigInteger) {
                        rowBuilder.AppendInt64(((java.math.BigInteger)value).longValue());
                    } else {
                        rowBuilder.AppendInt64((long)value);
                    }
                } else if (columnType== kFloat) {
                    rowBuilder.AppendFloat((float)value);
                } else if (columnType == kDouble) {
                    rowBuilder.AppendDouble((double)value);
                } else if (columnType == kBool) {
                    rowBuilder.AppendBool((boolean)value);
                } else if (columnType == kVarchar) {
                    String stringValue = String.valueOf(value);
                    rowBuilder.AppendString(stringValue, stringValue.length());
                } else if (columnType == kTimestamp) {
                    // TODO: Get the real time from flink schema

                    // TODO: Set time zone if needed for streaming processing
                    // Get the object of LocalDateTime
                    LocalDateTime localDateTime = (LocalDateTime) value;
                    rowBuilder.AppendTimestamp(localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

                    // TODO: Set for batch processing
                    //Timestamp timestamp = (Timestamp) value;
                    //rowBuilder.AppendTimestamp(timestamp.getTime());

                } else if (columnType == kDate) {
                    // TODO: Check date data object in Flink row
                    Date dateValue = (Date) value;
                    rowBuilder.AppendDate(dateValue.getYear()+1900, dateValue.getMonth(), dateValue.getDate());
                } else {
                    throw new FesqlException(String.format("Fail to encode row with type %s", columnType));
                }
            }

            totalFieldIndex += 1;
        }
    }

    public void delete() {
        for (RowView rowView: this.rowViews) {
            rowView.delete();
        }
        this.rowViews = null;

        for (RowBuilder rowBuilder: this.rowBuilders) {
            rowBuilder.delete();
        }
        this.rowBuilders = null;
    }

}
