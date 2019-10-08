package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;


public class RowCodec {
    private static Charset charset = Charset.forName("utf-8");
    private final static Logger logger = LoggerFactory.getLogger(RowCodec.class);
    private final static int stringMaxLength = 32767;

    public static ByteBuffer encode(Object[] row, List<ColumnDesc> schema) throws TabletException {
        return encode(row, schema, 0);
    }

    public static ByteBuffer encode(Object[] row, List<ColumnDesc> schema, int modifyTimes) throws TabletException {
        if (row.length != schema.size()) {
            throw new TabletException("row length mismatch schema");
        }

        Object[] cache = new Object[row.length];
        // TODO limit the max size
        int size = getSize(row, schema, cache);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        if (modifyTimes > 0) {
            buffer.put((byte) (modifyTimes | 0x8F));
        } else {
            if (row.length >= 128) {
                buffer.putShort((short) row.length);
            } else {
                buffer.put((byte) row.length);
            }
        }
        for (int i = 0; i < row.length; i++) {
            ColumnType ct = schema.get(i).getType();
            if (row[i] != null && ct == ColumnType.kString) {
                if (((byte[]) cache[i]).length <= 0) {
                    ct = ColumnType.kEmptyString;
                }
            }
            buffer.put((byte) ct.getValue());
            if (row[i] == null) {
                buffer.put((byte) 0);
                continue;
            }
            switch (ct) {
                case kString:
                    byte[] bytes = (byte[]) cache[i];
                    if (bytes.length < 128) {
                        buffer.put((byte) bytes.length);
                    } else if (bytes.length <= stringMaxLength) {
                        buffer.put((byte)(bytes.length >> 8 | 0x80));
                        buffer.put((byte)(bytes.length & 0xFF));
                    } else {
                        throw new TabletException("kString length should be less than or equal " + stringMaxLength);
                    }
                    buffer.put(bytes);
                    break;
                case kInt32:
                    buffer.put((byte) 4);
                    buffer.putInt((Integer) row[i]);
                    break;
                case kUInt32:
                    throw new TabletException("kUInt32 is not support on jvm platform");

                case kFloat:
                    buffer.put((byte) 4);
                    buffer.putFloat((Float) row[i]);
                    break;

                case kInt64:
                    buffer.put((byte) 8);
                    buffer.putLong((Long) row[i]);
                    break;

                case kUInt64:
                    throw new TabletException("kUInt64 is not support on jvm platform");

                case kDouble:
                    buffer.put((byte) 8);
                    buffer.putDouble((Double) row[i]);
                    break;
                case kTimestamp:
                    buffer.put((byte) 8);
                    if (row[i] instanceof DateTime) {
                        DateTime time = (DateTime) row[i];
                        buffer.putLong(time.getMillis());
                    } else if (row[i] instanceof Timestamp) {
                        Timestamp ts = (Timestamp) row[i];
                        buffer.putLong(ts.getTime());
                    } else {
                        throw new TabletException(row[i].getClass().getName() + " is not support for timestamp ");
                    }
                    break;
                case kInt16:
                    buffer.put((byte) 2);
                    buffer.putShort((Short) row[i]);
                    break;
                case kDate:
                    buffer.put((byte) 8);
                    if (row[i] instanceof Date) {
                        Date date = (Date) row[i];
                        buffer.putLong(date.getTime());
                    } else if (row[i] instanceof LocalDate) {
                        LocalDate date = (LocalDate) row[i];
                        buffer.putLong(date.toDate().getTime());
                    } else {
                        throw new TabletException(row[i].getClass().getName() + " is not support for date");
                    }
                    break;
                case kBool:
                    buffer.put((byte) 1);
                    Boolean bool = (Boolean) row[i];
                    if (bool) {
                        buffer.put((byte) 1);
                    } else {
                        buffer.put((byte) 0);
                    }
                    break;
                case kEmptyString:
                    buffer.put((byte) 0);
                    break;
                default:
                    throw new TabletException(schema.get(i).getType().toString() + " is not support on jvm platform");
            }
        }
        return buffer;
    }

    public static void decode(ByteBuffer buffer, List<ColumnDesc> schema, Object[] row, int start, int length) throws TabletException {
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        int colLength = 0;
        Byte temp = buffer.asReadOnlyBuffer().get();
        if ((temp & 0x80) != 0) {
            colLength = buffer.get() & 0x7F;
        } else {
            if (schema.size() >= 128) {
                colLength = buffer.getShort();
            } else {
                colLength = buffer.get() & 0xFF;
            }
        }
        if (colLength > length) {
            colLength = length;
        }
        int index = start;
        int count = 0;
        while (buffer.position() < buffer.limit() && count < colLength) {
            byte type = buffer.get();
            byte tmpSize = buffer.get();
            int size = 0;
            if ((tmpSize & 0x80) == 0) {
                size = tmpSize;
            } else {
                byte lowData = buffer.get();
                size = ((tmpSize & 0x7F) << 8) | (lowData & 0xFF);
            }
            ColumnType ctype = ColumnType.valueOf((int) type);
            if (size == 0 && ctype == ColumnType.kEmptyString) {
                row[index] = "";
                index++;
                count++;
                continue;
            }
            if (size == 0) {
                row[index] = null;
                index++;
                count++;
                continue;
            }
            switch (ctype) {
                case kString:
                    byte[] inner = new byte[size];
                    buffer.get(inner);
                    String val = new String(inner, charset);
                    row[index] = val;
                    break;
                case kInt32:
                    row[index] = buffer.getInt();
                    break;
                case kInt64:
                    row[index] = buffer.getLong();
                    break;
                case kDouble:
                    row[index] = buffer.getDouble();
                    break;
                case kFloat:
                    row[index] = buffer.getFloat();
                    break;
                case kTimestamp:
                    long time = buffer.getLong();
                    row[index] = new DateTime(time);
                    break;
                case kInt16:
                    row[index] = buffer.getShort();
                    break;
                case kDate:
                    long date = buffer.getLong();
                    row[index] = new Date(date);
                    break;
                case kBool:
                    int byteValue = buffer.get();
                    if (byteValue == 0) {
                        row[index] = false;
                    } else {
                        row[index] = true;
                    }
                    break;
//                case kEmptyString:
//                    row[index]="";
//                    break;
                default:
                    throw new TabletException(ctype.toString() + " is not support on jvm platform");
            }
            index++;
            count++;
        }
    }

    public static Object[] decode(ByteBuffer buffer, List<ColumnDesc> schema) throws TabletException {
        return decode(buffer, schema, 0);
    }

    //for adding field
    public static Object[] decode(ByteBuffer buffer, List<ColumnDesc> schema, int addFieldSize) throws TabletException {
        Object[] row = new Object[schema.size() + addFieldSize];
        decode(buffer, schema, row, 0, row.length);
        return row;
    }

    private static int getSize(Object[] row, List<ColumnDesc> schema, Object[] cache) {
        int totalSize = 1;
        if (schema.size() >= 128) {
            totalSize++;
        }
        for (int i = 0; i < row.length; i++) {
            totalSize += 2;
            if (row[i] == null) {
                continue;
            }
            switch (schema.get(i).getType()) {
                case kString:
                    byte[] bytes = ((String) row[i]).getBytes(charset);
                    cache[i] = bytes;
                    totalSize += bytes.length;
                    if (bytes.length >= 128) {
                        totalSize++;
                    }
                    break;
                case kBool:
                    totalSize += 1;
                    break;
                case kInt16:
                    totalSize += 2;
                    break;
                case kInt32:
                case kUInt32:
                case kFloat:
                    totalSize += 4;
                    break;
                case kInt64:
                case kUInt64:
                case kDouble:
                case kTimestamp:
                case kDate:
                    totalSize += 8;
                    break;
                default:
                    break;
            }
        }
        return totalSize;
    }
}
