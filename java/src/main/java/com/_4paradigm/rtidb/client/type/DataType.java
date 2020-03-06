package com._4paradigm.rtidb.client.type;

import com._4paradigm.rtidb.type.Type;

public enum DataType {

    kBool,
    kSmallInt,//int16
    kInt,   //int32
    kBigInt,  //int64
    kFloat,
    kDouble,
    kVarchar, //string
    kDate,
    kTimestamp,
    kBlob;

    public static Type.DataType valueFrom(DataType dataType) {
        switch (dataType) {
            case kBool:
                return Type.DataType.kBool;
            case kSmallInt:
                return Type.DataType.kSmallInt;
            case kInt:
                return Type.DataType.kInt;
            case kBigInt:
                return Type.DataType.kBigInt;
            case kFloat:
                return Type.DataType.kFloat;
            case kDouble:
                return Type.DataType.kDouble;
            case kVarchar:
                return Type.DataType.kVarchar;
            case kDate:
                return Type.DataType.kDate;
            case kTimestamp:
                return Type.DataType.kTimestamp;
            case kBlob:
                return Type.DataType.kBlob;
            default:
                throw new RuntimeException("not supported type with" + dataType);
        }
    }

    public static DataType valueFrom(Type.DataType dataType) {
        switch (dataType) {
            case kBool:
                return kBool;
            case kSmallInt:
                return kSmallInt;
            case kInt:
                return kInt;
            case kBigInt:
                return kBigInt;
            case kFloat:
                return kFloat;
            case kDouble:
                return kDouble;
            case kVarchar:
                return kVarchar;
            case kDate:
                return kDate;
            case kTimestamp:
                return kTimestamp;
            case kBlob:
                return kBlob;
            default:
                throw new RuntimeException("not supported type with" + dataType);
        }
    }
}
