package com._4paradigm.rtidb.client.type;

import com._4paradigm.rtidb.type.Type;

public enum DataType {

    Bool,
    SmallInt,//int16
    Int,   //int32
    BigInt,  //int64
    Float,
    Double,
    Varchar, //string
    Date,
    Timestamp,
    Blob;

    public static Type.DataType valueFrom(DataType dataType) {
        switch (dataType) {
            case Bool:
                return Type.DataType.kBool;
            case SmallInt:
                return Type.DataType.kSmallInt;
            case Int:
                return Type.DataType.kInt;
            case BigInt:
                return Type.DataType.kBigInt;
            case Float:
                return Type.DataType.kFloat;
            case Double:
                return Type.DataType.kDouble;
            case Varchar:
                return Type.DataType.kVarchar;
            case Date:
                return Type.DataType.kDate;
            case Timestamp:
                return Type.DataType.kTimestamp;
            case Blob:
                return Type.DataType.kBlob;
            default:
                throw new RuntimeException("not supported type with" + dataType);
        }
    }

    public static DataType valueFrom(Type.DataType dataType) {
        switch (dataType) {
            case kBool:
                return Bool;
            case kSmallInt:
                return SmallInt;
            case kInt:
                return Int;
            case kBigInt:
                return BigInt;
            case kFloat:
                return Float;
            case kDouble:
                return Double;
            case kVarchar:
                return Varchar;
            case kDate:
                return Date;
            case kTimestamp:
                return Timestamp;
            case kBlob:
                return Blob;
            default:
                throw new RuntimeException("not supported type with" + dataType);
        }
    }
}
