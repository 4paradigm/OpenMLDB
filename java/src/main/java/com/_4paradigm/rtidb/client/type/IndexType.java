package com._4paradigm.rtidb.client.type;

import com._4paradigm.rtidb.type.Type;

public enum IndexType {

    Unique, // uinque index
    NoUnique,
    PrimaryKey,
    AutoGen,  // auto gen primary key
    Increment; // auto gen increment id primary key

    public static Type.IndexType valueFrom(IndexType indexType) {
        switch (indexType) {
            case Unique:
                return Type.IndexType.kUnique;
            case NoUnique:
                return Type.IndexType.kNoUnique;
            case PrimaryKey:
                return Type.IndexType.kPrimaryKey;
            case AutoGen:
                return Type.IndexType.kAutoGen;
            case Increment:
                return Type.IndexType.kIncrement;
            default:
                throw new RuntimeException("not supported type with" + indexType);
        }
    }
}
