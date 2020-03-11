package com._4paradigm.rtidb.client.type;

import com._4paradigm.rtidb.type.Type;

public enum IndexType {

    kUnique, // uinque index
    kNoUinque,
    kPrimaryKey,
    kAutoGen,  // auto gen primary key
    kIncrement; // auto gen increment id primary key

    public static Type.IndexType valueFrom(IndexType indexType) {
        switch (indexType) {
            case kUnique:
                return Type.IndexType.kUnique;
            case kNoUinque:
                return Type.IndexType.kNoUinque;
            case kPrimaryKey:
                return Type.IndexType.kPrimaryKey;
            case kAutoGen:
                return Type.IndexType.kAutoGen;
            case kIncrement:
                return Type.IndexType.kIncrement;
            default:
                throw new RuntimeException("not supported type with" + indexType);
        }
    }
}
