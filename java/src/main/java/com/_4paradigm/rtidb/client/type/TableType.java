package com._4paradigm.rtidb.client.type;

import com._4paradigm.rtidb.type.Type;

public enum TableType {

    kTimeSeries,
    kRelational;

    public static Type.TableType valueFrom(TableType tableType) {
        switch (tableType) {
            case kTimeSeries:
                return Type.TableType.kTimeSeries;
            case kRelational:
                return Type.TableType.kRelational;
            default:
                throw new RuntimeException("not supported type with" + tableType);
        }
    }

}
